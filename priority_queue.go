package ds

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
)

// PriorityItem extends Item with a priority field
type PriorityItem struct {
	Item
	Priority uint8
}

// compositeKey combines priority and id into a single key for leveldb
func compositeKey(priority uint8, id uint64) []byte {
	key := make([]byte, 9) // 1 byte for priority + 8 bytes for id
	key[0] = priority
	binary.BigEndian.PutUint64(key[1:], id)
	return key
}

// decompositeKey extracts priority and id from the composite key
func decompositeKey(key []byte) (priority uint8, id uint64) {
	priority = key[0]
	id = binary.BigEndian.Uint64(key[1:])
	return
}

// PriorityQueue implements a priority queue using leveldb
type PriorityQueue struct {
	db            *leveldb.DB
	isOpen        bool
	DataDir       string
	priorityHeads []uint64
	minPriority   uint8
	hasItems      []bool
	count         uint64
	lastID        uint64
	sync.RWMutex
}

// findHighestPriority returns the priority value that has the highest priority (lowest number)
func (pq *PriorityQueue) findHighestPriority() uint8 {
	// Since we track minPriority, we can directly return it if there are items
	if pq.hasItems[pq.minPriority] {
		return pq.minPriority
	}

	// If minPriority is invalid, scan for the next valid priority
	for i := pq.minPriority + 1; i < 255; i++ {
		if pq.hasItems[i] {
			pq.minPriority = i // Update minPriority
			return i
		}
	}
	if pq.hasItems[255] {
		pq.minPriority = 255 // Update minPriority
		return 255
	}

	pq.minPriority = 0 // Reset minPriority when queue is empty
	return 0
}

// OpenPriorityQueue creates or opens a priority queue
func OpenPriorityQueue(dataDir string) (*PriorityQueue, error) {
	var err error
	pq := &PriorityQueue{
		priorityHeads: make([]uint64, 256),
		hasItems:      make([]bool, 256),
		minPriority:   255,
	}
	pq.DataDir = dataDir
	pq.db, err = leveldb.OpenFile(dataDir, nil)
	if err != nil {
		return pq, err
	}
	pq.isOpen = true
	return pq, pq.init()
}

// EnqueueWithPriority adds an item to the queue with specified priority
func (pq *PriorityQueue) EnqueueWithPriority(value []byte, priority uint8) (*PriorityItem, error) {
	pq.Lock()
	defer pq.Unlock()

	if !pq.isOpen {
		return nil, ErrDBClosed
	}

	id := pq.lastID + 1
	key := compositeKey(priority, id)

	batch := new(leveldb.Batch)
	batch.Put(key, value)

	metaKey := []byte(fmt.Sprintf("meta:priority:%d:%d", priority, id))
	batch.Put(metaKey, []byte{1}) // 1 表示存在

	lastIDKey := []byte("meta:last_id")
	lastIDBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(lastIDBytes, id)
	batch.Put(lastIDKey, lastIDBytes)

	if err := pq.db.Write(batch, nil); err != nil {
		return nil, err
	}

	if !pq.hasItems[priority] {
		pq.hasItems[priority] = true
		pq.priorityHeads[priority] = id
		if !pq.hasItems[pq.minPriority] || priority < pq.minPriority {
			pq.minPriority = priority
		}
	}

	pq.lastID = id
	pq.count++
	return &PriorityItem{
		Item:     Item{ID: id, Key: key, Value: value},
		Priority: priority,
	}, nil
}

func (pq *PriorityQueue) EnqueueString(value string, priority uint8) (*PriorityItem, error) {
	return pq.EnqueueWithPriority([]byte(value), priority)
}

func (pq *PriorityQueue) EnqueueObject(value interface{}, priority uint8) (*PriorityItem, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return pq.EnqueueWithPriority(buffer.Bytes(), priority)
}

// Dequeue removes and returns the highest priority item
func (pq *PriorityQueue) Dequeue() (*PriorityItem, error) {
	pq.Lock()
	defer pq.Unlock()

	if !pq.isOpen {
		return nil, ErrDBClosed
	}

	if pq.count == 0 {
		return nil, ErrEmpty
	}

	priority := pq.findHighestPriority()
	if priority == 0 && !pq.hasItems[0] {
		return nil, ErrEmpty
	}

	id := pq.priorityHeads[priority]
	key := compositeKey(priority, id)

	value, err := pq.db.Get(key, nil)
	if err != nil {
		return nil, err
	}

	batch := new(leveldb.Batch)
	batch.Delete(key)
	metaKey := []byte(fmt.Sprintf("meta:priority:%d:%d", priority, id))
	batch.Delete(metaKey)

	if err := pq.db.Write(batch, nil); err != nil {
		return nil, err
	}

	nextKey := compositeKey(priority, id+1)
	hasNext, err := pq.db.Has(nextKey, nil)
	if err != nil {
		return nil, err
	}

	if hasNext {
		pq.priorityHeads[priority] = id + 1
	} else {
		pq.hasItems[priority] = false
		if priority == pq.minPriority {
			pq.minPriority = pq.findHighestPriority()
		}
	}

	pq.count--
	return &PriorityItem{
		Item:     Item{ID: id, Key: key, Value: value},
		Priority: priority,
	}, nil
}

// Peek returns the highest priority item without removing it
func (pq *PriorityQueue) Peek() (*PriorityItem, error) {
	pq.RLock()
	defer pq.RUnlock()

	if !pq.isOpen {
		return nil, ErrDBClosed
	}

	if pq.Length() == 0 {
		return nil, ErrEmpty
	}

	priority := pq.findHighestPriority()
	if priority == 0 && !pq.hasItems[0] {
		return nil, ErrEmpty
	}

	id := pq.priorityHeads[priority]
	key := compositeKey(priority, id)

	value, err := pq.db.Get(key, nil)
	if err != nil {
		return nil, err
	}

	return &PriorityItem{
		Item:     Item{ID: id, Key: key, Value: value},
		Priority: priority,
	}, nil
}

// Length returns the number of items in the queue
func (pq *PriorityQueue) Length() uint64 {
	return pq.count
}

// init initializes the priority queue state
func (pq *PriorityQueue) init() error {
	for i := range pq.priorityHeads {
		pq.priorityHeads[i] = 0
		pq.hasItems[i] = false
	}
	pq.minPriority = 255
	pq.count = 0
	pq.lastID = 0

	iter := pq.db.NewIterator(nil, nil)
	defer iter.Release()

	for iter.Next() {
		key := iter.Key()
		if bytes.HasPrefix(key, []byte("meta:")) {
			continue
		}

		priority, id := decompositeKey(key)

		if !pq.hasItems[priority] {
			pq.hasItems[priority] = true
			pq.priorityHeads[priority] = id
			if priority < pq.minPriority {
				pq.minPriority = priority
			}
		} else if id < pq.priorityHeads[priority] {
			pq.priorityHeads[priority] = id
		}

		if id > pq.lastID {
			pq.lastID = id
		}

		pq.count++
	}

	return iter.Error()
}

// Close closes the priority queue database
func (pq *PriorityQueue) Close() error {
	pq.Lock()
	defer pq.Unlock()

	if !pq.isOpen {
		return nil
	}
	if err := pq.db.Close(); err != nil {
		return err
	}

	pq.isOpen = false
	return nil
}

// Drop closes and deletes the priority queue database
func (pq *PriorityQueue) Drop() error {
	if err := pq.Close(); err != nil {
		return err
	}
	return os.RemoveAll(pq.DataDir)
}
