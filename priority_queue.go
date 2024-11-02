package ds

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"

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
	Queue // 继承基本队列的属性
}

func OpenPriorityQueue(dataDir string) (*PriorityQueue, error) {
	var err error
	pq := &PriorityQueue{}
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

	id := pq.tail + 1
	key := compositeKey(priority, id)

	if err := pq.db.Put(key, value, nil); err != nil {
		return nil, err
	}

	pq.tail++
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

	if pq.Length() == 0 {
		return nil, ErrEmpty
	}

	// 使用迭代器找到最高优先级的项
	iter := pq.db.NewIterator(nil, nil)
	defer iter.Release()

	if !iter.First() {
		return nil, ErrEmpty
	}

	key := iter.Key()
	priority, id := decompositeKey(key)
	value := iter.Value()

	if err := pq.db.Delete(key, nil); err != nil {
		return nil, err
	}

	pq.head++
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

	iter := pq.db.NewIterator(nil, nil)
	defer iter.Release()

	if !iter.First() {
		return nil, ErrEmpty
	}

	key := iter.Key()
	priority, id := decompositeKey(key)

	return &PriorityItem{
		Item:     Item{ID: id, Key: key, Value: iter.Value()},
		Priority: priority,
	}, nil
}
