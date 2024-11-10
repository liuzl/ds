package ds

import (
	"bytes"
	"encoding/gob"
	"os"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// prefixSep is the prefix separator for each item key.
var prefixSep []byte = []byte(":")

// order defines the priority ordering of the queue.
type order int

// Defines which priority order to dequeue in.
const (
	ASC  order = iota // Set priority level 0 as most important.
	DESC              // Set priority level 255 as most important.
)

// priorityLevel holds the head and tail position of a priority
// level within the queue.
type priorityLevel struct {
	head uint64
	tail uint64
}

// length returns the total number of items in this priority level.
func (pl *priorityLevel) length() uint64 {
	return pl.tail - pl.head
}

// PriorityItem extends Item with a priority field
type PriorityItem struct {
	Item
	Priority uint8
}

// PriorityQueue implements a priority queue using leveldb
type PriorityQueue struct {
	db       *leveldb.DB
	isOpen   bool
	DataDir  string
	order    order
	levels   [256]*priorityLevel
	curLevel uint8
	sync.RWMutex
}

func OpenPriorityQueueWithOrder(dataDir string, order order) (*PriorityQueue, error) {
	var err error

	// Create a new PriorityQueue.
	pq := &PriorityQueue{
		DataDir: dataDir,
		db:      &leveldb.DB{},
		order:   order,
		isOpen:  false,
	}

	// Open database for the priority queue.
	if pq.db, err = leveldb.OpenFile(dataDir, nil); err != nil {
		return pq, err
	}

	// Set isOpen and return.
	pq.isOpen = true
	return pq, pq.init()
}

// OpenPriorityQueue creates or opens a priority queue
func OpenPriorityQueue(dataDir string) (*PriorityQueue, error) {
	return OpenPriorityQueueWithOrder(dataDir, ASC)
}

// EnqueueWithPriority adds an item to the queue with specified priority
func (pq *PriorityQueue) EnqueueWithPriority(value []byte, priority uint8) (*PriorityItem, error) {
	pq.Lock()
	defer pq.Unlock()

	// Check if queue is closed.
	if !pq.isOpen {
		return nil, ErrDBClosed
	}

	// Get the priorityLevel.
	level := pq.levels[priority]

	// Create new PriorityItem.
	item := &PriorityItem{
		Item: Item{
			ID:    level.tail + 1,
			Key:   pq.generateKey(priority, level.tail+1),
			Value: value,
		},
		Priority: priority,
	}

	// Add it to the priority queue.
	if err := pq.db.Put(item.Key, item.Value, nil); err != nil {
		return nil, err
	}

	// Increment tail position.
	level.tail++

	// If this priority level is more important than the curLevel.
	if pq.cmpAsc(priority) || pq.cmpDesc(priority) {
		pq.curLevel = priority
	}

	return item, nil
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

	// Check if queue is closed.
	if !pq.isOpen {
		return nil, ErrDBClosed
	}

	// Try to get the next item.
	item, err := pq.getNextItem()
	if err != nil {
		return nil, err
	}

	// Remove this item from the priority queue.
	if err = pq.db.Delete(item.Key, nil); err != nil {
		return nil, err
	}

	// Increment head position.
	pq.levels[pq.curLevel].head++

	return item, nil
}

// Peek returns the highest priority item without removing it
func (pq *PriorityQueue) Peek() (*PriorityItem, error) {
	pq.RLock()
	defer pq.RUnlock()

	// Check if queue is closed.
	if !pq.isOpen {
		return nil, ErrDBClosed
	}

	return pq.getNextItem()
}

// Length returns the number of items in the queue
func (pq *PriorityQueue) Length() uint64 {
	pq.RLock()
	defer pq.RUnlock()

	var length uint64
	for _, v := range pq.levels {
		length += v.length()
	}

	return length
}

// Close closes the priority queue database
func (pq *PriorityQueue) Close() error {
	pq.Lock()
	defer pq.Unlock()

	// Check if queue is already closed.
	if !pq.isOpen {
		return nil
	}

	// Close the LevelDB database.
	if err := pq.db.Close(); err != nil {
		return err
	}

	// Reset head and tail of each priority level
	// and set isOpen to false.
	for i := 0; i <= 255; i++ {
		pq.levels[uint8(i)].head = 0
		pq.levels[uint8(i)].tail = 0
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

// cmpAsc returns wehther the given priority level is higher than the
// current priority level based on ascending order.
func (pq *PriorityQueue) cmpAsc(priority uint8) bool {
	return pq.order == ASC && priority < pq.curLevel
}

// cmpAsc returns wehther the given priority level is higher than the
// current priority level based on descending order.
func (pq *PriorityQueue) cmpDesc(priority uint8) bool {
	return pq.order == DESC && priority > pq.curLevel
}

// resetCurrentLevel resets the current priority level of the queue
// so the highest level can be found.
func (pq *PriorityQueue) resetCurrentLevel() {
	if pq.order == ASC {
		pq.curLevel = 255
	} else if pq.order == DESC {
		pq.curLevel = 0
	}
}

// getNextItem returns the next item in the priority queue, updating
// the current priority level of the queue if necessary.
func (pq *PriorityQueue) getNextItem() (*PriorityItem, error) {
	// If the current priority level is empty.
	if pq.levels[pq.curLevel].length() == 0 {
		// Set starting value for curLevel.
		pq.resetCurrentLevel()

		// Try to get the next priority level.
		for i := 0; i <= 255; i++ {
			if (pq.cmpAsc(uint8(i)) || pq.cmpDesc(uint8(i))) && pq.levels[uint8(i)].length() > 0 {
				pq.curLevel = uint8(i)
			}
		}

		// If still empty, return queue empty error.
		if pq.levels[pq.curLevel].length() == 0 {
			return nil, ErrEmpty
		}
	}

	// Try to get the next item in the current priority level.
	return pq.getItemByPriorityID(pq.curLevel, pq.levels[pq.curLevel].head+1)
}

// getItemByID returns an item, if found, for the given ID.
func (pq *PriorityQueue) getItemByPriorityID(priority uint8, id uint64) (*PriorityItem, error) {
	// Check if empty or out of bounds.
	if pq.levels[priority].length() == 0 {
		return nil, ErrEmpty
	} else if id <= pq.levels[priority].head || id > pq.levels[priority].tail {
		return nil, ErrOutOfBounds
	}

	// Get item from database.
	var err error
	item := &PriorityItem{Item: Item{ID: id, Key: pq.generateKey(priority, id)}, Priority: priority}
	if item.Value, err = pq.db.Get(item.Key, nil); err != nil {
		return nil, err
	}

	return item, nil
}

// generatePrefix creates the key prefix for the given priority level.
func (pq *PriorityQueue) generatePrefix(level uint8) []byte {
	// priority + prefixSep = 1 + 1 = 2
	prefix := make([]byte, 2)
	prefix[0] = byte(level)
	prefix[1] = prefixSep[0]
	return prefix
}

// generateKey create a key to be used with LevelDB.
func (pq *PriorityQueue) generateKey(priority uint8, id uint64) []byte {
	// prefix + key = 2 + 8 = 10
	key := make([]byte, 10)
	copy(key[0:2], pq.generatePrefix(priority))
	copy(key[2:], idToKey(id))
	return key
}

// init initializes the priority queue data.
func (pq *PriorityQueue) init() error {
	// Set starting value for curLevel.
	pq.resetCurrentLevel()

	// Loop through each priority level.
	for i := 0; i <= 255; i++ {
		// Create a new LevelDB Iterator for this priority level.
		prefix := pq.generatePrefix(uint8(i))
		iter := pq.db.NewIterator(util.BytesPrefix(prefix), nil)

		// Create a new priorityLevel.
		pl := &priorityLevel{
			head: 0,
			tail: 0,
		}

		// Set priority level head to the first item.
		if iter.First() {
			pl.head = keyToID(iter.Key()[2:]) - 1

			// Since this priority level has item(s), handle updating curLevel.
			if pq.cmpAsc(uint8(i)) || pq.cmpDesc(uint8(i)) {
				pq.curLevel = uint8(i)
			}
		}

		// Set priority level tail to the last item.
		if iter.Last() {
			pl.tail = keyToID(iter.Key()[2:])
		}

		if iter.Error() != nil {
			return iter.Error()
		}

		pq.levels[i] = pl
		iter.Release()
	}

	return nil
}
