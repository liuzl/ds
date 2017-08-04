package ds

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"os"
	"sync"
)

var (
	ErrDBClosed    = errors.New("queue: database is closed")
	ErrEmpty       = errors.New("queue: queue is empty")
	ErrOutOfBounds = errors.New("queue: ID is outside range of queue")
)

type Item struct {
	ID    uint64
	Key   []byte
	Value []byte
}

func (i *Item) ToString() string {
	return string(i.Value)
}

func (i *Item) ToObject(value interface{}) error {
	buffer := bytes.NewBuffer(i.Value)
	decoder := gob.NewDecoder(buffer)
	return decoder.Decode(value)
}

func idToKey(id uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, id)
	return key
}
func keyToID(key []byte) uint64 {
	return binary.BigEndian.Uint64(key)
}

// Standard FIFO (fist in, first out) queue
type Queue struct {
	sync.RWMutex
	DataDir string
	db      *leveldb.DB
	head    uint64
	tail    uint64
	isOpen  bool
}

func OpenQueue(dataDir string) (*Queue, error) {
	var err error
	q := &Queue{
		DataDir: dataDir,
		db:      &leveldb.DB{},
		head:    0,
		tail:    0,
		isOpen:  false,
	}
	q.db, err = leveldb.OpenFile(dataDir, nil)
	if err != nil {
		return q, err
	}
	q.isOpen = true
	return q, q.init()
}

func (q *Queue) Close() error {
	q.Lock()
	defer q.Unlock()

	if !q.isOpen {
		return nil
	}
	if err := q.db.Close(); err != nil {
		return err
	}

	q.head = 0
	q.tail = 0
	q.isOpen = false
	return nil
}

func (q *Queue) Drop() error {
	if err := q.Close(); err != nil {
		return err
	}
	return os.RemoveAll(q.DataDir)
}

func (q *Queue) Length() uint64 {
	return q.tail - q.head
}

func (q *Queue) init() error {
	iter := q.db.NewIterator(nil, nil)
	defer iter.Release()
	if iter.First() {
		q.head = keyToID(iter.Key()) - 1
	}
	if iter.Last() {
		q.tail = keyToID(iter.Key())
	}
	return iter.Error()
}

func (q *Queue) getItemByID(id uint64) (*Item, error) {
	if q.Length() == 0 {
		return nil, ErrEmpty
	} else if id <= q.head || id > q.tail {
		return nil, ErrOutOfBounds
	}
	item := &Item{ID: id, Key: idToKey(id)}
	var err error
	if item.Value, err = q.db.Get(item.Key, nil); err != nil {
		return nil, err
	}
	return item, nil
}

func (q *Queue) Enqueue(value []byte) (*Item, error) {
	q.Lock()
	defer q.Unlock()

	if !q.isOpen {
		return nil, ErrDBClosed
	}
	item := &Item{ID: q.tail + 1, Key: idToKey(q.tail + 1), Value: value}
	if err := q.db.Put(item.Key, item.Value, nil); err != nil {
		return nil, err
	}
	q.tail++
	return item, nil
}

func (q *Queue) EnqueueString(value string) (*Item, error) {
	return q.Enqueue([]byte(value))
}

func (q *Queue) EnqueueObject(value interface{}) (*Item, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return q.Enqueue(buffer.Bytes())
}

func (q *Queue) Dequeue() (*Item, error) {
	q.Lock()
	defer q.Unlock()

	if !q.isOpen {
		return nil, ErrDBClosed
	}

	item, err := q.getItemByID(q.head + 1)
	if err != nil {
		return nil, err
	}
	if err = q.db.Delete(item.Key, nil); err != nil {
		return nil, err
	}
	q.head++
	return item, nil
}

func (q *Queue) Peek() (*Item, error) {
	q.RLock()
	defer q.RUnlock()

	if !q.isOpen {
		return nil, ErrDBClosed
	}
	return q.getItemByID(q.head + 1)
}
