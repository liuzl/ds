package ds

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestPriorityQueue(t *testing.T) {
	dir := "./test_priority_queue"
	defer os.RemoveAll(dir)

	t.Run("Open Priority Queue", func(t *testing.T) {
		pq, err := OpenPriorityQueue(dir)
		if err != nil {
			t.Error(err)
		}
		defer pq.Close()

		if !pq.isOpen {
			t.Error("Queue should be open")
		}
	})

	t.Run("Basic Enqueue and Dequeue", func(t *testing.T) {
		pq, err := OpenPriorityQueue(dir)
		if err != nil {
			t.Error(err)
		}
		defer pq.Close()

		value1 := []byte("test1")
		priority1 := uint8(1)
		item1, err := pq.EnqueueWithPriority(value1, priority1)
		if err != nil {
			t.Error(err)
		}

		if item1.Priority != priority1 {
			t.Errorf("Expected priority %d, got %d", priority1, item1.Priority)
		}

		dequeued, err := pq.Dequeue()
		if err != nil {
			t.Error(err)
		}

		if !bytes.Equal(dequeued.Value, value1) {
			t.Error("Dequeued value doesn't match enqueued value")
		}
	})

	t.Run("Priority Order", func(t *testing.T) {
		pq, err := OpenPriorityQueue(dir)
		if err != nil {
			t.Error(err)
		}
		defer pq.Close()

		_, err = pq.EnqueueWithPriority([]byte("low"), 3)
		if err != nil {
			t.Error(err)
		}
		_, err = pq.EnqueueWithPriority([]byte("high"), 1)
		if err != nil {
			t.Error(err)
		}
		_, err = pq.EnqueueWithPriority([]byte("medium"), 2)
		if err != nil {
			t.Error(err)
		}

		expected := []struct {
			value    string
			priority uint8
		}{
			{"high", 1},
			{"medium", 2},
			{"low", 3},
		}

		for _, exp := range expected {
			item, err := pq.Dequeue()
			if err != nil {
				t.Error(err)
			}

			if !bytes.Equal(item.Value, []byte(exp.value)) {
				t.Errorf("Expected value %s, got %s", exp.value, string(item.Value))
			}
			if item.Priority != exp.priority {
				t.Errorf("Expected priority %d, got %d", exp.priority, item.Priority)
			}
		}
	})

	t.Run("Peek Operation", func(t *testing.T) {
		pq, err := OpenPriorityQueue(dir)
		if err != nil {
			t.Error(err)
		}
		defer pq.Close()

		value1 := []byte("test1")
		priority1 := uint8(1)
		_, err = pq.EnqueueWithPriority(value1, priority1)
		if err != nil {
			t.Error(err)
		}

		value2 := []byte("test2")
		priority2 := uint8(2)
		_, err = pq.EnqueueWithPriority(value2, priority2)
		if err != nil {
			t.Error(err)
		}

		peeked, err := pq.Peek()
		if err != nil {
			t.Error(err)
		}

		if !bytes.Equal(peeked.Value, value1) {
			t.Error("Peeked value doesn't match highest priority value")
		}

		if peeked.Priority != priority1 {
			t.Errorf("Expected priority %d, got %d", priority1, peeked.Priority)
		}

		length := pq.Length()
		if length != 2 {
			t.Errorf("Expected length 2, got %d", length)
		}
	})

	t.Run("Empty Queue Operations", func(t *testing.T) {
		pq, err := OpenPriorityQueue("empty_test")
		defer os.RemoveAll("empty_test")
		if err != nil {
			t.Error(err)
		}
		defer pq.Close()

		_, err = pq.Dequeue()
		if err != ErrEmpty {
			t.Errorf("Expected ErrEmpty on empty queue Dequeue, got %+v", err)
		}

		_, err = pq.Peek()
		if err != ErrEmpty {
			t.Errorf("Expected ErrEmpty on empty queue Peek, got %+v", err)
		}
	})

	t.Run("Closed Queue Operations", func(t *testing.T) {
		pq, err := OpenPriorityQueue(dir)
		if err != nil {
			t.Error(err)
		}

		pq.Close()

		_, err = pq.EnqueueWithPriority([]byte("test"), 1)
		if err != ErrDBClosed {
			t.Errorf("Expected ErrDBClosed on enqueue to closed queue, got %+v", err)
		}

		_, err = pq.Dequeue()
		if err != ErrDBClosed {
			t.Error("Expected ErrDBClosed on dequeue from closed queue")
		}

		_, err = pq.Peek()
		if err != ErrDBClosed {
			t.Error("Expected ErrDBClosed on peek from closed queue")
		}
	})
}

func TestPriorityQueueRestart(t *testing.T) {
	dir := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	defer os.RemoveAll(dir)

	pq, err := OpenPriorityQueue(dir)
	if err != nil {
		t.Fatal(err)
	}

	items := []struct {
		value    string
		priority uint8
	}{
		{"test1", 1},
		{"test2", 2},
		{"test3", 1},
	}

	for _, item := range items {
		_, err := pq.EnqueueString(item.value, item.priority)
		if err != nil {
			t.Fatal(err)
		}
	}

	lastID := pq.lastID
	pq.Close()

	pq, err = OpenPriorityQueue(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer pq.Close()

	if pq.lastID != lastID {
		t.Errorf("Expected lastID to be %d, got %d", lastID, pq.lastID)
	}

	newItem, err := pq.EnqueueString("test4", 1)
	if err != nil {
		t.Fatal(err)
	}

	if newItem.ID != lastID+1 {
		t.Errorf("Expected new item ID to be %d, got %d", lastID+1, newItem.ID)
	}
}
