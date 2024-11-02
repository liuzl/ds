package ds

import (
	"bytes"
	"os"
	"testing"
)

func TestPriorityQueue(t *testing.T) {
	// 设置测试目录
	dir := "./test_priority_queue"
	defer os.RemoveAll(dir)

	// 测试打开队列
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

	// 测试基本的入队和出队操作
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

	// 测试优先级顺序
	t.Run("Priority Order", func(t *testing.T) {
		pq, err := OpenPriorityQueue(dir)
		if err != nil {
			t.Error(err)
		}
		defer pq.Close()

		// 按不同优先级入队
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

		// 验证出队顺序是否正确
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

	// 测试Peek操作
	t.Run("Peek Operation", func(t *testing.T) {
		pq, err := OpenPriorityQueue(dir)
		if err != nil {
			t.Error(err)
		}
		defer pq.Close()

		// 入队两个项
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

		// 测试Peek
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

		// 确保Peek不会移除项
		length := pq.Length()
		if length != 2 {
			t.Errorf("Expected length 2, got %d", length)
		}
	})

	// 测试空队列操作
	t.Run("Empty Queue Operations", func(t *testing.T) {
		pq, err := OpenPriorityQueue("empty_test")
		defer os.RemoveAll("empty_test")
		if err != nil {
			t.Error(err)
		}
		defer pq.Close()

		// 测试空队列的Dequeue
		_, err = pq.Dequeue()
		if err != ErrEmpty {
			t.Errorf("Expected ErrEmpty on empty queue Dequeue, got %+v", err)
		}

		// 测试空队列的Peek
		_, err = pq.Peek()
		if err != ErrEmpty {
			t.Errorf("Expected ErrEmpty on empty queue Peek, got %+v", err)
		}
	})

	// 测试关闭队列后的操作
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
