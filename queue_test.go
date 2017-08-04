package ds

import (
	"fmt"
	"testing"
	"time"
)

func TestQueueClose(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}
	defer q.Drop()

	if _, err = q.EnqueueString("value 1"); err != nil {
		t.Error(err)
	}

	if q.Length() != 1 {
		t.Errorf("Expected queue length of 1, got %d", q.Length())
	}

	err = q.Close()
	if err != nil {
		t.Error(err)
	}

	if _, err = q.Dequeue(); err != ErrDBClosed {
		t.Errorf("Expected to get ErrDBClosed error, got %s", err)
	}
}
