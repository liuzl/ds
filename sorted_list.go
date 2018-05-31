package ds

import (
	"container/list"
)

type CompareFunc func(interface{}, interface{}) int

type SortedList struct {
	container *list.List
	compare   CompareFunc
}

func NewSortedList(compare CompareFunc) *SortedList {
	return &SortedList{container: list.New(), compare: compare}
}

func (l *SortedList) Len() int64 {
	return int64(l.container.Len())
}

func (l *SortedList) Front() *list.Element {
	return l.container.Front()
}

func (l *SortedList) Back() *list.Element {
	return l.container.Back()
}

func (l *SortedList) Clear() {
	if l.Len() > 0 {
		l.container = list.New()
	}
}

// Only Search from front to end, optimized for from end to front
func (l *SortedList) Insert(item interface{}) {
	element := l.container.Front()
	for element != nil {
		if l.compare(item, element.Value) <= 0 {
			l.container.InsertBefore(item, element)
			return
		}
		element = element.Next()
	}
	if element == nil {
		l.container.PushBack(item)
	}
}

func (l *SortedList) Remove(e *list.Element) interface{} {
	return l.container.Remove(e)
}
