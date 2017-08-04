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

func (this *SortedList) Len() int64 {
	return int64(this.container.Len())
}

func (this *SortedList) Front() *list.Element {
	return this.container.Front()
}

func (this *SortedList) Back() *list.Element {
	return this.container.Back()
}

func (this *SortedList) Clear() {
	if this.Len() > 0 {
		this.container = list.New()
	}
}

// Only Search from front to end, optimized for from end to front
func (this *SortedList) Insert(item interface{}) {
	element := this.container.Front()
	for element != nil {
		if this.compare(item, element.Value) <= 0 {
			this.container.InsertBefore(item, element)
			return
		}
		element = element.Next()
	}
	if element == nil {
		this.container.PushBack(item)
	}
}

func (this *SortedList) Remove(e *list.Element) interface{} {
	return this.container.Remove(e)
}
