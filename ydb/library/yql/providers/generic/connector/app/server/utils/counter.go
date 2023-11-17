package utils

import "golang.org/x/exp/constraints"

type number interface {
	constraints.Integer | constraints.Float
}

type Counter[T number] struct {
	parent *Counter[T]
	value  T
}

func (c *Counter[T]) Add(delta T) {
	if c.parent != nil {
		c.parent.value += delta
	}

	c.value += delta
}

func (c *Counter[T]) Value() T {
	return c.value
}

func (c *Counter[T]) MakeChild() *Counter[T] {
	return &Counter[T]{parent: c}
}

func NewCounter[T number]() *Counter[T] {
	return &Counter[T]{}
}
