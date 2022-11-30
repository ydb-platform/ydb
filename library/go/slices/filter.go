package slices

import (
	"golang.org/x/exp/slices"
)

// Filter reduces slice values using given function.
// It operates with a copy of given slice
func Filter[S ~[]T, T any](s S, fn func(T) bool) S {
	if len(s) == 0 {
		return s
	}
	return Reduce(slices.Clone(s), fn)
}

// Reduce is like Filter, but modifies original slice.
func Reduce[S ~[]T, T any](s S, fn func(T) bool) S {
	if len(s) == 0 {
		return s
	}
	var p int
	for _, v := range s {
		if fn(v) {
			s[p] = v
			p++
		}
	}
	return s[:p]
}
