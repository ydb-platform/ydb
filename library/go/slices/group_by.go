package slices

import (
	"fmt"
)

func createNotUniqueKeyError[T comparable](key T) error {
	return fmt.Errorf("duplicated key \"%v\" found. keys are supposed to be unique", key)
}

// GroupBy groups slice entities into map by key provided via keyGetter.
func GroupBy[S ~[]T, T any, K comparable](s S, keyGetter func(T) K) map[K][]T {
	res := map[K][]T{}

	for _, entity := range s {
		key := keyGetter(entity)
		res[key] = append(res[key], entity)
	}

	return res
}

// GroupByUniqueKey groups slice entities into map by key provided via keyGetter with assumption that each key is unique.
//
// Returns an error in case of key ununiqueness.
func GroupByUniqueKey[S ~[]T, T any, K comparable](s S, keyGetter func(T) K) (map[K]T, error) {
	res := map[K]T{}

	for _, entity := range s {
		key := keyGetter(entity)

		_, duplicated := res[key]
		if duplicated {
			return res, createNotUniqueKeyError(key)
		}

		res[key] = entity
	}

	return res, nil
}

// IndexedEntity stores an entity of original slice with its initial index in that slice
type IndexedEntity[T any] struct {
	Value T
	Index int
}

// GroupByWithIndex groups slice entities into map by key provided via keyGetter.
// Each entity of underlying result slice contains the value itself and its index in the original slice
// (See IndexedEntity).
func GroupByWithIndex[S ~[]T, T any, K comparable](s S, keyGetter func(T) K) map[K][]IndexedEntity[T] {
	res := map[K][]IndexedEntity[T]{}

	for i, entity := range s {
		key := keyGetter(entity)
		res[key] = append(res[key], IndexedEntity[T]{
			Value: entity,
			Index: i,
		})
	}

	return res
}

// GroupByUniqueKeyWithIndex groups slice entities into map by key provided via keyGetter with assumption that
// each key is unique.
// Each result entity contains the value itself and its index in the original slice
// (See IndexedEntity).
//
// Returns an error in case of key ununiqueness.
func GroupByUniqueKeyWithIndex[S ~[]T, T any, K comparable](s S, keyGetter func(T) K) (map[K]IndexedEntity[T], error) {
	res := map[K]IndexedEntity[T]{}

	for i, entity := range s {
		key := keyGetter(entity)

		_, duplicated := res[key]
		if duplicated {
			return res, createNotUniqueKeyError(key)
		}

		res[key] = IndexedEntity[T]{
			Value: entity,
			Index: i,
		}
	}

	return res, nil
}
