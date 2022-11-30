package tvm

import "sort"

type entityAttribute struct {
	key   string
	value string
}

// subTree provides index for fast entity lookup with attributes
//
//	or some subset of entity attributes
type subTree struct {
	// entities contains entities with attributes from previous branches of tree:
	//   * root subTree contains all entities
	//   * next subTree contains entities with {"key#X": "value#X"}
	//   * next subTree after next contains entities with {"key#X": "value#X", "key#Y": "value#Y"}
	//   * and so on
	// "key#X", "key#Y", ... - are sorted
	entities []Entity
	// entityLengths provides O(1) for exact entity lookup
	entityLengths map[int]interface{}
	// entityIds is creation-time crutch
	entityIds  []int
	idxByAttrs *idxByAttrs
}

type idxByAttrs = map[entityAttribute]*subTree

func (s *subTree) containsExactEntity(entity Entity) bool {
	subtree := s.findSubTree(entity)
	if subtree == nil {
		return false
	}

	_, ok := subtree.entityLengths[len(entity)]
	return ok
}

func (s *subTree) getEntitiesWithAttrs(entityPart Entity) []Entity {
	subtree := s.findSubTree(entityPart)
	if subtree == nil {
		return nil
	}

	return subtree.entities
}

func (s *subTree) findSubTree(e Entity) *subTree {
	keys := make([]string, 0, len(e))
	for k := range e {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	res := s

	for _, k := range keys {
		if res.idxByAttrs == nil {
			return nil
		}

		kv := entityAttribute{key: k, value: e[k]}
		ok := false

		res, ok = (*res.idxByAttrs)[kv]
		if !ok {
			return nil
		}
	}

	return res
}
