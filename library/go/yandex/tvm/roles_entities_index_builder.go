package tvm

import "sort"

type stages struct {
	keys []string
	id   uint64
}

func createStages(keys []string) stages {
	return stages{
		keys: keys,
	}
}

func (s *stages) getNextStage(keys *[]string) bool {
	s.id += 1
	*keys = (*keys)[:0]

	for idx := range s.keys {
		need := (s.id >> idx) & 0x01
		if need == 1 {
			*keys = append(*keys, s.keys[idx])
		}
	}

	return len(*keys) > 0
}

func buildEntities(entities []Entity) *Entities {
	root := make(idxByAttrs)
	res := &Entities{
		subtree: subTree{
			idxByAttrs: &root,
		},
	}

	stage := createStages(getUniqueSortedKeys(entities))

	keySet := make([]string, 0, len(stage.keys))
	for stage.getNextStage(&keySet) {
		for entityID, entity := range entities {
			currentBranch := &res.subtree

			for _, key := range keySet {
				entValue, ok := entity[key]
				if !ok {
					continue
				}

				if currentBranch.idxByAttrs == nil {
					index := make(idxByAttrs)
					currentBranch.idxByAttrs = &index
				}

				kv := entityAttribute{key: key, value: entValue}
				subtree, ok := (*currentBranch.idxByAttrs)[kv]
				if !ok {
					subtree = &subTree{}
					(*currentBranch.idxByAttrs)[kv] = subtree
				}

				currentBranch = subtree
				currentBranch.entityIds = append(currentBranch.entityIds, entityID)
				res.subtree.entityIds = append(res.subtree.entityIds, entityID)
			}
		}
	}

	postProcessSubTree(&res.subtree, entities)

	return res
}

func postProcessSubTree(sub *subTree, entities []Entity) {
	tmp := make(map[int]interface{}, len(entities))
	for _, e := range sub.entityIds {
		tmp[e] = nil
	}
	sub.entityIds = sub.entityIds[:0]
	for i := range tmp {
		sub.entityIds = append(sub.entityIds, i)
	}
	sort.Ints(sub.entityIds)

	sub.entities = make([]Entity, 0, len(sub.entityIds))
	sub.entityLengths = make(map[int]interface{})
	for _, idx := range sub.entityIds {
		sub.entities = append(sub.entities, entities[idx])
		sub.entityLengths[len(entities[idx])] = nil
	}
	sub.entityIds = nil

	if sub.idxByAttrs != nil {
		for _, rest := range *sub.idxByAttrs {
			postProcessSubTree(rest, entities)
		}
	}
}

func getUniqueSortedKeys(entities []Entity) []string {
	tmp := map[string]interface{}{}

	for _, e := range entities {
		for k := range e {
			tmp[k] = nil
		}
	}

	res := make([]string, 0, len(tmp))
	for k := range tmp {
		res = append(res, k)
	}

	sort.Strings(res)
	return res
}
