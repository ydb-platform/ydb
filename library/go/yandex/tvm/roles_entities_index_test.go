package tvm

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRolesSubTreeContainsExactEntity(t *testing.T) {
	origEntities := []Entity{
		{"key#1": "value#1"},
		{"key#1": "value#1", "key#2": "value#2", "key#4": "value#4"},
		{"key#1": "value#1", "key#2": "value#2"},
		{"key#1": "value#2", "key#2": "value#2"},
		{"key#3": "value#3"},
	}
	entities := buildEntities(origEntities)

	for _, e := range generatedRandEntities() {
		found := false
		for _, o := range origEntities {
			if reflect.DeepEqual(e, o) {
				found = true
				break
			}
		}

		require.Equal(t, found, entities.subtree.containsExactEntity(e), e)
	}
}

func generatedRandEntities() []Entity {
	rand.Seed(time.Now().UnixNano())

	keysStages := createStages([]string{"key#1", "key#2", "key#3", "key#4", "key#5"})
	valuesSet := []string{"value#1", "value#2", "value#3", "value#4", "value#5"}

	res := make([]Entity, 0)

	keySet := make([]string, 0, 5)
	for keysStages.getNextStage(&keySet) {
		entity := Entity{}
		for _, key := range keySet {
			entity[key] = valuesSet[rand.Intn(len(valuesSet))]

			e := Entity{}
			for k, v := range entity {
				e[k] = v
			}
			res = append(res, e)
		}
	}

	return res
}

func TestRolesGetEntitiesWithAttrs(t *testing.T) {
	type TestCase struct {
		in  Entity
		out []Entity
	}

	cases := []TestCase{
		{
			out: []Entity{
				{"key#1": "value#1"},
				{"key#1": "value#1", "key#2": "value#2", "key#4": "value#4"},
				{"key#1": "value#2", "key#2": "value#2"},
				{"key#3": "value#3"},
			},
		},
		{
			in: Entity{"key#1": "value#1"},
			out: []Entity{
				{"key#1": "value#1"},
				{"key#1": "value#1", "key#2": "value#2", "key#4": "value#4"},
			},
		},
		{
			in: Entity{"key#1": "value#2"},
			out: []Entity{
				{"key#1": "value#2", "key#2": "value#2"},
			},
		},
		{
			in: Entity{"key#2": "value#2"},
			out: []Entity{
				{"key#1": "value#1", "key#2": "value#2", "key#4": "value#4"},
				{"key#1": "value#2", "key#2": "value#2"},
			},
		},
		{
			in: Entity{"key#3": "value#3"},
			out: []Entity{
				{"key#3": "value#3"},
			},
		},
	}

	entities := buildEntities([]Entity{
		{"key#1": "value#1"},
		{"key#1": "value#1", "key#2": "value#2", "key#4": "value#4"},
		{"key#1": "value#2", "key#2": "value#2"},
		{"key#3": "value#3"},
	})

	for idx, c := range cases {
		require.Equal(t, c.out, entities.subtree.getEntitiesWithAttrs(c.in), idx)
	}
}
