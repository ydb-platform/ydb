package tvm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRolesGetNextStage(t *testing.T) {
	s := createStages([]string{"key#1", "key#2", "key#3", "key#4"})

	results := [][]string{
		{"key#1"},
		{"key#2"},
		{"key#1", "key#2"},
		{"key#3"},
		{"key#1", "key#3"},
		{"key#2", "key#3"},
		{"key#1", "key#2", "key#3"},
		{"key#4"},
		{"key#1", "key#4"},
		{"key#2", "key#4"},
		{"key#1", "key#2", "key#4"},
		{"key#3", "key#4"},
		{"key#1", "key#3", "key#4"},
		{"key#2", "key#3", "key#4"},
		{"key#1", "key#2", "key#3", "key#4"},
	}

	keySet := make([]string, 0)
	for idx, exp := range results {
		s.getNextStage(&keySet)
		require.Equal(t, exp, keySet, idx)
	}

	// require.False(t, s.getNextStage(&keySet))
}

func TestRolesBuildEntities(t *testing.T) {
	type TestCase struct {
		in  []Entity
		out Entities
	}
	cases := []TestCase{
		{
			in: []Entity{
				{"key#1": "value#1"},
				{"key#1": "value#1", "key#2": "value#2", "key#4": "value#4"},
				{"key#1": "value#2", "key#2": "value#2"},
				{"key#3": "value#3"},
			},
			out: Entities{subtree: subTree{
				entities: []Entity{
					{"key#1": "value#1"},
					{"key#1": "value#1", "key#2": "value#2", "key#4": "value#4"},
					{"key#1": "value#2", "key#2": "value#2"},
					{"key#3": "value#3"},
				},
				entityLengths: map[int]interface{}{1: nil, 2: nil, 3: nil},
				idxByAttrs: &idxByAttrs{
					entityAttribute{key: "key#1", value: "value#1"}: &subTree{
						entities: []Entity{
							{"key#1": "value#1"},
							{"key#1": "value#1", "key#2": "value#2", "key#4": "value#4"},
						},
						entityLengths: map[int]interface{}{1: nil, 3: nil},
						idxByAttrs: &idxByAttrs{
							entityAttribute{key: "key#2", value: "value#2"}: &subTree{
								entities: []Entity{
									{"key#1": "value#1", "key#2": "value#2", "key#4": "value#4"},
								},
								entityLengths: map[int]interface{}{3: nil},
								idxByAttrs: &idxByAttrs{
									entityAttribute{key: "key#4", value: "value#4"}: &subTree{
										entities: []Entity{
											{"key#1": "value#1", "key#2": "value#2", "key#4": "value#4"},
										},
										entityLengths: map[int]interface{}{3: nil},
									},
								},
							},
							entityAttribute{key: "key#4", value: "value#4"}: &subTree{
								entities: []Entity{
									{"key#1": "value#1", "key#2": "value#2", "key#4": "value#4"},
								},
								entityLengths: map[int]interface{}{3: nil},
							},
						},
					},
					entityAttribute{key: "key#1", value: "value#2"}: &subTree{
						entities: []Entity{
							{"key#1": "value#2", "key#2": "value#2"},
						},
						entityLengths: map[int]interface{}{2: nil},
						idxByAttrs: &idxByAttrs{
							entityAttribute{key: "key#2", value: "value#2"}: &subTree{
								entities: []Entity{
									{"key#1": "value#2", "key#2": "value#2"},
								},
								entityLengths: map[int]interface{}{2: nil},
							},
						},
					},
					entityAttribute{key: "key#2", value: "value#2"}: &subTree{
						entities: []Entity{
							{"key#1": "value#1", "key#2": "value#2", "key#4": "value#4"},
							{"key#1": "value#2", "key#2": "value#2"},
						},
						entityLengths: map[int]interface{}{2: nil, 3: nil},
						idxByAttrs: &idxByAttrs{
							entityAttribute{key: "key#4", value: "value#4"}: &subTree{
								entities: []Entity{
									{"key#1": "value#1", "key#2": "value#2", "key#4": "value#4"},
								},
								entityLengths: map[int]interface{}{3: nil},
							},
						},
					},
					entityAttribute{key: "key#3", value: "value#3"}: &subTree{
						entities: []Entity{
							{"key#3": "value#3"},
						},
						entityLengths: map[int]interface{}{1: nil},
					},
					entityAttribute{key: "key#4", value: "value#4"}: &subTree{
						entities: []Entity{
							{"key#1": "value#1", "key#2": "value#2", "key#4": "value#4"},
						},
						entityLengths: map[int]interface{}{3: nil},
					},
				},
			}},
		},
	}

	for idx, c := range cases {
		require.Equal(t, c.out, *buildEntities(c.in), idx)
	}
}

func TestRolesPostProcessSubTree(t *testing.T) {
	type TestCase struct {
		in  subTree
		out subTree
	}

	cases := []TestCase{
		{
			in: subTree{
				entityIds: []int{1, 1, 1, 1, 1, 2, 0, 0, 0},
			},
			out: subTree{
				entities: []Entity{
					{"key#1": "value#1"},
					{"key#1": "value#2", "key#2": "value#2"},
					{"key#3": "value#3"},
				},
				entityLengths: map[int]interface{}{1: nil, 2: nil},
			},
		},
		{
			in: subTree{
				entityIds:     []int{1, 0},
				entityLengths: map[int]interface{}{1: nil, 2: nil},
				idxByAttrs: &idxByAttrs{
					entityAttribute{key: "key#1", value: "value#1"}: &subTree{
						entityIds: []int{2, 0, 0},
					},
					entityAttribute{key: "key#4", value: "value#4"}: &subTree{
						entityIds: []int{0, 0, 0},
					},
				},
			},
			out: subTree{
				entities: []Entity{
					{"key#1": "value#1"},
					{"key#1": "value#2", "key#2": "value#2"},
				},
				entityLengths: map[int]interface{}{1: nil, 2: nil},
				idxByAttrs: &idxByAttrs{
					entityAttribute{key: "key#1", value: "value#1"}: &subTree{
						entities: []Entity{
							{"key#1": "value#1"},
							{"key#3": "value#3"},
						},
						entityLengths: map[int]interface{}{1: nil},
					},
					entityAttribute{key: "key#4", value: "value#4"}: &subTree{
						entities: []Entity{
							{"key#1": "value#1"},
						},
						entityLengths: map[int]interface{}{1: nil},
					},
				},
			},
		},
	}

	entities := []Entity{
		{"key#1": "value#1"},
		{"key#1": "value#2", "key#2": "value#2"},
		{"key#3": "value#3"},
	}

	for idx, c := range cases {
		postProcessSubTree(&c.in, entities)
		require.Equal(t, c.out, c.in, idx)
	}
}

func TestRolesGetUniqueSortedKeys(t *testing.T) {
	type TestCase struct {
		in  []Entity
		out []string
	}

	cases := []TestCase{
		{
			in:  nil,
			out: []string{},
		},
		{
			in:  []Entity{},
			out: []string{},
		},
		{
			in: []Entity{
				{},
			},
			out: []string{},
		},
		{
			in: []Entity{
				{"key#1": "value#1"},
				{},
			},
			out: []string{"key#1"},
		},
		{
			in: []Entity{
				{"key#1": "value#1"},
				{"key#1": "value#2"},
			},
			out: []string{"key#1"},
		},
		{
			in: []Entity{
				{"key#1": "value#1"},
				{"key#1": "value#2", "key#2": "value#2"},
				{"key#3": "value#3"},
			},
			out: []string{"key#1", "key#2", "key#3"},
		},
	}

	for idx, c := range cases {
		require.Equal(t, c.out, getUniqueSortedKeys(c.in), idx)
	}
}
