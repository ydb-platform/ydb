package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDataConverter(t *testing.T) {
	dc := DataConverter{}

	type testCase struct {
		src          [][]any
		dst          [][][]any
		rowsPerBlock int
	}

	testCases := []testCase{
		{
			src: [][]any{
				{int32(1), "a"},
				{int32(2), "b"},
				{int32(3), "c"},
				{int32(4), "d"},
			},
			dst: [][][]any{
				{
					{int32(1), int32(2)},
					{"a", "b"},
				},
				{
					{int32(3), int32(4)},
					{"c", "d"},
				},
			},
			rowsPerBlock: 2,
		},
		{
			src: [][]any{
				{int32(1), "a"},
				{int32(2), "b"},
				{int32(3), "c"},
				{int32(4), "d"},
				{int32(5), "e"},
			},
			dst: [][][]any{
				{
					{int32(1), int32(2)},
					{"a", "b"},
				},
				{
					{int32(3), int32(4)},
					{"c", "d"},
				},
				{
					{int32(5)},
					{"e"},
				},
			},
			rowsPerBlock: 2,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("rowsPerRecord_%d", tc.rowsPerBlock), func(t *testing.T) {
			actual := dc.RowsToColumnBlocks(tc.src, tc.rowsPerBlock)
			require.Equal(t, tc.dst, actual)
		})
	}
}
