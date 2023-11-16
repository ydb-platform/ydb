package proto

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func requireEqual[T any](t *testing.T, a, b ColumnOf[T]) {
	t.Helper()
	require.Equal(t, a.Rows(), b.Rows(), "rows count should match")
	for i := 0; i < a.Rows(); i++ {
		require.Equalf(t, a.Row(i), b.Row(i), "[%d]", i)
	}
}

func TestColumnType_Elem(t *testing.T) {
	t.Run("Array", func(t *testing.T) {
		v := ColumnTypeInt16.Array()
		assert.Equal(t, ColumnType("Array(Int16)"), v)
		assert.True(t, v.IsArray())
		assert.Equal(t, ColumnTypeInt16, v.Elem())
	})
	t.Run("Simple", func(t *testing.T) {
		assert.Equal(t, ColumnTypeNone, ColumnTypeFloat32.Elem())
		assert.False(t, ColumnTypeInt32.IsArray())
	})
	t.Run("Conflict", func(t *testing.T) {
		t.Run("Compatible", func(t *testing.T) {
			for _, tt := range []struct {
				A, B ColumnType
			}{
				{}, // blank
				{A: ColumnTypeInt32, B: ColumnTypeInt32},
				{A: ColumnTypeDateTime, B: ColumnTypeDateTime},
				{A: ColumnTypeArray.Sub(ColumnTypeInt32), B: ColumnTypeArray.Sub(ColumnTypeInt32)},
				{A: ColumnTypeDateTime.With("Europe/Moscow"), B: ColumnTypeDateTime.With("UTC")},
				{A: ColumnTypeDateTime.With("Europe/Moscow"), B: ColumnTypeDateTime},
				{A: "Map(String,String)", B: "Map(String, String)"},
				{A: "Enum8('increment' = 1, 'gauge' = 2)", B: "Int8"},
				{A: "Int8", B: "Enum8('increment' = 1, 'gauge' = 2)"},
			} {
				assert.False(t, tt.A.Conflicts(tt.B),
					"%s ~ %s", tt.A, tt.B,
				)
			}
		})
		t.Run("Incompatible", func(t *testing.T) {
			for _, tt := range []struct {
				A, B ColumnType
			}{
				{A: ColumnTypeInt64}, // blank
				{A: ColumnTypeInt32, B: ColumnTypeInt64},
				{A: ColumnTypeDateTime, B: ColumnTypeInt32},
				{A: ColumnTypeArray.Sub(ColumnTypeInt32), B: ColumnTypeArray.Sub(ColumnTypeInt64)},
				{A: "Map(String,String)", B: "Map(String,Int32)"},
				{A: "Enum16('increment' = 1, 'gauge' = 2)", B: "Int8"},
			} {
				assert.True(t, tt.A.Conflicts(tt.B),
					"%s !~ %s", tt.A, tt.B,
				)
			}
		})
	})
}
