package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCounter(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		counter := NewCounter[int64]()
		counter.Add(1)
		counter.Add(-1)
		require.Equal(t, int64(0), counter.Value())
	})

	t.Run("hierarchy", func(t *testing.T) {
		parent := NewCounter[uint64]()
		require.Equal(t, uint64(0), parent.Value())

		child1 := parent.MakeChild()
		child2 := parent.MakeChild()

		child1.Add(1)
		require.Equal(t, uint64(1), child1.Value())

		child2.Add(1)
		require.Equal(t, uint64(1), child1.Value())

		require.Equal(t, uint64(2), parent.Value())
	})
}
