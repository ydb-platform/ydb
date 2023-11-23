package proto

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReader_Int32(t *testing.T) {
	var b Buffer
	b.PutInt32(1056)

	v, err := b.Reader().Int32()
	require.NoError(t, err)
	require.Equal(t, int32(1056), v)
}

func TestReader_Int64(t *testing.T) {
	var b Buffer
	b.PutInt64(204789)

	v, err := b.Reader().Int64()
	require.NoError(t, err)
	require.Equal(t, int64(204789), v)
}

func TestReader_Int(t *testing.T) {
	var b Buffer
	b.PutInt(529)

	v, err := b.Reader().Int()
	require.NoError(t, err)
	require.Equal(t, 529, v)
}
