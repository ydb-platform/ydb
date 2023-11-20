package proto

import (
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"
)

func TestError_Error(t *testing.T) {
	err := errors.Wrap(ErrNoZookeeper, "failed")
	require.Equal(t, err.Error(), "failed: NO_ZOOKEEPER (225)")
	require.ErrorIs(t, err, ErrNoZookeeper)

	var codeErr Error
	require.ErrorAs(t, err, &codeErr)
	require.Equal(t, ErrNoZookeeper, codeErr)

	require.Equal(t, errors.Wrap(Error(-1), "failed").Error(), "failed: UNKNOWN (-1)")
}
