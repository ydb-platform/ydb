package xerrors

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorfMultipleErrors(t *testing.T) {
	err1 := New("error1")
	err2 := New("error2")
	err3 := New("error3")

	compositeErr := Errorf("errorf: %w, %w", err1, err2)

	require.True(t, Is(compositeErr, err1))
	require.True(t, Is(compositeErr, err2))
	require.False(t, Is(compositeErr, err3))
}
