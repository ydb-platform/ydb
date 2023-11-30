package example

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFoo(t *testing.T) {
	_, err := os.Stat("/code")
	require.NoError(t, err)
}
