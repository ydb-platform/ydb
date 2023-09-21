package yatest

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContextParameters(t *testing.T) {
	val, ok := BuildFlag("AUTOCHECK")
	if ok {
		assert.Equal(t, "yes", val)
	} else {
		_, ok = BuildFlag("TESTS_REQUESTED")
		assert.Equal(t, true, ok)
	}

	assert.Equal(t, "library/go/test/yatest/gotest", ProjectPath())
}
