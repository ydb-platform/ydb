package registryutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildFQName(t *testing.T) {
	testCases := []struct {
		name     string
		parts    []string
		sep      string
		expected string
	}{
		{
			name:     "empty",
			parts:    nil,
			sep:      "_",
			expected: "",
		},
		{
			name:     "one part",
			parts:    []string{"part"},
			sep:      "_",
			expected: "part",
		},
		{
			name:     "two parts",
			parts:    []string{"part", "another"},
			sep:      "_",
			expected: "part_another",
		},
		{
			name:     "parts with sep",
			parts:    []string{"abcde", "deabc"},
			sep:      "abc",
			expected: "deabcde",
		},
	}

	for _, testCase := range testCases {
		c := testCase
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.expected, BuildFQName(c.sep, c.parts...))
		})
	}
}
