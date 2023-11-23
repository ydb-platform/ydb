package paging

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testCaseSize[Type any] struct {
	value        Type
	expectedSize uint64
	expectedKind acceptorKind
}

func (tc testCaseSize[Type]) execute(t *testing.T) {
	name := reflect.TypeOf(tc.value).Name()

	t.Run(name, func(t *testing.T) {
		x0 := tc.value
		x1 := new(Type)
		*x1 = x0
		x2 := new(*Type)
		*x2 = x1

		size0, kind0, err := sizeOfValue(x0)
		require.NoError(t, err)
		require.Equal(t, size0, tc.expectedSize)
		require.Equal(t, kind0, tc.expectedKind)

		size1, kind1, err := sizeOfValue(x1)
		require.NoError(t, err)
		require.Equal(t, size1, tc.expectedSize)
		require.Equal(t, kind1, tc.expectedKind)

		size2, kind2, err := sizeOfValue(x2)
		require.NoError(t, err)
		require.Equal(t, size2, tc.expectedSize)
		require.Equal(t, kind2, tc.expectedKind)
	})
}

func TestSize(t *testing.T) {
	type testCase interface {
		execute(t *testing.T)
	}

	testCases := []testCase{
		testCaseSize[int8]{value: 1, expectedSize: 1, expectedKind: fixedSize},
		testCaseSize[int16]{value: 1, expectedSize: 2, expectedKind: fixedSize},
		testCaseSize[int32]{value: 1, expectedSize: 4, expectedKind: fixedSize},
		testCaseSize[int64]{value: 1, expectedSize: 8, expectedKind: fixedSize},
		testCaseSize[uint8]{value: 1, expectedSize: 1, expectedKind: fixedSize},
		testCaseSize[uint16]{value: 1, expectedSize: 2, expectedKind: fixedSize},
		testCaseSize[uint32]{value: 1, expectedSize: 4, expectedKind: fixedSize},
		testCaseSize[uint64]{value: 1, expectedSize: 8, expectedKind: fixedSize},
		testCaseSize[float32]{value: 1.0, expectedSize: 4, expectedKind: fixedSize},
		testCaseSize[float64]{value: 1.0, expectedSize: 8, expectedKind: fixedSize},
		testCaseSize[string]{value: "abcde", expectedSize: 5, expectedKind: variableSize},
		testCaseSize[string]{value: "абвгд", expectedSize: 10, expectedKind: variableSize},
		testCaseSize[[]byte]{value: []byte("abcde"), expectedSize: 5, expectedKind: variableSize},
		testCaseSize[[]byte]{value: []byte("абвгд"), expectedSize: 10, expectedKind: variableSize},
		testCaseSize[time.Time]{value: time.Now().UTC(), expectedSize: 16, expectedKind: fixedSize},
	}

	for _, tc := range testCases {
		tc.execute(t)
	}
}
