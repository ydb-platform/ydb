package proto

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInt128_Int(t *testing.T) {
	for _, x := range []int{
		math.MinInt,
		-1000,
		0,
		1,
		10,
		42,
		12345,
		math.MaxInt,
	} {
		assert.Equal(t, x, Int128FromInt(x).Int())
	}
	assert.Equal(t, math.MaxInt, Int128{High: 1}.Int())
}

func TestInt128_UInt64(t *testing.T) {
	for _, x := range []uint64{
		0,
		1,
		10,
		42,
		12345,
		math.MaxUint64,
	} {
		assert.Equal(t, x, Int128FromUInt64(x).UInt64())
	}
	assert.Equal(t, uint64(math.MaxUint64), Int128FromInt(-1).UInt64())
	assert.Equal(t, uint64(math.MaxUint64), Int128{High: 1}.UInt64())
}

func TestUInt128_Int(t *testing.T) {
	for _, x := range []int{
		0,
		1,
		10,
		42,
		12345,
		math.MaxInt,
	} {
		assert.Equal(t, x, UInt128FromInt(x).Int())
	}
	assert.Equal(t, -1, UInt128{High: 1}.Int())
}

func TestUInt128_UInt64(t *testing.T) {
	for _, x := range []uint64{
		0,
		1,
		10,
		42,
		12345,
		math.MaxUint64,
	} {
		assert.Equal(t, x, UInt128FromUInt64(x).UInt64())
	}
	assert.Equal(t, uint64(math.MaxUint64), UInt128FromInt(-1).UInt64())
	assert.Equal(t, uint64(math.MaxUint64), UInt128{High: 1}.UInt64())
}
