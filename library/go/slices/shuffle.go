package slices

import (
	"math/rand"
)

// Shuffle shuffles values in slice using given or pseudo-random source.
// It will alter original non-empty slice, consider copy it beforehand.
func Shuffle[E any](a []E, src rand.Source) []E {
	if len(a) < 2 {
		return a
	}
	shuffle(src)(len(a), func(i, j int) {
		a[i], a[j] = a[j], a[i]
	})
	return a
}

// ShuffleStrings shuffles values in string slice using given or pseudo-random source.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Shuffle instead.
var ShuffleStrings = Shuffle[string]

// ShuffleInts shuffles values in int slice using given or pseudo-random source.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Shuffle instead.
var ShuffleInts = Shuffle[int]

// ShuffleInt8s shuffles values in int8 slice using given or pseudo-random source.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Shuffle instead.
var ShuffleInt8s = Shuffle[int8]

// ShuffleInt16s shuffles values in int16 slice using given or pseudo-random source.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Shuffle instead.
var ShuffleInt16s = Shuffle[int16]

// ShuffleInt32s shuffles values in int32 slice using given or pseudo-random source.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Shuffle instead.
var ShuffleInt32s = Shuffle[int32]

// ShuffleInt64s shuffles values in int64 slice using given or pseudo-random source.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Shuffle instead.
var ShuffleInt64s = Shuffle[int64]

// ShuffleUints shuffles values in uint slice using given or pseudo-random source.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Shuffle instead.
var ShuffleUints = Shuffle[uint]

// ShuffleUint8s shuffles values in uint8 slice using given or pseudo-random source.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Shuffle instead.
var ShuffleUint8s = Shuffle[uint8]

// ShuffleUint16s shuffles values in uint16 slice using given or pseudo-random source.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Shuffle instead.
var ShuffleUint16s = Shuffle[uint16]

// ShuffleUint32s shuffles values in uint32 slice using given or pseudo-random source.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Shuffle instead.
var ShuffleUint32s = Shuffle[uint32]

// ShuffleUint64s shuffles values in uint64 slice using given or pseudo-random source.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Shuffle instead.
var ShuffleUint64s = Shuffle[uint64]

// ShuffleFloat32s shuffles values in float32 slice using given or pseudo-random source.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Shuffle instead.
var ShuffleFloat32s = Shuffle[float32]

// ShuffleFloat64s shuffles values in float64 slice using given or pseudo-random source.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Shuffle instead.
var ShuffleFloat64s = Shuffle[float64]

// ShuffleBools shuffles values in bool slice using given or pseudo-random source.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Shuffle instead.
var ShuffleBools = Shuffle[bool]

func shuffle(src rand.Source) func(n int, swap func(i, j int)) {
	shuf := rand.Shuffle
	if src != nil {
		shuf = rand.New(src).Shuffle
	}
	return shuf
}
