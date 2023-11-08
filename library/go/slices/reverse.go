package slices

// Reverse reverses given slice.
// It will alter original non-empty slice, consider copy it beforehand.
func Reverse[E any](s []E) []E {
	if len(s) < 2 {
		return s
	}
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
	return s
}

// ReverseStrings reverses given string slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Reverse instead.
var ReverseStrings = Reverse[string]

// ReverseInts reverses given int slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Reverse instead.
var ReverseInts = Reverse[int]

// ReverseInt8s reverses given int8 slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Reverse instead.
var ReverseInt8s = Reverse[int8]

// ReverseInt16s reverses given int16 slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Reverse instead.
var ReverseInt16s = Reverse[int16]

// ReverseInt32s reverses given int32 slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Reverse instead.
var ReverseInt32s = Reverse[int32]

// ReverseInt64s reverses given int64 slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Reverse instead.
var ReverseInt64s = Reverse[int64]

// ReverseUints reverses given uint slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Reverse instead.
var ReverseUints = Reverse[uint]

// ReverseUint8s reverses given uint8 slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Reverse instead.
var ReverseUint8s = Reverse[uint8]

// ReverseUint16s reverses given uint16 slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Reverse instead.
var ReverseUint16s = Reverse[uint16]

// ReverseUint32s reverses given uint32 slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Reverse instead.
var ReverseUint32s = Reverse[uint32]

// ReverseUint64s reverses given uint64 slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Reverse instead.
var ReverseUint64s = Reverse[uint64]

// ReverseFloat32s reverses given float32 slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Reverse instead.
var ReverseFloat32s = Reverse[float32]

// ReverseFloat64s reverses given float64 slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Reverse instead.
var ReverseFloat64s = Reverse[float64]

// ReverseBools reverses given bool slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Reverse instead.
var ReverseBools = Reverse[bool]
