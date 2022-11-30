package slices

import (
	"bytes"
)

// ContainsAny checks if slice of type E contains any element from given slice
func ContainsAny[E comparable](haystack, needle []E) bool {
	return len(Intersection(haystack, needle)) > 0
}

// ContainsAnyString checks if string slice contains any element from given slice
// Deprecated: use ContainsAny instead.
var ContainsAnyString = ContainsAny[string]

// ContainsAnyBool checks if bool slice contains any element from given slice
// Deprecated: use ContainsAny instead.
var ContainsAnyBool = ContainsAny[bool]

// ContainsAnyInt checks if int slice contains any element from given slice
// Deprecated: use ContainsAny instead.
var ContainsAnyInt = ContainsAny[int]

// ContainsAnyInt8 checks if int8 slice contains any element from given slice
// Deprecated: use ContainsAny instead.
var ContainsAnyInt8 = ContainsAny[int8]

// ContainsAnyInt16 checks if int16 slice contains any element from given slice
// Deprecated: use ContainsAny instead.
var ContainsAnyInt16 = ContainsAny[int16]

// ContainsAnyInt32 checks if int32 slice contains any element from given slice
// Deprecated: use ContainsAny instead.
var ContainsAnyInt32 = ContainsAny[int32]

// ContainsAnyInt64 checks if int64 slice contains any element from given slice
// Deprecated: use ContainsAny instead.
var ContainsAnyInt64 = ContainsAny[int64]

// ContainsAnyUint checks if uint slice contains any element from given slice
// Deprecated: use ContainsAny instead.
var ContainsAnyUint = ContainsAny[uint]

// ContainsAnyUint8 checks if uint8 slice contains any element from given slice
func ContainsAnyUint8(haystack []uint8, needle []uint8) bool {
	return bytes.Contains(haystack, needle)
}

// ContainsAnyUint16 checks if uint16 slice contains any element from given slice
// Deprecated: use ContainsAny instead.
var ContainsAnyUint16 = ContainsAny[uint16]

// ContainsAnyUint32 checks if uint32 slice contains any element from given slice
// Deprecated: use ContainsAny instead.
var ContainsAnyUint32 = ContainsAny[uint32]

// ContainsAnyUint64 checks if uint64 slice contains any element from given slice
// Deprecated: use ContainsAny instead.
var ContainsAnyUint64 = ContainsAny[uint64]

// ContainsAnyFloat32 checks if float32 slice contains any element from given slice
// Deprecated: use ContainsAny instead.
var ContainsAnyFloat32 = ContainsAny[float32]

// ContainsAnyFloat64 checks if float64 slice any element from given slice
// Deprecated: use ContainsAny instead.
var ContainsAnyFloat64 = ContainsAny[float64]

// ContainsAnyByte checks if byte slice contains any element from given slice
func ContainsAnyByte(haystack []byte, needle []byte) bool {
	return bytes.Contains(haystack, needle)
}
