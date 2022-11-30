package slices

import (
	"bytes"
	"net"

	"github.com/gofrs/uuid"
	"golang.org/x/exp/slices"
)

// ContainsString checks if string slice contains given string.
// Deprecated: use golang.org/x/exp/slices.Contains instead
var ContainsString = slices.Contains[string]

// ContainsBool checks if bool slice contains given bool.
// Deprecated: use golang.org/x/exp/slices.Contains instead
var ContainsBool = slices.Contains[bool]

// ContainsInt checks if int slice contains given int
var ContainsInt = slices.Contains[int]

// ContainsInt8 checks if int8 slice contains given int8.
// Deprecated: use golang.org/x/exp/slices.Contains instead
var ContainsInt8 = slices.Contains[int8]

// ContainsInt16 checks if int16 slice contains given int16.
// Deprecated: use golang.org/x/exp/slices.Contains instead
var ContainsInt16 = slices.Contains[int16]

// ContainsInt32 checks if int32 slice contains given int32.
// Deprecated: use golang.org/x/exp/slices.Contains instead
var ContainsInt32 = slices.Contains[int32]

// ContainsInt64 checks if int64 slice contains given int64.
// Deprecated: use golang.org/x/exp/slices.Contains instead
var ContainsInt64 = slices.Contains[int64]

// ContainsUint checks if uint slice contains given uint.
// Deprecated: use golang.org/x/exp/slices.Contains instead
var ContainsUint = slices.Contains[uint]

// ContainsUint8 checks if uint8 slice contains given uint8.
func ContainsUint8(haystack []uint8, needle uint8) bool {
	return bytes.IndexByte(haystack, needle) != -1
}

// ContainsUint16 checks if uint16 slice contains given uint16.
// Deprecated: use golang.org/x/exp/slices.Contains instead
var ContainsUint16 = slices.Contains[uint16]

// ContainsUint32 checks if uint32 slice contains given uint32.
// Deprecated: use golang.org/x/exp/slices.Contains instead
var ContainsUint32 = slices.Contains[uint32]

// ContainsUint64 checks if uint64 slice contains given uint64.
// Deprecated: use golang.org/x/exp/slices.Contains instead
var ContainsUint64 = slices.Contains[uint64]

// ContainsFloat32 checks if float32 slice contains given float32.
// Deprecated: use golang.org/x/exp/slices.Contains instead
var ContainsFloat32 = slices.Contains[float32]

// ContainsFloat64 checks if float64 slice contains given float64.
// Deprecated: use golang.org/x/exp/slices.Contains instead
var ContainsFloat64 = slices.Contains[float64]

// ContainsByte checks if byte slice contains given byte
func ContainsByte(haystack []byte, needle byte) bool {
	return bytes.IndexByte(haystack, needle) != -1
}

// ContainsIP checks if net.IP slice contains given net.IP
func ContainsIP(haystack []net.IP, needle net.IP) bool {
	for _, e := range haystack {
		if e.Equal(needle) {
			return true
		}
	}
	return false
}

// ContainsUUID checks if UUID slice contains given UUID.
// Deprecated: use golang.org/x/exp/slices.Contains instead
var ContainsUUID = slices.Contains[uuid.UUID]

// Contains checks if slice of T contains given T
// Deprecated: use golang.org/x/exp/slices.Contains instead.
func Contains[E comparable](haystack []E, needle E) (bool, error) {
	return slices.Contains(haystack, needle), nil
}
