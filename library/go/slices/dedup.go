package slices

import (
	"sort"

	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
)

// Dedup removes duplicate values from slice.
// It will alter original non-empty slice, consider copy it beforehand.
func Dedup[E constraints.Ordered](s []E) []E {
	if len(s) < 2 {
		return s
	}
	slices.Sort(s)
	tmp := s[:1]
	cur := s[0]
	for i := 1; i < len(s); i++ {
		if s[i] != cur {
			tmp = append(tmp, s[i])
			cur = s[i]
		}
	}
	return tmp
}

// DedupBools removes duplicate values from bool slice.
// It will alter original non-empty slice, consider copy it beforehand.
func DedupBools(a []bool) []bool {
	if len(a) < 2 {
		return a
	}
	sort.Slice(a, func(i, j int) bool { return a[i] != a[j] })
	tmp := a[:1]
	cur := a[0]
	for i := 1; i < len(a); i++ {
		if a[i] != cur {
			tmp = append(tmp, a[i])
			cur = a[i]
		}
	}
	return tmp
}

// DedupStrings removes duplicate values from string slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Dedup instead.
var DedupStrings = Dedup[string]

// DedupInts removes duplicate values from ints slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Dedup instead.
var DedupInts = Dedup[int]

// DedupInt8s removes duplicate values from int8 slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Dedup instead.
var DedupInt8s = Dedup[int8]

// DedupInt16s removes duplicate values from int16 slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Dedup instead.
var DedupInt16s = Dedup[int16]

// DedupInt32s removes duplicate values from int32 slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Dedup instead.
var DedupInt32s = Dedup[int32]

// DedupInt64s removes duplicate values from int64 slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Dedup instead.
var DedupInt64s = Dedup[int64]

// DedupUints removes duplicate values from uint slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Dedup instead.
var DedupUints = Dedup[uint]

// DedupUint8s removes duplicate values from uint8 slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Dedup instead.
var DedupUint8s = Dedup[uint8]

// DedupUint16s removes duplicate values from uint16 slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Dedup instead.
var DedupUint16s = Dedup[uint16]

// DedupUint32s removes duplicate values from uint32 slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Dedup instead.
var DedupUint32s = Dedup[uint32]

// DedupUint64s removes duplicate values from uint64 slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Dedup instead.
var DedupUint64s = Dedup[uint64]

// DedupFloat32s removes duplicate values from float32 slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Dedup instead.
var DedupFloat32s = Dedup[float32]

// DedupFloat64s removes duplicate values from float64 slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Dedup instead.
var DedupFloat64s = Dedup[float64]
