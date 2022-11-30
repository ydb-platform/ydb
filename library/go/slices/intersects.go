package slices

// Intersection returns intersection for slices of various built-in types
func Intersection[E comparable](a, b []E) []E {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}

	p, s := a, b
	if len(b) > len(a) {
		p, s = b, a
	}

	m := make(map[E]struct{})
	for _, i := range p {
		m[i] = struct{}{}
	}

	var res []E
	for _, v := range s {
		if _, exists := m[v]; exists {
			res = append(res, v)
		}
	}

	return res
}

// IntersectStrings returns intersection of two string slices
// Deprecated: use Intersection instead.
var IntersectStrings = Intersection[string]

// IntersectInts returns intersection of two int slices
// Deprecated: use Intersection instead.
var IntersectInts = Intersection[int]

// IntersectInt8s returns intersection of two int8 slices
// Deprecated: use Intersection instead.
var IntersectInt8s = Intersection[int8]

// IntersectInt16s returns intersection of two int16 slices
// Deprecated: use Intersection instead.
var IntersectInt16s = Intersection[int16]

// IntersectInt32s returns intersection of two int32 slices
// Deprecated: use Intersection instead.
var IntersectInt32s = Intersection[int32]

// IntersectInt64s returns intersection of two int64 slices
// Deprecated: use Intersection instead.
var IntersectInt64s = Intersection[int64]

// IntersectUints returns intersection of two uint slices
// Deprecated: use Intersection instead.
var IntersectUints = Intersection[uint]

// IntersectUint8s returns intersection of two uint8 slices
// Deprecated: use Intersection instead.
var IntersectUint8s = Intersection[uint8]

// IntersectUint16s returns intersection of two uint16 slices
// Deprecated: use Intersection instead.
var IntersectUint16s = Intersection[uint16]

// IntersectUint32s returns intersection of two uint32 slices
// Deprecated: use Intersection instead.
var IntersectUint32s = Intersection[uint32]

// IntersectUint64s returns intersection of two uint64 slices
// Deprecated: use Intersection instead.
var IntersectUint64s = Intersection[uint64]

// IntersectFloat32s returns intersection of two float32 slices
// Deprecated: use Intersection instead.
var IntersectFloat32s = Intersection[float32]

// IntersectFloat64s returns intersection of two float64 slices
// Deprecated: use Intersection instead.
var IntersectFloat64s = Intersection[float64]

// IntersectBools returns intersection of two bool slices
// Deprecated: use Intersection instead.
var IntersectBools = Intersection[bool]
