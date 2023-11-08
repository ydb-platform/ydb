package slices

// EqualUnordered checks if slices of type E are equal, order independent.
func EqualUnordered[E comparable](a []E, b []E) bool {
	if len(a) != len(b) {
		return false
	}

	ma := make(map[E]int)
	for _, v := range a {
		ma[v]++
	}
	for _, v := range b {
		if ma[v] == 0 {
			return false
		}
		ma[v]--
	}
	return true
}

// EqualAnyOrderStrings checks if string slices are equal, order independent.
// Deprecated: use EqualUnordered instead.
var EqualAnyOrderStrings = EqualUnordered[string]
