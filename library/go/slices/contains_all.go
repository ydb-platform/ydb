package slices

// ContainsAll checks if slice of type E contains all elements of given slice, order independent
func ContainsAll[E comparable](haystack []E, needle []E) bool {
	m := make(map[E]struct{}, len(haystack))
	for _, i := range haystack {
		m[i] = struct{}{}
	}
	for _, v := range needle {
		if _, ok := m[v]; !ok {
			return false
		}
	}
	return true
}

// ContainsAllStrings checks if string slice contains all elements of given slice
// Deprecated: use ContainsAll instead
var ContainsAllStrings = ContainsAll[string]

// ContainsAllBools checks if bool slice contains all elements of given slice
// Deprecated: use ContainsAll instead
var ContainsAllBools = ContainsAll[bool]
