package testhelpers

// Recurse calls itself 'depth' times then executes 'f'. Useful for testing things where stack size matters.
func Recurse(depth int, f func()) {
	if depth > 0 {
		depth--
		Recurse(depth, f)
		return
	}

	f()
}
