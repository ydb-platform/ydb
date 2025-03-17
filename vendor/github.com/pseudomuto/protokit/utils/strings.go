package utils

// InStringSlice returns whether or not the supplied value is in the slice
func InStringSlice(sl []string, val string) bool {
	for _, s := range sl {
		if s == val {
			return true
		}
	}

	return false
}
