package slices

import (
	"fmt"
	"strings"
)

// Join joins slice of any types
func Join(s interface{}, glue string) string {
	if t, ok := s.([]string); ok {
		return strings.Join(t, glue)
	}
	return strings.Trim(strings.Join(strings.Fields(fmt.Sprint(s)), glue), "[]")
}
