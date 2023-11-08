// Package resource provides integration with RESOURCE and RESOURCE_FILES macros.
//
// Use RESOURCE macro to "link" file into the library or executable.
//
//	RESOURCE(my_file.txt some_key)
//
// And then retrieve file content in the runtime.
//
//	blob := resource.Get("some_key")
//
// Warning: Excessive consumption of resource leads to obesity.
package resource

import (
	"fmt"
	"sort"
)

var resources = map[string][]byte{}

// InternalRegister is private API used by generated code.
func InternalRegister(key string, blob []byte) {
	if _, ok := resources[key]; ok {
		panic(fmt.Sprintf("resource key %q is already defined", key))
	}

	resources[key] = blob
}

// Get returns content of the file registered by the given key.
//
// If no file was registered for the given key, nil slice is returned.
//
// User should take care, to avoid mutating returned slice.
func Get(key string) []byte {
	return resources[key]
}

// MustGet is like Get, but panics when associated resource is not defined.
func MustGet(key string) []byte {
	r, ok := resources[key]
	if !ok {
		panic(fmt.Sprintf("resource with key %q is not defined", key))
	}
	return r
}

// Keys returns sorted keys of all registered resources inside the binary
func Keys() []string {
	keys := make([]string, 0, len(resources))
	for k := range resources {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
