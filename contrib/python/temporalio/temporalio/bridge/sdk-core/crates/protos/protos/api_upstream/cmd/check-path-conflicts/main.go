// check-path-conflicts reads an OpenAPI v2 JSON file and detects HTTP path
// conflicts where a literal path segment and a parameterized segment overlap at
// the same position (e.g. /items/pause vs /items/{id}).
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

type openAPISpec struct {
	Paths map[string]map[string]json.RawMessage `json:"paths"`
}

// httpMethods are the valid HTTP method keys in an OpenAPI path item.
var httpMethods = map[string]bool{
	"get": true, "put": true, "post": true, "delete": true,
	"options": true, "head": true, "patch": true,
}

// segment represents one piece of a URL path.
type segment struct {
	value string
	param bool // true when the segment is a path parameter like {id}
}

func parseSegments(path string) []segment {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	segs := make([]segment, len(parts))
	for i, p := range parts {
		segs[i] = segment{
			value: p,
			param: strings.HasPrefix(p, "{") && strings.HasSuffix(p, "}"),
		}
	}
	return segs
}

// Two paths conflict when they have the same number of segments and at every
// position, they either match literally or at least one of them is a parameter,
// AND there is at least one position where one path has a literal and the other
// has a parameter (otherwise they are the same path or differ only in parameter
// names, which is a different issue).
func conflicts(a, b []segment) bool {
	if len(a) != len(b) {
		return false
	}
	hasParamLiteralMismatch := false
	for i := range a {
		aParam := a[i].param
		bParam := b[i].param
		if !aParam && !bParam {
			// Both literals — must match exactly.
			if a[i].value != b[i].value {
				return false
			}
		} else if aParam != bParam {
			// One is a param, the other is a literal — potential conflict.
			hasParamLiteralMismatch = true
		}
		// Both params — always compatible at this position, continue.
	}
	return hasParamLiteralMismatch
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <openapi-v2.json>\n", os.Args[0])
		os.Exit(2)
	}

	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading file: %v\n", err)
		os.Exit(2)
	}

	var spec openAPISpec
	if err := json.Unmarshal(data, &spec); err != nil {
		fmt.Fprintf(os.Stderr, "error parsing JSON: %v\n", err)
		os.Exit(2)
	}

	type parsedPath struct {
		raw      string
		methods  map[string]bool
		segments []segment
	}

	var parsed []parsedPath
	for p, item := range spec.Paths {
		methods := make(map[string]bool)
		for key := range item {
			if httpMethods[strings.ToLower(key)] {
				methods[strings.ToLower(key)] = true
			}
		}
		parsed = append(parsed, parsedPath{raw: p, methods: methods, segments: parseSegments(p)})
	}
	sort.Slice(parsed, func(i, j int) bool { return parsed[i].raw < parsed[j].raw })

	var found []string
	for i := 0; i < len(parsed); i++ {
		for j := i + 1; j < len(parsed); j++ {
			if !conflicts(parsed[i].segments, parsed[j].segments) {
				continue
			}
			// Find overlapping HTTP methods.
			var shared []string
			for m := range parsed[i].methods {
				if parsed[j].methods[m] {
					shared = append(shared, strings.ToUpper(m))
				}
			}
			if len(shared) == 0 {
				continue
			}
			sort.Strings(shared)
			found = append(found, fmt.Sprintf("  %s\n  %s\n  methods: %s",
				parsed[i].raw, parsed[j].raw, strings.Join(shared, ", ")))
		}
	}

	if len(found) > 0 {
		fmt.Fprintf(os.Stderr, "found %d path conflict(s):\n\n", len(found))
		for _, f := range found {
			fmt.Fprintln(os.Stderr, f)
			fmt.Fprintln(os.Stderr)
		}
		os.Exit(1)
	}

	fmt.Println("no path conflicts found")
}
