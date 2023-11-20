// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package json_test

import (
	"fmt"
	"log"
	"strings"

	"github.com/goccy/go-json"
)

type Size int

const (
	unrecognized Size = iota
	small
	large
)

func (s *Size) UnmarshalText(text []byte) error {
	switch strings.ToLower(string(text)) {
	default:
		*s = unrecognized
	case "small":
		*s = small
	case "large":
		*s = large
	}
	return nil
}

func (s Size) MarshalText() ([]byte, error) {
	var name string
	switch s {
	default:
		name = "unrecognized"
	case small:
		name = "small"
	case large:
		name = "large"
	}
	return []byte(name), nil
}

func Example_textMarshalJSON() {
	blob := `["small","regular","large","unrecognized","small","normal","small","large"]`
	var inventory []Size
	if err := json.Unmarshal([]byte(blob), &inventory); err != nil {
		log.Fatal(err)
	}

	counts := make(map[Size]int)
	for _, size := range inventory {
		counts[size] += 1
	}

	fmt.Printf("Inventory Counts:\n* Small:        %d\n* Large:        %d\n* Unrecognized: %d\n",
		counts[small], counts[large], counts[unrecognized])

	// Output:
	// Inventory Counts:
	// * Small:        3
	// * Large:        2
	// * Unrecognized: 3
}
