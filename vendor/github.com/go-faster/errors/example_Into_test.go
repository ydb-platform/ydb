//go:build go1.18

package errors_test

import (
	"fmt"
	"os"

	"github.com/go-faster/errors"
)

func ExampleInto() {
	_, err := os.Open("non-existing")
	if err != nil {
		if pathError, ok := errors.Into[*os.PathError](err); ok {
			fmt.Println("Failed at path:", pathError.Path)
		}
	}

	// Output:
	// Failed at path: non-existing
}
