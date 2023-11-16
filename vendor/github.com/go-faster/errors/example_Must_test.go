//go:build go1.18

package errors_test

import (
	"fmt"
	"net/url"

	"github.com/go-faster/errors"
)

func ExampleMust() {
	r := errors.Must(url.Parse(`https://google.com`))
	fmt.Println(r.String())

	// Output:
	// https://google.com
}
