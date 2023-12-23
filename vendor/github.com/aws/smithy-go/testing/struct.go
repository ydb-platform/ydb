package testing

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/aws/smithy-go/document"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/google/go-cmp/cmp"
)

// CompareValues compares two values to determine if they are equal.
func CompareValues(expect, actual interface{}, opts ...cmp.Option) error {
	opts = append(make([]cmp.Option, 0, len(opts)+1), opts...)

	var skippedReaders filterSkipDifferentIoReader

	opts = append(opts,
		cmp.Transformer("http.NoBody", transformHTTPNoBodyToNil),
		cmp.FilterValues(skippedReaders.filter, cmp.Ignore()),
		cmp.Comparer(compareDocumentTypes),
	)

	if diff := cmp.Diff(expect, actual, opts...); len(diff) != 0 {
		return fmt.Errorf("values do not match\n%s", diff)
	}

	var errs []error
	for _, s := range skippedReaders {
		if err := CompareReaders(s.A, s.B); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		return fmt.Errorf("io.Readers have different values\n%v", errs)
	}

	return nil
}

type documentInterface interface {
	document.Marshaler
	document.Unmarshaler
}

func compareDocumentTypes(x documentInterface, y documentInterface) bool {
	xBytes, err := x.MarshalSmithyDocument()
	if err != nil {
		panic(fmt.Sprintf("MarshalSmithyDocument error: %v", err))
	}
	yBytes, err := y.MarshalSmithyDocument()
	if err != nil {
		panic(fmt.Sprintf("MarshalSmithyDocument error: %v", err))
	}
	return JSONEqual(xBytes, yBytes) == nil
}

func transformHTTPNoBodyToNil(v io.Reader) io.Reader {
	if v == http.NoBody {
		return nil
	}
	return v
}

type filterSkipDifferentIoReader []skippedReaders

func (f *filterSkipDifferentIoReader) filter(a, b io.Reader) bool {
	if a == nil || b == nil {
		return false
	}
	//at, bt := reflect.TypeOf(a), reflect.TypeOf(b)
	//for at.Kind() == reflect.Ptr {
	//	at = at.Elem()
	//}
	//for bt.Kind() == reflect.Ptr {
	//	bt = bt.Elem()
	//}

	//// The underlying reader types are the same they can be compared directly.
	//if at == bt {
	//	return false
	//}

	*f = append(*f, skippedReaders{A: a, B: b})
	return true
}

type skippedReaders struct {
	A, B io.Reader
}

// CompareReaders two io.Reader values together to determine if they are equal.
// Will read the contents of the readers until they are empty.
func CompareReaders(expect, actual io.Reader) error {
	e, err := ioutil.ReadAll(expect)
	if err != nil {
		return fmt.Errorf("failed to read expect body, %w", err)
	}

	a, err := ioutil.ReadAll(actual)
	if err != nil {
		return fmt.Errorf("failed to read actual body, %w", err)
	}

	if !bytes.Equal(e, a) {
		return fmt.Errorf("bytes do not match\nexpect:\n%s\nactual:\n%s",
			hex.Dump(e), hex.Dump(a))
	}

	return nil
}
