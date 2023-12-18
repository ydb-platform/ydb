package xml

import (
	"bytes"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSortXML(t *testing.T) {
	xmlInput := bytes.NewReader([]byte(`<Root><cde>xyz</cde><abc>123</abc><xyz><item>1</item></xyz></Root>`))
	sortedXML, err := SortXML(xmlInput, false)
	expectedsortedXML := `<Root><abc>123</abc><cde>xyz</cde><xyz><item>1</item></xyz></Root>`
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if diff := cmp.Diff(sortedXML, expectedsortedXML); len(diff) != 0 {
		t.Fatalf("found diff: %v", diff)
	}
}
