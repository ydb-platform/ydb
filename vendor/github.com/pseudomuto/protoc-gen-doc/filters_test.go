package gendoc_test

import (
	html "html/template"
	"testing"

	. "github.com/pseudomuto/protoc-gen-doc"
	"github.com/stretchr/testify/require"
)

func TestPFilter(t *testing.T) {
	tests := map[string]string{
		"Some content.":                          "<p>Some content.</p>",
		"Some content.\nRight here.":             "<p>Some content.</p><p>Right here.</p>",
		"Some content.\r\nRight here.":           "<p>Some content.</p><p>Right here.</p>",
		"Some content.\n\tRight here.":           "<p>Some content.</p><p>Right here.</p>",
		"Some content.\r\n\n  \r\n  Right here.": "<p>Some content.</p><p>Right here.</p>",
	}

	for input, output := range tests {
		require.Equal(t, html.HTML(output), PFilter(input))
	}
}

func TestParaFilter(t *testing.T) {
	tests := map[string]string{
		"Some content.":                          "<para>Some content.</para>",
		"Some content.\nRight here.":             "<para>Some content.</para><para>Right here.</para>",
		"Some content.\r\nRight here.":           "<para>Some content.</para><para>Right here.</para>",
		"Some content.\n\tRight here.":           "<para>Some content.</para><para>Right here.</para>",
		"Some content.\r\n\n  \r\n  Right here.": "<para>Some content.</para><para>Right here.</para>",
	}

	for input, output := range tests {
		require.Equal(t, output, ParaFilter(input))
	}
}

func TestNoBrFilter(t *testing.T) {
	tests := map[string]string{
		"My content":                     "My content",
		"My content \r\nHere.":           "My content Here.",
		"My\n content\r right\r\n here.": "My content right here.",
		"My\ncontent\rright\r\nhere.":    "My content right here.",
		"My content.\n\nMore content.":   "My content.\n\nMore content.",
	}

	for input, output := range tests {
		require.Equal(t, output, NoBrFilter(input))
	}
}

func TestAnchorFilter(t *testing.T) {
	tests := map[string]string{
		"com/example/test.proto":  "com_example_test-proto",
		"com.example.SomeRequest": "com-example-SomeRequest",
		"héllô":                   "h-ll-",
		"un_modified-Content":     "un_modified-Content",
	}

	for input, output := range tests {
		require.Equal(t, output, AnchorFilter(input))
	}
}
