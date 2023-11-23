package headers_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb/library/go/httputil/headers"
)

// examples for tests taken from https://tools.ietf.org/html/rfc2616#section-14.3
func TestParseAcceptEncoding(t *testing.T) {
	testCases := []struct {
		name        string
		input       string
		expected    headers.AcceptableEncodings
		expectedErr error
	}{
		{
			"ietf_example_1",
			"compress, gzip",
			headers.AcceptableEncodings{
				{Encoding: headers.ContentEncoding("compress"), Weight: 1.0},
				{Encoding: headers.ContentEncoding("gzip"), Weight: 1.0},
			},
			nil,
		},
		{
			"ietf_example_2",
			"",
			nil,
			nil,
		},
		{
			"ietf_example_3",
			"*",
			headers.AcceptableEncodings{
				{Encoding: headers.ContentEncoding("*"), Weight: 1.0},
			},
			nil,
		},
		{
			"ietf_example_4",
			"compress;q=0.5, gzip;q=1.0",
			headers.AcceptableEncodings{
				{Encoding: headers.ContentEncoding("gzip"), Weight: 1.0},
				{Encoding: headers.ContentEncoding("compress"), Weight: 0.5},
			},
			nil,
		},
		{
			"ietf_example_5",
			"gzip;q=1.0, identity; q=0.5, *;q=0",
			headers.AcceptableEncodings{
				{Encoding: headers.ContentEncoding("gzip"), Weight: 1.0},
				{Encoding: headers.ContentEncoding("identity"), Weight: 0.5},
				{Encoding: headers.ContentEncoding("*"), Weight: 0},
			},
			nil,
		},
		{
			"solomon_headers",
			"zstd,lz4,gzip,deflate",
			headers.AcceptableEncodings{
				{Encoding: headers.ContentEncoding("zstd"), Weight: 1.0},
				{Encoding: headers.ContentEncoding("lz4"), Weight: 1.0},
				{Encoding: headers.ContentEncoding("gzip"), Weight: 1.0},
				{Encoding: headers.ContentEncoding("deflate"), Weight: 1.0},
			},
			nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			acceptableEncodings, err := headers.ParseAcceptEncoding(tc.input)

			if tc.expectedErr != nil {
				assert.EqualError(t, err, tc.expectedErr.Error())
			} else {
				assert.NoError(t, err)
			}

			require.Len(t, acceptableEncodings, len(tc.expected))

			opt := cmpopts.IgnoreUnexported(headers.AcceptableEncoding{})
			assert.True(t, cmp.Equal(tc.expected, acceptableEncodings, opt), cmp.Diff(tc.expected, acceptableEncodings, opt))
		})
	}
}

func TestParseAccept(t *testing.T) {
	testCases := []struct {
		name        string
		input       string
		expected    headers.AcceptableTypes
		expectedErr error
	}{
		{
			"empty_header",
			"",
			nil,
			nil,
		},
		{
			"accept_any",
			"*/*",
			headers.AcceptableTypes{
				{Type: headers.ContentTypeAny, Weight: 1.0},
			},
			nil,
		},
		{
			"accept_single",
			"application/json",
			headers.AcceptableTypes{
				{Type: headers.TypeApplicationJSON, Weight: 1.0},
			},
			nil,
		},
		{
			"accept_multiple",
			"application/json, application/protobuf",
			headers.AcceptableTypes{
				{Type: headers.TypeApplicationJSON, Weight: 1.0},
				{Type: headers.TypeApplicationProtobuf, Weight: 1.0},
			},
			nil,
		},
		{
			"accept_multiple_weighted",
			"application/json;q=0.8, application/protobuf",
			headers.AcceptableTypes{
				{Type: headers.TypeApplicationProtobuf, Weight: 1.0},
				{Type: headers.TypeApplicationJSON, Weight: 0.8},
			},
			nil,
		},
		{
			"accept_multiple_weighted_unsorted",
			"text/plain;q=0.5, application/protobuf, application/json;q=0.5",
			headers.AcceptableTypes{
				{Type: headers.TypeApplicationProtobuf, Weight: 1.0},
				{Type: headers.TypeTextPlain, Weight: 0.5},
				{Type: headers.TypeApplicationJSON, Weight: 0.5},
			},
			nil,
		},
		{
			"unknown_type",
			"custom/type, unknown/my_type;q=0.2",
			headers.AcceptableTypes{
				{Type: headers.ContentType("custom/type"), Weight: 1.0},
				{Type: headers.ContentType("unknown/my_type"), Weight: 0.2},
			},
			nil,
		},
		{
			"yabro_19.6.0",
			"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
			headers.AcceptableTypes{
				{Type: headers.ContentType("text/html"), Weight: 1.0},
				{Type: headers.ContentType("application/xhtml+xml"), Weight: 1.0},
				{Type: headers.ContentType("image/webp"), Weight: 1.0},
				{Type: headers.ContentType("image/apng"), Weight: 1.0},
				{Type: headers.ContentType("application/signed-exchange"), Weight: 1.0, Extension: map[string]string{"v": "b3"}},
				{Type: headers.ContentType("application/xml"), Weight: 0.9},
				{Type: headers.ContentType("*/*"), Weight: 0.8},
			},
			nil,
		},
		{
			"chrome_81.0.4044",
			"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
			headers.AcceptableTypes{
				{Type: headers.ContentType("text/html"), Weight: 1.0},
				{Type: headers.ContentType("application/xhtml+xml"), Weight: 1.0},
				{Type: headers.ContentType("image/webp"), Weight: 1.0},
				{Type: headers.ContentType("image/apng"), Weight: 1.0},
				{Type: headers.ContentType("application/xml"), Weight: 0.9},
				{Type: headers.ContentType("application/signed-exchange"), Weight: 0.9, Extension: map[string]string{"v": "b3"}},
				{Type: headers.ContentType("*/*"), Weight: 0.8},
			},
			nil,
		},
		{
			"firefox_77.0b3",
			"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
			headers.AcceptableTypes{
				{Type: headers.ContentType("text/html"), Weight: 1.0},
				{Type: headers.ContentType("application/xhtml+xml"), Weight: 1.0},
				{Type: headers.ContentType("image/webp"), Weight: 1.0},
				{Type: headers.ContentType("application/xml"), Weight: 0.9},
				{Type: headers.ContentType("*/*"), Weight: 0.8},
			},
			nil,
		},
		{
			"sort_by_most_specific",
			"text/*, text/html, */*, text/html;level=1",
			headers.AcceptableTypes{
				{Type: headers.ContentType("text/html"), Weight: 1.0, Extension: map[string]string{"level": "1"}},
				{Type: headers.ContentType("text/html"), Weight: 1.0},
				{Type: headers.ContentType("text/*"), Weight: 1.0},
				{Type: headers.ContentType("*/*"), Weight: 1.0},
			},
			nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			at, err := headers.ParseAccept(tc.input)

			if tc.expectedErr != nil {
				assert.EqualError(t, err, tc.expectedErr.Error())
			} else {
				assert.NoError(t, err)
			}

			require.Len(t, at, len(tc.expected))

			opt := cmpopts.IgnoreUnexported(headers.AcceptableType{})
			assert.True(t, cmp.Equal(tc.expected, at, opt), cmp.Diff(tc.expected, at, opt))
		})
	}
}

func TestAcceptableTypesString(t *testing.T) {
	testCases := []struct {
		name     string
		types    headers.AcceptableTypes
		expected string
	}{
		{
			"empty",
			headers.AcceptableTypes{},
			"",
		},
		{
			"single",
			headers.AcceptableTypes{
				{Type: headers.TypeApplicationJSON},
			},
			"application/json",
		},
		{
			"single_weighted",
			headers.AcceptableTypes{
				{Type: headers.TypeApplicationJSON, Weight: 0.8},
			},
			"application/json;q=0.8",
		},
		{
			"multiple",
			headers.AcceptableTypes{
				{Type: headers.TypeApplicationJSON},
				{Type: headers.TypeApplicationProtobuf},
			},
			"application/json, application/protobuf",
		},
		{
			"multiple_weighted",
			headers.AcceptableTypes{
				{Type: headers.TypeApplicationProtobuf},
				{Type: headers.TypeApplicationJSON, Weight: 0.8},
			},
			"application/protobuf, application/json;q=0.8",
		},
		{
			"multiple_weighted_with_extension",
			headers.AcceptableTypes{
				{Type: headers.TypeApplicationProtobuf},
				{Type: headers.TypeApplicationJSON, Weight: 0.8},
				{Type: headers.TypeApplicationXML, Weight: 0.5, Extension: map[string]string{"label": "1"}},
			},
			"application/protobuf, application/json;q=0.8, application/xml;q=0.5;label=1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.types.String())
		})
	}
}

func BenchmarkParseAccept(b *testing.B) {
	benchCases := []string{
		"",
		"*/*",
		"application/json",
		"application/json, application/protobuf",
		"application/json;q=0.8, application/protobuf",
		"text/plain;q=0.5, application/protobuf, application/json;q=0.5",
		"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
		"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
		"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
		"text/*, text/html, */*, text/html;level=1",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = headers.ParseAccept(benchCases[i%len(benchCases)])
	}
}
