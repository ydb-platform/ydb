//go:build go1.16
// +build go1.16

package checksum

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"testing/iotest"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/smithy-go/logging"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/google/go-cmp/cmp"
)

// TODO test cases:
//    * Retry re-wrapping payload

func TestComputeInputPayloadChecksum(t *testing.T) {
	cases := map[string]map[string]struct {
		optionsFn   func(*computeInputPayloadChecksum)
		initContext func(context.Context) context.Context
		buildInput  middleware.BuildInput

		expectErr         string
		expectBuildErr    bool
		expectFinalizeErr bool
		expectReadErr     bool

		expectHeader        http.Header
		expectContentLength int64
		expectPayload       []byte
		expectPayloadHash   string

		expectChecksumMetadata map[string]string

		expectDeferToFinalize bool
		expectLogged          string
	}{
		"no op": {
			"checksum header set known length": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("https://example.aws")
						r.Header.Set(AlgorithmHTTPHeader(AlgorithmCRC32), "AAAAAA==")
						r = requestMust(r.SetStream(strings.NewReader("hello world")))
						r.ContentLength = 11
						return r
					}(),
				},
				expectHeader: http.Header{
					"X-Amz-Checksum-Crc32": []string{"AAAAAA=="},
				},
				expectContentLength: 11,
				expectPayload:       []byte("hello world"),
				expectChecksumMetadata: map[string]string{
					"CRC32": "AAAAAA==",
				},
			},
			"checksum header set unknown length": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("https://example.aws")
						r.Header.Set(AlgorithmHTTPHeader(AlgorithmCRC32), "AAAAAA==")
						r = requestMust(r.SetStream(strings.NewReader("hello world")))
						r.ContentLength = -1
						return r
					}(),
				},
				expectHeader: http.Header{
					"X-Amz-Checksum-Crc32": []string{"AAAAAA=="},
				},
				expectContentLength: -1,
				expectPayload:       []byte("hello world"),
				expectChecksumMetadata: map[string]string{
					"CRC32": "AAAAAA==",
				},
			},
			"no algorithm": {
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("https://example.aws")
						r = requestMust(r.SetStream(strings.NewReader("hello world")))
						r.ContentLength = 11
						return r
					}(),
				},
				expectHeader:        http.Header{},
				expectContentLength: 11,
				expectPayload:       []byte("hello world"),
			},
			"nil stream no algorithm require checksum": {
				optionsFn: func(o *computeInputPayloadChecksum) {
					o.RequireChecksum = true
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("http://example.aws")
						return r
					}(),
				},
				expectContentLength: -1,
				expectHeader:        http.Header{},
			},
		},

		"build handled": {
			"http nil stream": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("http://example.aws")
						return r
					}(),
				},
				expectHeader: http.Header{
					"X-Amz-Checksum-Crc32": []string{"AAAAAA=="},
				},
				expectContentLength: -1,
				expectPayloadHash:   "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				expectChecksumMetadata: map[string]string{
					"CRC32": "AAAAAA==",
				},
			},
			"http empty stream": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("http://example.aws")
						r.ContentLength = 0
						r = requestMust(r.SetStream(strings.NewReader("")))
						return r
					}(),
				},
				expectHeader: http.Header{
					"X-Amz-Checksum-Crc32": []string{"AAAAAA=="},
				},
				expectContentLength: 0,
				expectPayload:       []byte{},
				expectPayloadHash:   "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				expectChecksumMetadata: map[string]string{
					"CRC32": "AAAAAA==",
				},
			},
			"https empty stream unseekable": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("https://example.aws")
						r.ContentLength = 0
						r = requestMust(r.SetStream(&bytes.Buffer{}))
						return r
					}(),
				},
				expectHeader: http.Header{
					"X-Amz-Checksum-Crc32": []string{"AAAAAA=="},
				},
				expectContentLength: 0,
				expectPayload:       nil,
				expectPayloadHash:   "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				expectChecksumMetadata: map[string]string{
					"CRC32": "AAAAAA==",
				},
			},
			"http empty stream unseekable": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("http://example.aws")
						r.ContentLength = 0
						r = requestMust(r.SetStream(&bytes.Buffer{}))
						return r
					}(),
				},
				expectHeader: http.Header{
					"X-Amz-Checksum-Crc32": []string{"AAAAAA=="},
				},
				expectContentLength: 0,
				expectPayload:       nil,
				expectPayloadHash:   "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				expectChecksumMetadata: map[string]string{
					"CRC32": "AAAAAA==",
				},
			},
			"https nil stream": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("https://example.aws")
						return r
					}(),
				},
				expectHeader: http.Header{
					"X-Amz-Checksum-Crc32": []string{"AAAAAA=="},
				},
				expectContentLength: -1,
				expectChecksumMetadata: map[string]string{
					"CRC32": "AAAAAA==",
				},
			},
			"https empty stream": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("https://example.aws")
						r.ContentLength = 0
						r = requestMust(r.SetStream(strings.NewReader("")))
						return r
					}(),
				},
				expectHeader: http.Header{
					"X-Amz-Checksum-Crc32": []string{"AAAAAA=="},
				},
				expectContentLength: 0,
				expectPayload:       []byte{},
				expectChecksumMetadata: map[string]string{
					"CRC32": "AAAAAA==",
				},
			},
			"http no algorithm require checksum": {
				optionsFn: func(o *computeInputPayloadChecksum) {
					o.RequireChecksum = true
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("http://example.aws")
						r.ContentLength = 11
						r = requestMust(r.SetStream(bytes.NewReader([]byte("hello world"))))
						return r
					}(),
				},
				expectHeader: http.Header{
					"Content-Md5": []string{"XrY7u+Ae7tCTyyK7j1rNww=="},
				},
				expectContentLength: 11,
				expectPayload:       []byte("hello world"),
				expectChecksumMetadata: map[string]string{
					"MD5": "XrY7u+Ae7tCTyyK7j1rNww==",
				},
			},
			"http no algorithm require checksum header preset": {
				optionsFn: func(o *computeInputPayloadChecksum) {
					o.RequireChecksum = true
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("http://example.aws")
						r.ContentLength = 11
						r.Header.Set("Content-MD5", "XrY7u+Ae7tCTyyK7j1rNww==")
						r = requestMust(r.SetStream(bytes.NewReader([]byte("hello world"))))
						return r
					}(),
				},
				expectHeader: http.Header{
					"Content-Md5": []string{"XrY7u+Ae7tCTyyK7j1rNww=="},
				},
				expectContentLength: 11,
				expectPayload:       []byte("hello world"),
				expectChecksumMetadata: map[string]string{
					"MD5": "XrY7u+Ae7tCTyyK7j1rNww==",
				},
			},
			"https no algorithm require checksum": {
				optionsFn: func(o *computeInputPayloadChecksum) {
					o.RequireChecksum = true
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("https://example.aws")
						r.ContentLength = 11
						r = requestMust(r.SetStream(bytes.NewReader([]byte("hello world"))))
						return r
					}(),
				},
				expectHeader: http.Header{
					"Content-Md5": []string{"XrY7u+Ae7tCTyyK7j1rNww=="},
				},
				expectContentLength: 11,
				expectPayload:       []byte("hello world"),
				expectChecksumMetadata: map[string]string{
					"MD5": "XrY7u+Ae7tCTyyK7j1rNww==",
				},
			},
			"http seekable": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("http://example.aws")
						r.ContentLength = 11
						r = requestMust(r.SetStream(bytes.NewReader([]byte("hello world"))))
						return r
					}(),
				},
				expectHeader: http.Header{
					"X-Amz-Checksum-Crc32": []string{"DUoRhQ=="},
				},
				expectContentLength: 11,
				expectPayload:       []byte("hello world"),
				expectPayloadHash:   "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
				expectChecksumMetadata: map[string]string{
					"CRC32": "DUoRhQ==",
				},
			},
			"http payload hash already set": {
				initContext: func(ctx context.Context) context.Context {
					ctx = setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
					ctx = v4.SetPayloadHash(ctx, "somehash")
					return ctx
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("http://example.aws")
						r.ContentLength = 11
						r = requestMust(r.SetStream(bytes.NewReader([]byte("hello world"))))
						return r
					}(),
				},
				expectHeader: http.Header{
					"X-Amz-Checksum-Crc32": []string{"DUoRhQ=="},
				},
				expectContentLength: 11,
				expectPayload:       []byte("hello world"),
				expectPayloadHash:   "somehash",
				expectChecksumMetadata: map[string]string{
					"CRC32": "DUoRhQ==",
				},
			},
			"http seekable checksum matches payload hash": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmSHA256))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("http://example.aws")
						r.ContentLength = 11
						r = requestMust(r.SetStream(bytes.NewReader([]byte("hello world"))))
						return r
					}(),
				},
				expectHeader: http.Header{
					"X-Amz-Checksum-Sha256": []string{"uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek="},
				},
				expectContentLength: 11,
				expectPayload:       []byte("hello world"),
				expectPayloadHash:   "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
				expectChecksumMetadata: map[string]string{
					"SHA256": "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=",
				},
			},
			"http payload hash disabled": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				optionsFn: func(o *computeInputPayloadChecksum) {
					o.EnableComputePayloadHash = false
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("http://example.aws")
						r.ContentLength = 11
						r = requestMust(r.SetStream(bytes.NewReader([]byte("hello world"))))
						return r
					}(),
				},
				expectHeader: http.Header{
					"X-Amz-Checksum-Crc32": []string{"DUoRhQ=="},
				},
				expectContentLength: 11,
				expectPayload:       []byte("hello world"),
				expectChecksumMetadata: map[string]string{
					"CRC32": "DUoRhQ==",
				},
			},
			"https no trailing checksum": {
				optionsFn: func(o *computeInputPayloadChecksum) {
					o.EnableTrailingChecksum = false
				},
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("https://example.aws")
						r.ContentLength = 11
						r = requestMust(r.SetStream(bytes.NewReader([]byte("hello world"))))
						return r
					}(),
				},
				expectHeader: http.Header{
					"X-Amz-Checksum-Crc32": []string{"DUoRhQ=="},
				},
				expectContentLength: 11,
				expectPayload:       []byte("hello world"),
				expectChecksumMetadata: map[string]string{
					"CRC32": "DUoRhQ==",
				},
			},
			"with content encoding set": {
				optionsFn: func(o *computeInputPayloadChecksum) {
					o.EnableTrailingChecksum = false
				},
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("https://example.aws")
						r.ContentLength = 11
						r.Header.Set("Content-Encoding", "gzip")
						r = requestMust(r.SetStream(bytes.NewReader([]byte("hello world"))))
						return r
					}(),
				},
				expectHeader: http.Header{
					"X-Amz-Checksum-Crc32": []string{"DUoRhQ=="},
					"Content-Encoding":     []string{"gzip"},
				},
				expectContentLength: 11,
				expectPayload:       []byte("hello world"),
				expectChecksumMetadata: map[string]string{
					"CRC32": "DUoRhQ==",
				},
			},
		},

		"build error": {
			"unknown algorithm": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string("unknown"))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("http://example.aws")
						r = requestMust(r.SetStream(bytes.NewBuffer([]byte("hello world"))))
						return r
					}(),
				},
				expectErr:      "failed to parse algorithm",
				expectBuildErr: true,
			},
			"no algorithm require checksum unseekable stream": {
				optionsFn: func(o *computeInputPayloadChecksum) {
					o.RequireChecksum = true
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("http://example.aws")
						r = requestMust(r.SetStream(bytes.NewBuffer([]byte("hello world"))))
						return r
					}(),
				},
				expectErr:      "unseekable stream is not supported",
				expectBuildErr: true,
			},
			"http unseekable stream": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("http://example.aws")
						r = requestMust(r.SetStream(bytes.NewBuffer([]byte("hello world"))))
						return r
					}(),
				},
				expectErr:      "unseekable stream is not supported",
				expectBuildErr: true,
			},
			"http stream read error": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("http://example.aws")
						r.ContentLength = 128
						r = requestMust(r.SetStream(&mockReadSeeker{
							Reader: iotest.ErrReader(fmt.Errorf("read error")),
						}))
						return r
					}(),
				},
				expectErr:      "failed to read stream to compute hash",
				expectBuildErr: true,
			},
			"http stream rewind error": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("http://example.aws")
						r.ContentLength = 11
						r = requestMust(r.SetStream(&errSeekReader{
							Reader: strings.NewReader("hello world"),
						}))
						return r
					}(),
				},
				expectErr:      "failed to rewind stream",
				expectBuildErr: true,
			},
			"https no trailing unseekable stream": {
				optionsFn: func(o *computeInputPayloadChecksum) {
					o.EnableTrailingChecksum = false
				},
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("https://example.aws")
						r = requestMust(r.SetStream(bytes.NewBuffer([]byte("hello world"))))
						return r
					}(),
				},
				expectErr:      "unseekable stream is not supported",
				expectBuildErr: true,
			},
		},

		"finalize handled": {
			"https unseekable": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("https://example.aws")
						r.ContentLength = 11
						r = requestMust(r.SetStream(bytes.NewBuffer([]byte("hello world"))))
						return r
					}(),
				},
				expectHeader: http.Header{
					"Content-Encoding":             []string{"aws-chunked"},
					"X-Amz-Decoded-Content-Length": []string{"11"},
					"X-Amz-Trailer":                []string{"x-amz-checksum-crc32"},
				},
				expectContentLength:   52,
				expectPayload:         []byte("b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:DUoRhQ==\r\n\r\n"),
				expectPayloadHash:     "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
				expectDeferToFinalize: true,
				expectChecksumMetadata: map[string]string{
					"CRC32": "DUoRhQ==",
				},
			},
			"https unseekable unknown length": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("https://example.aws")
						r.ContentLength = -1
						r = requestMust(r.SetStream(ioutil.NopCloser(bytes.NewBuffer([]byte("hello world")))))
						return r
					}(),
				},
				expectHeader: http.Header{
					"Content-Encoding": []string{"aws-chunked"},
					"X-Amz-Trailer":    []string{"x-amz-checksum-crc32"},
				},
				expectContentLength:   -1,
				expectPayload:         []byte("b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:DUoRhQ==\r\n\r\n"),
				expectPayloadHash:     "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
				expectDeferToFinalize: true,
				expectChecksumMetadata: map[string]string{
					"CRC32": "DUoRhQ==",
				},
			},
			"https seekable": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("https://example.aws")
						r.ContentLength = 11
						r = requestMust(r.SetStream(bytes.NewReader([]byte("hello world"))))
						return r
					}(),
				},
				expectHeader: http.Header{
					"Content-Encoding":             []string{"aws-chunked"},
					"X-Amz-Decoded-Content-Length": []string{"11"},
					"X-Amz-Trailer":                []string{"x-amz-checksum-crc32"},
				},
				expectContentLength:   52,
				expectPayload:         []byte("b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:DUoRhQ==\r\n\r\n"),
				expectPayloadHash:     "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
				expectDeferToFinalize: true,
				expectChecksumMetadata: map[string]string{
					"CRC32": "DUoRhQ==",
				},
			},
			"https seekable unknown length": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("https://example.aws")
						r.ContentLength = -1
						r = requestMust(r.SetStream(bytes.NewReader([]byte("hello world"))))
						return r
					}(),
				},
				expectHeader: http.Header{
					"Content-Encoding":             []string{"aws-chunked"},
					"X-Amz-Decoded-Content-Length": []string{"11"},
					"X-Amz-Trailer":                []string{"x-amz-checksum-crc32"},
				},
				expectContentLength:   52,
				expectPayload:         []byte("b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:DUoRhQ==\r\n\r\n"),
				expectPayloadHash:     "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
				expectDeferToFinalize: true,
				expectChecksumMetadata: map[string]string{
					"CRC32": "DUoRhQ==",
				},
			},
			"https no compute payload hash": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				optionsFn: func(o *computeInputPayloadChecksum) {
					o.EnableComputePayloadHash = false
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("https://example.aws")
						r.ContentLength = 11
						r = requestMust(r.SetStream(bytes.NewReader([]byte("hello world"))))
						return r
					}(),
				},
				expectHeader: http.Header{
					"Content-Encoding":             []string{"aws-chunked"},
					"X-Amz-Decoded-Content-Length": []string{"11"},
					"X-Amz-Trailer":                []string{"x-amz-checksum-crc32"},
				},
				expectContentLength:   52,
				expectPayload:         []byte("b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:DUoRhQ==\r\n\r\n"),
				expectDeferToFinalize: true,
				expectChecksumMetadata: map[string]string{
					"CRC32": "DUoRhQ==",
				},
			},
			"https no decode content length": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				optionsFn: func(o *computeInputPayloadChecksum) {
					o.EnableDecodedContentLengthHeader = false
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("https://example.aws")
						r.ContentLength = 11
						r = requestMust(r.SetStream(bytes.NewReader([]byte("hello world"))))
						return r
					}(),
				},
				expectHeader: http.Header{
					"Content-Encoding": []string{"aws-chunked"},
					"X-Amz-Trailer":    []string{"x-amz-checksum-crc32"},
				},
				expectContentLength:   52,
				expectPayloadHash:     "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
				expectPayload:         []byte("b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:DUoRhQ==\r\n\r\n"),
				expectDeferToFinalize: true,
				expectChecksumMetadata: map[string]string{
					"CRC32": "DUoRhQ==",
				},
			},
			"with content encoding set": {
				initContext: func(ctx context.Context) context.Context {
					return setContextInputAlgorithm(ctx, string(AlgorithmCRC32))
				},
				buildInput: middleware.BuildInput{
					Request: func() *smithyhttp.Request {
						r := smithyhttp.NewStackRequest().(*smithyhttp.Request)
						r.URL, _ = url.Parse("https://example.aws")
						r.ContentLength = 11
						r.Header.Set("Content-Encoding", "gzip")
						r = requestMust(r.SetStream(bytes.NewReader([]byte("hello world"))))
						return r
					}(),
				},
				expectHeader: http.Header{
					"X-Amz-Trailer":                []string{"x-amz-checksum-crc32"},
					"X-Amz-Decoded-Content-Length": []string{"11"},
					"Content-Encoding":             []string{"gzip", "aws-chunked"},
				},
				expectContentLength:   52,
				expectPayloadHash:     "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
				expectPayload:         []byte("b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:DUoRhQ==\r\n\r\n"),
				expectDeferToFinalize: true,
				expectChecksumMetadata: map[string]string{
					"CRC32": "DUoRhQ==",
				},
			},
		},
	}

	for name, cs := range cases {
		t.Run(name, func(t *testing.T) {
			for name, c := range cs {
				t.Run(name, func(t *testing.T) {
					m := &computeInputPayloadChecksum{
						EnableTrailingChecksum:           true,
						EnableComputePayloadHash:         true,
						EnableDecodedContentLengthHeader: true,
					}
					if c.optionsFn != nil {
						c.optionsFn(m)
					}

					ctx := context.Background()
					var logged bytes.Buffer
					logger := logging.LoggerFunc(
						func(classification logging.Classification, format string, v ...interface{}) {
							fmt.Fprintf(&logged, format, v...)
						},
					)

					stack := middleware.NewStack("test", smithyhttp.NewStackRequest)
					middleware.AddSetLoggerMiddleware(stack, logger)

					//------------------------------
					// Build handler
					//------------------------------
					// On return path validate any errors were expected.
					stack.Build.Add(middleware.BuildMiddlewareFunc(
						"build-assert",
						func(ctx context.Context, input middleware.BuildInput, next middleware.BuildHandler) (
							out middleware.BuildOutput, metadata middleware.Metadata, err error,
						) {
							// ignore initial build input for the test case's build input.
							out, metadata, err = next.HandleBuild(ctx, c.buildInput)
							if err == nil && c.expectBuildErr {
								t.Fatalf("expect build error, got none")
							}

							if !m.buildHandlerRun {
								t.Fatalf("expect build handler run")
							}
							return out, metadata, err
						},
					), middleware.After)

					// Build middleware
					stack.Build.Add(m, middleware.After)

					// Validate defer to finalize was performed as expected
					stack.Build.Add(middleware.BuildMiddlewareFunc(
						"assert-defer-to-finalize",
						func(ctx context.Context, input middleware.BuildInput, next middleware.BuildHandler) (
							out middleware.BuildOutput, metadata middleware.Metadata, err error,
						) {
							if e, a := c.expectDeferToFinalize, m.deferToFinalizeHandler; e != a {
								t.Fatalf("expect %v defer to finalize, got %v", e, a)
							}
							return next.HandleBuild(ctx, input)
						},
					), middleware.After)

					//------------------------------
					// Finalize handler
					//------------------------------
					if m.EnableTrailingChecksum {
						// On return path assert any errors are expected.
						stack.Finalize.Add(middleware.FinalizeMiddlewareFunc(
							"build-assert",
							func(ctx context.Context, input middleware.FinalizeInput, next middleware.FinalizeHandler) (
								out middleware.FinalizeOutput, metadata middleware.Metadata, err error,
							) {
								out, metadata, err = next.HandleFinalize(ctx, input)
								if err == nil && c.expectFinalizeErr {
									t.Fatalf("expect finalize error, got none")
								}

								return out, metadata, err
							},
						), middleware.After)

						// Add finalize middleware
						stack.Finalize.Add(m, middleware.After)
					}

					//------------------------------
					// Request validation
					//------------------------------
					validateRequestHandler := middleware.HandlerFunc(
						func(ctx context.Context, input interface{}) (
							output interface{}, metadata middleware.Metadata, err error,
						) {
							request := input.(*smithyhttp.Request)

							if diff := cmp.Diff(c.expectHeader, request.Header); diff != "" {
								t.Errorf("expect header to match:\n%s", diff)
							}
							if e, a := c.expectContentLength, request.ContentLength; e != a {
								t.Errorf("expect %v content length, got %v", e, a)
							}

							stream := request.GetStream()
							if e, a := stream != nil, c.expectPayload != nil; e != a {
								t.Fatalf("expect nil payload %t, got %t", e, a)
							}
							if stream == nil {
								return
							}

							actualPayload, err := ioutil.ReadAll(stream)
							if err == nil && c.expectReadErr {
								t.Fatalf("expected read error, got none")
							}

							if diff := cmp.Diff(string(c.expectPayload), string(actualPayload)); diff != "" {
								t.Errorf("expect payload match:\n%s", diff)
							}

							payloadHash := v4.GetPayloadHash(ctx)
							if e, a := c.expectPayloadHash, payloadHash; e != a {
								t.Errorf("expect %v payload hash, got %v", e, a)
							}

							return &smithyhttp.Response{}, metadata, nil
						},
					)

					if c.initContext != nil {
						ctx = c.initContext(ctx)
					}
					_, metadata, err := stack.HandleMiddleware(ctx, struct{}{}, validateRequestHandler)
					if err == nil && len(c.expectErr) != 0 {
						t.Fatalf("expected error: %v, got none", c.expectErr)
					}
					if err != nil && len(c.expectErr) == 0 {
						t.Fatalf("expect no error, got %v", err)
					}
					if err != nil && !strings.Contains(err.Error(), c.expectErr) {
						t.Fatalf("expected error %v to contain %v", err, c.expectErr)
					}
					if c.expectErr != "" {
						return
					}

					if c.expectLogged != "" {
						if e, a := c.expectLogged, logged.String(); !strings.Contains(a, e) {
							t.Errorf("expected %q logged in:\n%s", e, a)
						}
					}

					// assert computed input checksums metadata
					computedMetadata, ok := GetComputedInputChecksums(metadata)
					if e, a := ok, (c.expectChecksumMetadata != nil); e != a {
						t.Fatalf("expect checksum metadata %t, got %t, %v", e, a, computedMetadata)
					}
					if c.expectChecksumMetadata != nil {
						if diff := cmp.Diff(c.expectChecksumMetadata, computedMetadata); diff != "" {
							t.Errorf("expect checksum metadata match\n%s", diff)
						}
					}
				})
			}
		})
	}
}

type mockReadSeeker struct {
	io.Reader
}

func (r *mockReadSeeker) Seek(int64, int) (int64, error) {
	return 0, nil
}

type errSeekReader struct {
	io.Reader
}

func (r *errSeekReader) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekCurrent {
		return 0, nil
	}

	return 0, fmt.Errorf("seek failed")
}

func requestMust(r *smithyhttp.Request, err error) *smithyhttp.Request {
	if err != nil {
		panic(err.Error())
	}

	return r
}
