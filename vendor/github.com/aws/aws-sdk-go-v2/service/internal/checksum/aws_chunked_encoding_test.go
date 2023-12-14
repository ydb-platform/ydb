//go:build go1.16
// +build go1.16

package checksum

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/google/go-cmp/cmp"
)

func TestAWSChunkedEncoding(t *testing.T) {
	cases := map[string]struct {
		reader              *awsChunkedEncoding
		expectErr           string
		expectEncodedLength int64
		expectHTTPHeaders   map[string][]string
		expectPayload       []byte
	}{
		"empty payload fixed stream length": {
			reader: newUnsignedAWSChunkedEncoding(strings.NewReader(""),
				func(o *awsChunkedEncodingOptions) {
					o.StreamLength = 0
				}),
			expectEncodedLength: 5,
			expectHTTPHeaders: map[string][]string{
				contentEncodingHeaderName: {
					awsChunkedContentEncodingHeaderValue,
				},
			},
			expectPayload: []byte("0\r\n\r\n"),
		},
		"empty payload unknown stream length": {
			reader:              newUnsignedAWSChunkedEncoding(strings.NewReader("")),
			expectEncodedLength: -1,
			expectHTTPHeaders: map[string][]string{
				contentEncodingHeaderName: {
					awsChunkedContentEncodingHeaderValue,
				},
			},
			expectPayload: []byte("0\r\n\r\n"),
		},
		"payload fixed stream length": {
			reader: newUnsignedAWSChunkedEncoding(strings.NewReader("hello world"),
				func(o *awsChunkedEncodingOptions) {
					o.StreamLength = 11
				}),
			expectEncodedLength: 21,
			expectHTTPHeaders: map[string][]string{
				contentEncodingHeaderName: {
					awsChunkedContentEncodingHeaderValue,
				},
			},
			expectPayload: []byte("b\r\nhello world\r\n0\r\n\r\n"),
		},
		"payload unknown stream length": {
			reader:              newUnsignedAWSChunkedEncoding(strings.NewReader("hello world")),
			expectEncodedLength: -1,
			expectHTTPHeaders: map[string][]string{
				contentEncodingHeaderName: {
					awsChunkedContentEncodingHeaderValue,
				},
			},
			expectPayload: []byte("b\r\nhello world\r\n0\r\n\r\n"),
		},
		"payload unknown stream length with chunk size": {
			reader: newUnsignedAWSChunkedEncoding(strings.NewReader("hello world"),
				func(o *awsChunkedEncodingOptions) {
					o.ChunkLength = 8
				}),
			expectEncodedLength: -1,
			expectHTTPHeaders: map[string][]string{
				contentEncodingHeaderName: {
					awsChunkedContentEncodingHeaderValue,
				},
			},
			expectPayload: []byte("8\r\nhello wo\r\n3\r\nrld\r\n0\r\n\r\n"),
		},
		"payload fixed stream length with chunk size": {
			reader: newUnsignedAWSChunkedEncoding(strings.NewReader("hello world"),
				func(o *awsChunkedEncodingOptions) {
					o.StreamLength = 11
					o.ChunkLength = 8
				}),
			expectEncodedLength: 26,
			expectHTTPHeaders: map[string][]string{
				contentEncodingHeaderName: {
					awsChunkedContentEncodingHeaderValue,
				},
			},
			expectPayload: []byte("8\r\nhello wo\r\n3\r\nrld\r\n0\r\n\r\n"),
		},
		"payload fixed stream length with fixed length trailer": {
			reader: newUnsignedAWSChunkedEncoding(strings.NewReader("hello world"),
				func(o *awsChunkedEncodingOptions) {
					o.StreamLength = 11
					o.Trailers = map[string]awsChunkedTrailerValue{
						"foo": {
							Get: func() (string, error) {
								return "abc123", nil
							},
							Length: 6,
						},
					}
				}),
			expectEncodedLength: 33,
			expectHTTPHeaders: map[string][]string{
				contentEncodingHeaderName: {
					awsChunkedContentEncodingHeaderValue,
				},
				awsTrailerHeaderName: {"foo"},
			},
			expectPayload: []byte("b\r\nhello world\r\n0\r\nfoo:abc123\r\n\r\n"),
		},
		"payload fixed stream length with unknown length trailer": {
			reader: newUnsignedAWSChunkedEncoding(strings.NewReader("hello world"),
				func(o *awsChunkedEncodingOptions) {
					o.StreamLength = 11
					o.Trailers = map[string]awsChunkedTrailerValue{
						"foo": {
							Get: func() (string, error) {
								return "abc123", nil
							},
							Length: -1,
						},
					}
				}),
			expectEncodedLength: -1,
			expectHTTPHeaders: map[string][]string{
				contentEncodingHeaderName: {
					awsChunkedContentEncodingHeaderValue,
				},
				awsTrailerHeaderName: {"foo"},
			},
			expectPayload: []byte("b\r\nhello world\r\n0\r\nfoo:abc123\r\n\r\n"),
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if e, a := c.expectEncodedLength, c.reader.EncodedLength(); e != a {
				t.Errorf("expect %v encoded length, got %v", e, a)
			}
			if diff := cmp.Diff(c.expectHTTPHeaders, c.reader.HTTPHeaders()); diff != "" {
				t.Errorf("expect HTTP headers match\n%v", diff)
			}

			actualPayload, err := ioutil.ReadAll(c.reader)
			if err == nil && len(c.expectErr) != 0 {
				t.Fatalf("expect error %v, got none", c.expectErr)
			}
			if err != nil && len(c.expectErr) == 0 {
				t.Fatalf("expect no error, got %v", err)
			}
			if err != nil && !strings.Contains(err.Error(), c.expectErr) {
				t.Fatalf("expect error to contain %v, got %v", c.expectErr, err)
			}
			if c.expectErr != "" {
				return
			}

			if diff := cmp.Diff(string(c.expectPayload), string(actualPayload)); diff != "" {
				t.Errorf("expect payload match\n%v", diff)
			}
		})
	}
}

func TestUnsignedAWSChunkReader(t *testing.T) {
	cases := map[string]struct {
		payload interface {
			io.Reader
			Len() int
		}

		expectPayload []byte
		expectErr     string
	}{
		"empty body": {
			payload:       bytes.NewReader([]byte{}),
			expectPayload: []byte("0\r\n"),
		},
		"with body": {
			payload:       strings.NewReader("Hello world"),
			expectPayload: []byte("b\r\nHello world\r\n0\r\n"),
		},
		"large body": {
			payload: bytes.NewBufferString("Hello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello world"),
			expectPayload: []byte("205\r\nHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello world\r\n0\r\n"),
		},
		"reader error": {
			payload:   newLimitReadLener(iotest.ErrReader(fmt.Errorf("some read error")), 128),
			expectErr: "some read error",
		},
		"unknown length reader": {
			payload: newUnknownLenReader(io.LimitReader(byteReader('a'), defaultChunkLength*2)),
			expectPayload: func() []byte {
				reader := newBufferedAWSChunkReader(
					io.LimitReader(byteReader('a'), defaultChunkLength*2),
					defaultChunkLength,
				)
				actualPayload, err := ioutil.ReadAll(reader)
				if err != nil {
					t.Fatalf("failed to create unknown length reader test data, %v", err)
				}
				return actualPayload
			}(),
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			reader := newUnsignedChunkReader(c.payload, int64(c.payload.Len()))

			actualPayload, err := ioutil.ReadAll(reader)
			if err == nil && len(c.expectErr) != 0 {
				t.Fatalf("expect error %v, got none", c.expectErr)
			}
			if err != nil && len(c.expectErr) == 0 {
				t.Fatalf("expect no error, got %v", err)
			}
			if err != nil && !strings.Contains(err.Error(), c.expectErr) {
				t.Fatalf("expect error to contain %v, got %v", c.expectErr, err)
			}
			if c.expectErr != "" {
				return
			}

			if diff := cmp.Diff(string(c.expectPayload), string(actualPayload)); diff != "" {
				t.Errorf("expect payload match\n%v", diff)
			}
		})
	}
}

func TestBufferedAWSChunkReader(t *testing.T) {
	cases := map[string]struct {
		payload   io.Reader
		readSize  int
		chunkSize int

		expectPayload []byte
		expectErr     string
	}{
		"empty body": {
			payload:       bytes.NewReader([]byte{}),
			chunkSize:     4,
			expectPayload: []byte("0\r\n"),
		},
		"with one chunk body": {
			payload:       strings.NewReader("Hello world"),
			chunkSize:     20,
			expectPayload: []byte("b\r\nHello world\r\n0\r\n"),
		},
		"single byte read": {
			payload:       strings.NewReader("Hello world"),
			chunkSize:     8,
			readSize:      1,
			expectPayload: []byte("8\r\nHello wo\r\n3\r\nrld\r\n0\r\n"),
		},
		"single chunk and byte read": {
			payload:       strings.NewReader("Hello world"),
			chunkSize:     1,
			readSize:      1,
			expectPayload: []byte("1\r\nH\r\n1\r\ne\r\n1\r\nl\r\n1\r\nl\r\n1\r\no\r\n1\r\n \r\n1\r\nw\r\n1\r\no\r\n1\r\nr\r\n1\r\nl\r\n1\r\nd\r\n0\r\n"),
		},
		"with two chunk body": {
			payload:       strings.NewReader("Hello world"),
			chunkSize:     8,
			expectPayload: []byte("8\r\nHello wo\r\n3\r\nrld\r\n0\r\n"),
		},
		"chunk size equal to read size": {
			payload:       strings.NewReader("Hello world"),
			chunkSize:     512,
			expectPayload: []byte("b\r\nHello world\r\n0\r\n"),
		},
		"chunk size greater than read size": {
			payload:       strings.NewReader("Hello world"),
			chunkSize:     1024,
			expectPayload: []byte("b\r\nHello world\r\n0\r\n"),
		},
		"payload size more than default read size, chunk size less than read size": {
			payload: bytes.NewBufferString("Hello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello world"),
			chunkSize: 500,
			expectPayload: []byte("1f4\r\nHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello\r\n11\r\n worldHello world\r\n0\r\n"),
		},
		"payload size more than default read size, chunk size equal to read size": {
			payload: bytes.NewBufferString("Hello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello world"),
			chunkSize: 512,
			expectPayload: []byte("200\r\nHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello \r\n5\r\nworld\r\n0\r\n"),
		},
		"payload size more than default read size, chunk size more than read size": {
			payload: bytes.NewBufferString("Hello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello world"),
			chunkSize: 1024,
			expectPayload: []byte("205\r\nHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello worldHello " +
				"worldHello worldHello worldHello worldHello worldHello world\r\n0\r\n"),
		},
		"reader error": {
			payload:   iotest.ErrReader(fmt.Errorf("some read error")),
			chunkSize: 128,
			expectErr: "some read error",
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			reader := newBufferedAWSChunkReader(c.payload, c.chunkSize)

			var actualPayload []byte
			var err error

			if c.readSize != 0 {
				for err == nil {
					var n int
					p := make([]byte, c.readSize)
					n, err = reader.Read(p)
					if n != 0 {
						actualPayload = append(actualPayload, p[:n]...)
					}
				}
				if err == io.EOF {
					err = nil
				}
			} else {
				actualPayload, err = ioutil.ReadAll(reader)
			}

			if err == nil && len(c.expectErr) != 0 {
				t.Fatalf("expect error %v, got none", c.expectErr)
			}
			if err != nil && len(c.expectErr) == 0 {
				t.Fatalf("expect no error, got %v", err)
			}
			if err != nil && !strings.Contains(err.Error(), c.expectErr) {
				t.Fatalf("expect error to contain %v, got %v", c.expectErr, err)
			}
			if c.expectErr != "" {
				return
			}

			if diff := cmp.Diff(string(c.expectPayload), string(actualPayload)); diff != "" {
				t.Errorf("expect payload match\n%v", diff)
			}
		})
	}
}

func TestAwsChunkedTrailerReader(t *testing.T) {
	cases := map[string]struct {
		reader *awsChunkedTrailerReader

		expectErr           string
		expectEncodedLength int
		expectPayload       []byte
	}{
		"no trailers": {
			reader:        newAWSChunkedTrailerReader(nil),
			expectPayload: []byte{},
		},
		"unknown length trailers": {
			reader: newAWSChunkedTrailerReader(map[string]awsChunkedTrailerValue{
				"foo": {
					Get: func() (string, error) {
						return "abc123", nil
					},
					Length: -1,
				},
			}),
			expectEncodedLength: -1,
			expectPayload:       []byte("foo:abc123\r\n"),
		},
		"known length trailers": {
			reader: newAWSChunkedTrailerReader(map[string]awsChunkedTrailerValue{
				"foo": {
					Get: func() (string, error) {
						return "abc123", nil
					},
					Length: 6,
				},
			}),
			expectEncodedLength: 12,
			expectPayload:       []byte("foo:abc123\r\n"),
		},
		"trailer error": {
			reader: newAWSChunkedTrailerReader(map[string]awsChunkedTrailerValue{
				"foo": {
					Get: func() (string, error) {
						return "", fmt.Errorf("some error")
					},
					Length: 6,
				},
			}),
			expectEncodedLength: 12,
			expectErr:           "failed to get trailer",
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if e, a := c.expectEncodedLength, c.reader.EncodedLength(); e != a {
				t.Errorf("expect %v encoded length, got %v", e, a)
			}

			actualPayload, err := ioutil.ReadAll(c.reader)

			// Asserts
			if err == nil && len(c.expectErr) != 0 {
				t.Fatalf("expect error %v, got none", c.expectErr)
			}
			if err != nil && len(c.expectErr) == 0 {
				t.Fatalf("expect no error, got %v", err)
			}
			if err != nil && !strings.Contains(err.Error(), c.expectErr) {
				t.Fatalf("expect error to contain %v, got %v", c.expectErr, err)
			}
			if c.expectErr != "" {
				return
			}

			if diff := cmp.Diff(string(c.expectPayload), string(actualPayload)); diff != "" {
				t.Errorf("expect payload match\n%v", diff)
			}
		})
	}
}

type limitReadLener struct {
	io.Reader
	length int
}

func newLimitReadLener(r io.Reader, l int) *limitReadLener {
	return &limitReadLener{
		Reader: io.LimitReader(r, int64(l)),
		length: l,
	}
}
func (r *limitReadLener) Len() int {
	return r.length
}

type unknownLenReader struct {
	io.Reader
}

func newUnknownLenReader(r io.Reader) *unknownLenReader {
	return &unknownLenReader{
		Reader: r,
	}
}
func (r *unknownLenReader) Len() int {
	return -1
}

type byteReader byte

func (r byteReader) Read(p []byte) (int, error) {
	for i := 0; i < len(p); i++ {
		p[i] = byte(r)
	}
	return len(p), nil
}
