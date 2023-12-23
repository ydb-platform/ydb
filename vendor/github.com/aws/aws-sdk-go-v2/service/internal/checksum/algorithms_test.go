//go:build go1.16
// +build go1.16

package checksum

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/google/go-cmp/cmp"
)

func TestComputeChecksumReader(t *testing.T) {
	cases := map[string]struct {
		Input             io.Reader
		Algorithm         Algorithm
		ExpectErr         string
		ExpectChecksumLen int

		ExpectRead       string
		ExpectReadErr    string
		ExpectComputeErr string
		ExpectChecksum   string
	}{
		"unknown algorithm": {
			Input:     bytes.NewBuffer(nil),
			Algorithm: Algorithm("something"),
			ExpectErr: "unknown checksum algorithm",
		},
		"read error": {
			Input:             iotest.ErrReader(fmt.Errorf("some error")),
			Algorithm:         AlgorithmSHA256,
			ExpectChecksumLen: base64.StdEncoding.EncodedLen(sha256.Size),
			ExpectReadErr:     "some error",
			ExpectComputeErr:  "some error",
		},
		"crc32c": {
			Input:             strings.NewReader("hello world"),
			Algorithm:         AlgorithmCRC32C,
			ExpectChecksumLen: base64.StdEncoding.EncodedLen(crc32.Size),
			ExpectRead:        "hello world",
			ExpectChecksum:    "yZRlqg==",
		},
		"crc32": {
			Input:             strings.NewReader("hello world"),
			Algorithm:         AlgorithmCRC32,
			ExpectChecksumLen: base64.StdEncoding.EncodedLen(crc32.Size),
			ExpectRead:        "hello world",
			ExpectChecksum:    "DUoRhQ==",
		},
		"sha1": {
			Input:             strings.NewReader("hello world"),
			Algorithm:         AlgorithmSHA1,
			ExpectChecksumLen: base64.StdEncoding.EncodedLen(sha1.Size),
			ExpectRead:        "hello world",
			ExpectChecksum:    "Kq5sNclPz7QV2+lfQIuc6R7oRu0=",
		},
		"sha256": {
			Input:             strings.NewReader("hello world"),
			Algorithm:         AlgorithmSHA256,
			ExpectChecksumLen: base64.StdEncoding.EncodedLen(sha256.Size),
			ExpectRead:        "hello world",
			ExpectChecksum:    "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=",
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			// Validate reader can be created as expected.
			r, err := newComputeChecksumReader(c.Input, c.Algorithm)
			if err == nil && len(c.ExpectErr) != 0 {
				t.Fatalf("expect error %v, got none", c.ExpectErr)
			}
			if err != nil && len(c.ExpectErr) == 0 {
				t.Fatalf("expect no error, got %v", err)
			}
			if err != nil && !strings.Contains(err.Error(), c.ExpectErr) {
				t.Fatalf("expect error to contain %v, got %v", c.ExpectErr, err)
			}
			if c.ExpectErr != "" {
				return
			}

			if e, a := c.Algorithm, r.Algorithm(); e != a {
				t.Errorf("expect %v algorithm, got %v", e, a)
			}

			// Validate expected checksum length.
			if e, a := c.ExpectChecksumLen, r.Base64ChecksumLength(); e != a {
				t.Errorf("expect %v checksum length, got %v", e, a)
			}

			// Validate read reads underlying stream's bytes as expected.
			b, err := ioutil.ReadAll(r)
			if err == nil && len(c.ExpectReadErr) != 0 {
				t.Fatalf("expect error %v, got none", c.ExpectReadErr)
			}
			if err != nil && len(c.ExpectReadErr) == 0 {
				t.Fatalf("expect no error, got %v", err)
			}
			if err != nil && !strings.Contains(err.Error(), c.ExpectReadErr) {
				t.Fatalf("expect error to contain %v, got %v", c.ExpectReadErr, err)
			}
			if len(c.ExpectReadErr) != 0 {
				return
			}

			if diff := cmp.Diff(string(c.ExpectRead), string(b)); diff != "" {
				t.Errorf("expect read match, got\n%v", diff)
			}

			// validate computed base64
			v, err := r.Base64Checksum()
			if err == nil && len(c.ExpectComputeErr) != 0 {
				t.Fatalf("expect error %v, got none", c.ExpectComputeErr)
			}
			if err != nil && len(c.ExpectComputeErr) == 0 {
				t.Fatalf("expect no error, got %v", err)
			}
			if err != nil && !strings.Contains(err.Error(), c.ExpectComputeErr) {
				t.Fatalf("expect error to contain %v, got %v", c.ExpectComputeErr, err)
			}
			if diff := cmp.Diff(c.ExpectChecksum, v); diff != "" {
				t.Errorf("expect checksum match, got\n%v", diff)
			}
			if c.ExpectComputeErr != "" {
				return
			}

			if e, a := c.ExpectChecksumLen, len(v); e != a {
				t.Errorf("expect computed checksum length %v, got %v", e, a)
			}
		})
	}
}

func TestComputeChecksumReader_earlyGetChecksum(t *testing.T) {
	r, err := newComputeChecksumReader(strings.NewReader("hello world"), AlgorithmCRC32C)
	if err != nil {
		t.Fatalf("expect no error, got %v", err)
	}

	v, err := r.Base64Checksum()
	if err == nil {
		t.Fatalf("expect error, got none")
	}
	if err != nil && !strings.Contains(err.Error(), "not available") {
		t.Fatalf("expect error to match, got %v", err)
	}
	if v != "" {
		t.Errorf("expect no checksum, got %v", err)
	}
}

// TODO race condition case with many reads, and get checksum

func TestValidateChecksumReader(t *testing.T) {
	cases := map[string]struct {
		payload           io.ReadCloser
		algorithm         Algorithm
		checksum          string
		expectErr         string
		expectChecksumErr string
		expectedBody      []byte
	}{
		"unknown algorithm": {
			payload:   ioutil.NopCloser(bytes.NewBuffer(nil)),
			algorithm: Algorithm("something"),
			expectErr: "unknown checksum algorithm",
		},
		"empty body": {
			payload:      ioutil.NopCloser(bytes.NewReader([]byte(""))),
			algorithm:    AlgorithmCRC32,
			checksum:     "AAAAAA==",
			expectedBody: []byte(""),
		},
		"standard body": {
			payload:      ioutil.NopCloser(bytes.NewReader([]byte("Hello world"))),
			algorithm:    AlgorithmCRC32,
			checksum:     "i9aeUg==",
			expectedBody: []byte("Hello world"),
		},
		"checksum mismatch": {
			payload:           ioutil.NopCloser(bytes.NewReader([]byte("Hello world"))),
			algorithm:         AlgorithmCRC32,
			checksum:          "AAAAAA==",
			expectedBody:      []byte("Hello world"),
			expectChecksumErr: "checksum did not match",
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			response, err := newValidateChecksumReader(c.payload, c.algorithm, c.checksum)
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

			if response == nil {
				if c.expectedBody == nil {
					return
				}
				t.Fatalf("expected non nil response, got nil")
			}

			actualResponse, err := ioutil.ReadAll(response)
			if err == nil && len(c.expectChecksumErr) != 0 {
				t.Fatalf("expected error %v, got none", c.expectChecksumErr)
			}
			if err != nil && !strings.Contains(err.Error(), c.expectChecksumErr) {
				t.Fatalf("expected error %v to contain %v", err.Error(), c.expectChecksumErr)
			}

			if diff := cmp.Diff(c.expectedBody, actualResponse); len(diff) != 0 {
				t.Fatalf("found diff comparing response body  %v", diff)
			}

			err = response.Close()
			if err != nil {
				t.Fatalf("expect no error, got %v", err)
			}
		})
	}
}

func TestComputeMD5Checksum(t *testing.T) {
	cases := map[string]struct {
		payload        io.Reader
		expectErr      string
		expectChecksum string
	}{
		"empty payload": {
			payload:        strings.NewReader(""),
			expectChecksum: "1B2M2Y8AsgTpgAmY7PhCfg==",
		},
		"payload": {
			payload:        strings.NewReader("hello world"),
			expectChecksum: "XrY7u+Ae7tCTyyK7j1rNww==",
		},
		"error payload": {
			payload:   iotest.ErrReader(fmt.Errorf("some error")),
			expectErr: "some error",
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			actualChecksum, err := computeMD5Checksum(c.payload)
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

			if e, a := c.expectChecksum, string(actualChecksum); !strings.EqualFold(e, a) {
				t.Errorf("expect %v checksum, got %v", e, a)
			}
		})
	}
}

func TestParseAlgorithm(t *testing.T) {
	cases := map[string]struct {
		Value           string
		expectAlgorithm Algorithm
		expectErr       string
	}{
		"crc32c": {
			Value:           "crc32c",
			expectAlgorithm: AlgorithmCRC32C,
		},
		"CRC32C": {
			Value:           "CRC32C",
			expectAlgorithm: AlgorithmCRC32C,
		},
		"crc32": {
			Value:           "crc32",
			expectAlgorithm: AlgorithmCRC32,
		},
		"CRC32": {
			Value:           "CRC32",
			expectAlgorithm: AlgorithmCRC32,
		},
		"sha1": {
			Value:           "sha1",
			expectAlgorithm: AlgorithmSHA1,
		},
		"SHA1": {
			Value:           "SHA1",
			expectAlgorithm: AlgorithmSHA1,
		},
		"sha256": {
			Value:           "sha256",
			expectAlgorithm: AlgorithmSHA256,
		},
		"SHA256": {
			Value:           "SHA256",
			expectAlgorithm: AlgorithmSHA256,
		},
		"empty": {
			Value:     "",
			expectErr: "unknown checksum algorithm",
		},
		"unknown": {
			Value:     "unknown",
			expectErr: "unknown checksum algorithm",
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			// Asserts
			algorithm, err := ParseAlgorithm(c.Value)
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

			if e, a := c.expectAlgorithm, algorithm; e != a {
				t.Errorf("expect %v algorithm, got %v", e, a)
			}
		})
	}
}

func TestFilterSupportedAlgorithms(t *testing.T) {
	cases := map[string]struct {
		values           []string
		expectAlgorithms []Algorithm
	}{
		"no algorithms": {
			expectAlgorithms: []Algorithm{},
		},
		"no supported algorithms": {
			values:           []string{"abc", "123"},
			expectAlgorithms: []Algorithm{},
		},
		"duplicate algorithms": {
			values: []string{"crc32", "crc32c", "crc32c"},
			expectAlgorithms: []Algorithm{
				AlgorithmCRC32,
				AlgorithmCRC32C,
			},
		},
		"preserve order": {
			values: []string{"crc32", "crc32c", "sha1", "sha256"},
			expectAlgorithms: []Algorithm{
				AlgorithmCRC32,
				AlgorithmCRC32C,
				AlgorithmSHA1,
				AlgorithmSHA256,
			},
		},
		"preserve order 2": {
			values: []string{"sha256", "crc32", "sha1", "crc32c"},
			expectAlgorithms: []Algorithm{
				AlgorithmSHA256,
				AlgorithmCRC32,
				AlgorithmSHA1,
				AlgorithmCRC32C,
			},
		},
		"mixed case": {
			values: []string{"Crc32", "cRc32c", "shA1", "sHA256"},
			expectAlgorithms: []Algorithm{
				AlgorithmCRC32,
				AlgorithmCRC32C,
				AlgorithmSHA1,
				AlgorithmSHA256,
			},
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			algorithms := FilterSupportedAlgorithms(c.values)
			if diff := cmp.Diff(c.expectAlgorithms, algorithms); diff != "" {
				t.Errorf("expect algorithms match\n%s", diff)
			}
		})
	}
}

func TestAlgorithmChecksumLength(t *testing.T) {
	cases := map[string]struct {
		algorithm    Algorithm
		expectErr    string
		expectLength int
	}{
		"empty": {
			algorithm: "",
			expectErr: "unknown checksum algorithm",
		},
		"unknown": {
			algorithm: "",
			expectErr: "unknown checksum algorithm",
		},
		"crc32": {
			algorithm:    AlgorithmCRC32,
			expectLength: crc32.Size,
		},
		"crc32c": {
			algorithm:    AlgorithmCRC32C,
			expectLength: crc32.Size,
		},
		"sha1": {
			algorithm:    AlgorithmSHA1,
			expectLength: sha1.Size,
		},
		"sha256": {
			algorithm:    AlgorithmSHA256,
			expectLength: sha256.Size,
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			l, err := AlgorithmChecksumLength(c.algorithm)
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

			if e, a := c.expectLength, l; e != a {
				t.Errorf("expect %v checksum length, got %v", e, a)
			}
		})
	}
}
