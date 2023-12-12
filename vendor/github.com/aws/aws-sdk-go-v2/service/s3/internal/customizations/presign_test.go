package customizations_test

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/internal/awstesting/unit"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestPutObject_PresignURL(t *testing.T) {
	cases := map[string]struct {
		input                  s3.PutObjectInput
		options                []func(*s3.PresignOptions)
		expectPresignedURLHost string
		expectRequestURIQuery  []string
		expectSignedHeader     http.Header
		expectMethod           string
		expectError            string
	}{
		"standard case": {
			input: s3.PutObjectInput{
				Bucket: aws.String("mock-bucket"),
				Key:    aws.String("mockkey"),
				Body:   strings.NewReader("hello-world"),
			},
			expectPresignedURLHost: "https://mock-bucket.s3.us-west-2.amazonaws.com/mockkey?",
			expectRequestURIQuery: []string{
				"X-Amz-Expires=900",
				"X-Amz-Credential",
				"X-Amz-Date",
				"x-id=PutObject",
				"X-Amz-Signature",
			},
			expectMethod: "PUT",
			expectSignedHeader: http.Header{
				"Content-Length": []string{"11"},
				"Content-Type":   []string{"application/octet-stream"},
				"Host":           []string{"mock-bucket.s3.us-west-2.amazonaws.com"},
			},
		},
		"seekable payload": {
			input: s3.PutObjectInput{
				Bucket: aws.String("mock-bucket"),
				Key:    aws.String("mockkey"),
				Body:   bytes.NewReader([]byte("hello-world")),
			},
			expectPresignedURLHost: "https://mock-bucket.s3.us-west-2.amazonaws.com/mockkey?",
			expectRequestURIQuery: []string{
				"X-Amz-Expires=900",
				"X-Amz-Credential",
				"X-Amz-Date",
				"x-id=PutObject",
				"X-Amz-Signature",
			},
			expectMethod: "PUT",
			expectSignedHeader: http.Header{
				"Content-Length": []string{"11"},
				"Content-Type":   []string{"application/octet-stream"},
				"Host":           []string{"mock-bucket.s3.us-west-2.amazonaws.com"},
			},
		},
		"unseekable payload": {
			// unseekable payload succeeds as we disable content sha256 computation for streaming input
			input: s3.PutObjectInput{
				Bucket: aws.String("mock-bucket"),
				Key:    aws.String("mockkey"),
				Body:   bytes.NewBuffer([]byte(`hello-world`)),
			},
			expectPresignedURLHost: "https://mock-bucket.s3.us-west-2.amazonaws.com/mockkey?",
			expectRequestURIQuery: []string{
				"X-Amz-Expires=900",
				"X-Amz-Credential",
				"X-Amz-Date",
				"x-id=PutObject",
				"X-Amz-Signature",
			},
			expectMethod: "PUT",
			expectSignedHeader: http.Header{
				"Content-Length": []string{"11"},
				"Content-Type":   []string{"application/octet-stream"},
				"Host":           []string{"mock-bucket.s3.us-west-2.amazonaws.com"},
			},
		},
		"empty body": {
			input: s3.PutObjectInput{
				Bucket: aws.String("mock-bucket"),
				Key:    aws.String("mockkey"),
				Body:   bytes.NewReader([]byte(``)),
			},
			expectPresignedURLHost: "https://mock-bucket.s3.us-west-2.amazonaws.com/mockkey?",
			expectRequestURIQuery: []string{
				"X-Amz-Expires=900",
				"X-Amz-Credential",
				"X-Amz-Date",
				"x-id=PutObject",
				"X-Amz-Signature",
			},
			expectMethod: "PUT",
			expectSignedHeader: http.Header{
				"Host": []string{"mock-bucket.s3.us-west-2.amazonaws.com"},
			},
		},
		"nil body": {
			input: s3.PutObjectInput{
				Bucket: aws.String("mock-bucket"),
				Key:    aws.String("mockkey"),
			},
			expectPresignedURLHost: "https://mock-bucket.s3.us-west-2.amazonaws.com/mockkey?",
			expectRequestURIQuery: []string{
				"X-Amz-Expires=900",
				"X-Amz-Credential",
				"X-Amz-Date",
				"x-id=PutObject",
				"X-Amz-Signature",
			},
			expectMethod: "PUT",
			expectSignedHeader: http.Header{
				"Host": []string{"mock-bucket.s3.us-west-2.amazonaws.com"},
			},
		},
		"nil body with content-length": {
			input: s3.PutObjectInput{
				Bucket:        aws.String("mock-bucket"),
				Key:           aws.String("mockkey"),
				ContentLength: 100,
			},
			expectPresignedURLHost: "https://mock-bucket.s3.us-west-2.amazonaws.com/mockkey?",
			expectRequestURIQuery: []string{
				"X-Amz-Expires=900",
				"X-Amz-Credential",
				"X-Amz-Date",
				"x-id=PutObject",
				"X-Amz-Signature",
			},
			expectMethod: "PUT",
			expectSignedHeader: http.Header{
				"Host":           []string{"mock-bucket.s3.us-west-2.amazonaws.com"},
				"Content-Length": []string{"100"},
			},
		},
		"mrap presigned": {
			input: s3.PutObjectInput{
				Bucket: aws.String("arn:aws:s3::123456789012:accesspoint:mfzwi23gnjvgw.mrap"),
				Key:    aws.String("mockkey"),
				Body:   strings.NewReader("hello-world"),
			},
			expectPresignedURLHost: "https://mfzwi23gnjvgw.mrap.accesspoint.s3-global.amazonaws.com/mockkey?",
			expectRequestURIQuery: []string{
				"X-Amz-Expires=900",
				"X-Amz-Credential",
				"X-Amz-Date",
				"x-id=PutObject",
				"X-Amz-Signature",
				"X-Amz-Region-Set",
			},
			expectMethod: "PUT",
			expectSignedHeader: http.Header{
				"Content-Length": []string{"11"},
				"Content-Type":   []string{"application/octet-stream"},
				"Host":           []string{"mfzwi23gnjvgw.mrap.accesspoint.s3-global.amazonaws.com"},
			},
		},
		"mrap presigned with mrap disabled": {
			input: s3.PutObjectInput{
				Bucket: aws.String("arn:aws:s3::123456789012:accesspoint:mfzwi23gnjvgw.mrap"),
				Key:    aws.String("mockkey"),
				Body:   strings.NewReader("hello-world"),
			},
			options: []func(option *s3.PresignOptions){
				func(option *s3.PresignOptions) {
					option.ClientOptions = []func(o *s3.Options){
						func(o *s3.Options) {
							o.DisableMultiRegionAccessPoints = true
						},
					}
				},
			},
			expectError: "Invalid configuration: Multi-Region Access Point ARNs are disabled.",
		},
		"standard case with checksum preset checksum": {
			input: s3.PutObjectInput{
				Bucket:            aws.String("mock-bucket"),
				Key:               aws.String("mockkey"),
				Body:              strings.NewReader("hello world"),
				ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32c,
				ChecksumCRC32:     aws.String("DUoRhQ=="),
			},
			expectPresignedURLHost: "https://mock-bucket.s3.us-west-2.amazonaws.com/mockkey?",
			expectRequestURIQuery: []string{
				"X-Amz-Expires=900",
				"X-Amz-Credential",
				"X-Amz-Date",
				"x-id=PutObject",
				"X-Amz-Signature",
				"X-Amz-Checksum-Crc32",
			},
			expectMethod: "PUT",
			expectSignedHeader: http.Header{
				"Content-Length": []string{"11"},
				"Content-Type":   []string{"application/octet-stream"},
				"Host":           []string{"mock-bucket.s3.us-west-2.amazonaws.com"},
			},
		},
		"standard case with checksum empty body": {
			input: s3.PutObjectInput{
				Bucket:            aws.String("mock-bucket"),
				Key:               aws.String("mockkey"),
				Body:              strings.NewReader(""),
				ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32c,
				ChecksumCRC32:     aws.String("AAAAAA=="),
			},
			expectPresignedURLHost: "https://mock-bucket.s3.us-west-2.amazonaws.com/mockkey?",
			expectRequestURIQuery: []string{
				"X-Amz-Expires=900",
				"X-Amz-Credential",
				"X-Amz-Date",
				"x-id=PutObject",
				"X-Amz-Signature",
				"X-Amz-Checksum-Crc32",
			},
			expectMethod: "PUT",
			expectSignedHeader: http.Header{
				"Host": []string{"mock-bucket.s3.us-west-2.amazonaws.com"},
			},
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			cfg := aws.Config{
				Region:      "us-west-2",
				Credentials: unit.StubCredentialsProvider{},
				Retryer: func() aws.Retryer {
					return aws.NopRetryer{}
				},
			}
			presignClient := s3.NewPresignClient(s3.NewFromConfig(cfg), c.options...)

			req, err := presignClient.PresignPutObject(ctx, &c.input)
			if err != nil {
				if len(c.expectError) == 0 {
					t.Fatalf("expected no error, got %v", err)
				}
				// if expect error, match error and skip rest
				if e, a := c.expectError, err.Error(); !strings.Contains(a, e) {
					t.Fatalf("expected error to be %s, got %s", e, a)
				}
				return
			} else {
				if len(c.expectError) != 0 {
					t.Fatalf("expected error to be %v, got none", c.expectError)
				}
			}

			if e, a := c.expectPresignedURLHost, req.URL; !strings.Contains(a, e) {
				t.Fatalf("expected presigned url to contain host %s, got %s", e, a)
			}

			if len(c.expectRequestURIQuery) != 0 {
				for _, label := range c.expectRequestURIQuery {
					if e, a := label, req.URL; !strings.Contains(a, e) {
						t.Fatalf("expected presigned url to contain %v label in url: %v", label, req.URL)
					}
				}
			}

			if e, a := c.expectSignedHeader, req.SignedHeader; len(cmp.Diff(e, a)) != 0 {
				t.Fatalf("expected signed header to be %s, got %s, \n diff : %s", e, a, cmp.Diff(e, a))
			}

			if e, a := c.expectMethod, req.Method; !strings.EqualFold(e, a) {
				t.Fatalf("expected presigning Method to be %s, got %s", e, a)
			}

		})
	}
}

func TestUploadPart_PresignURL(t *testing.T) {
	cases := map[string]struct {
		input                  s3.UploadPartInput
		options                s3.PresignOptions
		expectPresignedURLHost string
		expectRequestURIQuery  []string
		expectSignedHeader     http.Header
		expectMethod           string
		expectError            string
	}{
		"standard case": {
			input: s3.UploadPartInput{
				Bucket:     aws.String("mock-bucket"),
				Key:        aws.String("mockkey"),
				PartNumber: 1,
				UploadId:   aws.String("123456"),
				Body:       strings.NewReader("hello-world"),
			},
			expectPresignedURLHost: "https://mock-bucket.s3.us-west-2.amazonaws.com/mockkey?",
			expectRequestURIQuery: []string{
				"X-Amz-Expires=900",
				"X-Amz-Credential",
				"X-Amz-Date",
				"partNumber=1",
				"uploadId=123456",
				"x-id=UploadPart",
				"X-Amz-Signature",
			},
			expectMethod: "PUT",
			expectSignedHeader: http.Header{
				"Content-Length": []string{"11"},
				"Content-Type":   []string{"application/octet-stream"},
				"Host":           []string{"mock-bucket.s3.us-west-2.amazonaws.com"},
			},
		},
		"seekable payload": {
			input: s3.UploadPartInput{
				Bucket:     aws.String("mock-bucket"),
				Key:        aws.String("mockkey"),
				PartNumber: 1,
				UploadId:   aws.String("123456"),
				Body:       bytes.NewReader([]byte("hello-world")),
			},
			expectPresignedURLHost: "https://mock-bucket.s3.us-west-2.amazonaws.com/mockkey?",
			expectRequestURIQuery: []string{
				"X-Amz-Expires=900",
				"X-Amz-Credential",
				"X-Amz-Date",
				"partNumber=1",
				"uploadId=123456",
				"x-id=UploadPart",
				"X-Amz-Signature",
			},
			expectMethod: "PUT",
			expectSignedHeader: http.Header{
				"Content-Length": []string{"11"},
				"Content-Type":   []string{"application/octet-stream"},
				"Host":           []string{"mock-bucket.s3.us-west-2.amazonaws.com"},
			},
		},
		"unseekable payload": {
			// unseekable payload succeeds as we disable content sha256 computation for streaming input
			input: s3.UploadPartInput{
				Bucket:     aws.String("mock-bucket"),
				Key:        aws.String("mockkey"),
				PartNumber: 1,
				UploadId:   aws.String("123456"),
				Body:       bytes.NewBuffer([]byte(`hello-world`)),
			},
			expectPresignedURLHost: "https://mock-bucket.s3.us-west-2.amazonaws.com/mockkey?",
			expectRequestURIQuery: []string{
				"X-Amz-Expires=900",
				"X-Amz-Credential",
				"X-Amz-Date",
				"partNumber=1",
				"uploadId=123456",
				"x-id=UploadPart",
				"X-Amz-Signature",
			},
			expectMethod: "PUT",
			expectSignedHeader: http.Header{
				"Content-Length": []string{"11"},
				"Content-Type":   []string{"application/octet-stream"},
				"Host":           []string{"mock-bucket.s3.us-west-2.amazonaws.com"},
			},
		},
		"empty body": {
			input: s3.UploadPartInput{
				Bucket:     aws.String("mock-bucket"),
				Key:        aws.String("mockkey"),
				PartNumber: 1,
				UploadId:   aws.String("123456"),
				Body:       bytes.NewReader([]byte(``)),
			},
			expectPresignedURLHost: "https://mock-bucket.s3.us-west-2.amazonaws.com/mockkey?",
			expectRequestURIQuery: []string{
				"X-Amz-Expires=900",
				"X-Amz-Credential",
				"X-Amz-Date",
				"partNumber=1",
				"uploadId=123456",
				"x-id=UploadPart",
				"X-Amz-Signature",
			},
			expectMethod: "PUT",
			expectSignedHeader: http.Header{
				"Host": []string{"mock-bucket.s3.us-west-2.amazonaws.com"},
			},
		},
		"nil body": {
			input: s3.UploadPartInput{
				Bucket:     aws.String("mock-bucket"),
				Key:        aws.String("mockkey"),
				PartNumber: 1,
				UploadId:   aws.String("123456"),
			},
			expectPresignedURLHost: "https://mock-bucket.s3.us-west-2.amazonaws.com/mockkey?",
			expectRequestURIQuery: []string{
				"X-Amz-Expires=900",
				"X-Amz-Credential",
				"X-Amz-Date",
				"partNumber=1",
				"uploadId=123456",
				"x-id=UploadPart",
				"X-Amz-Signature",
			},
			expectMethod: "PUT",
			expectSignedHeader: http.Header{
				"Host": []string{"mock-bucket.s3.us-west-2.amazonaws.com"},
			},
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			cfg := aws.Config{
				Region:      "us-west-2",
				Credentials: unit.StubCredentialsProvider{},
				Retryer: func() aws.Retryer {
					return aws.NopRetryer{}
				},
			}
			presignClient := s3.NewPresignClient(s3.NewFromConfig(cfg), func(options *s3.PresignOptions) {
				options = &c.options
			})

			req, err := presignClient.PresignUploadPart(ctx, &c.input)
			if err != nil {
				if len(c.expectError) == 0 {
					t.Fatalf("expected no error, got %v", err)
				}
				// if expect error, match error and skip rest
				if e, a := c.expectError, err.Error(); !strings.Contains(a, e) {
					t.Fatalf("expected error to be %s, got %s", e, a)
				}
			} else {
				if len(c.expectError) != 0 {
					t.Fatalf("expected error to be %v, got none", c.expectError)
				}
			}

			if e, a := c.expectPresignedURLHost, req.URL; !strings.Contains(a, e) {
				t.Fatalf("expected presigned url to contain host %s, got %s", e, a)
			}

			if len(c.expectRequestURIQuery) != 0 {
				for _, label := range c.expectRequestURIQuery {
					if e, a := label, req.URL; !strings.Contains(a, e) {
						t.Fatalf("expected presigned url to contain %v label in url: %v", label, req.URL)
					}
				}
			}

			if e, a := c.expectSignedHeader, req.SignedHeader; len(cmp.Diff(e, a)) != 0 {
				t.Fatalf("expected signed header to be %s, got %s, \n diff : %s", e, a, cmp.Diff(e, a))
			}

			if e, a := c.expectMethod, req.Method; !strings.EqualFold(e, a) {
				t.Fatalf("expected presigning Method to be %s, got %s", e, a)
			}

		})
	}
}
