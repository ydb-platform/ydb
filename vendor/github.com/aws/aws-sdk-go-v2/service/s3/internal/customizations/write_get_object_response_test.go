package customizations_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/go-cmp/cmp"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
)

type readSeeker struct {
	br *bytes.Reader
}

func (r *readSeeker) Read(p []byte) (int, error) {
	return r.br.Read(p)
}

func (r *readSeeker) Seek(offset int64, whence int) (int64, error) {
	return r.br.Seek(offset, whence)
}

type readOnlyReader struct {
	br *bytes.Reader
}

func (r *readOnlyReader) Read(p []byte) (int, error) {
	return r.br.Read(p)
}

type lenReader struct {
	br *bytes.Reader
}

func (r *lenReader) Read(p []byte) (int, error) {
	return r.br.Read(p)
}

func (r *lenReader) Len() int {
	return r.br.Len()
}

func TestWriteGetObjectResponse(t *testing.T) {
	const contentLength = "Content-Length"
	const contentSha256 = "X-Amz-Content-Sha256"
	const unsignedPayload = "UNSIGNED-PAYLOAD"

	cases := map[string]struct {
		Handler func(*testing.T) http.Handler
		Input   s3.WriteGetObjectResponseInput
	}{
		"Content-Length seekable": {
			Handler: func(t *testing.T) http.Handler {
				return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					expectedInput := []byte("test input")

					if len(request.TransferEncoding) != 0 {
						t.Errorf("expect no transfer-encoding")
					}

					if diff := cmp.Diff(request.Header.Get(contentLength), fmt.Sprintf("%d", len(expectedInput))); len(diff) > 0 {
						t.Error(diff)
					}

					if diff := cmp.Diff(request.Header.Get(contentSha256), unsignedPayload); len(diff) > 0 {
						t.Error(diff)
					}

					all, err := ioutil.ReadAll(request.Body)
					if err != nil {
						t.Errorf("expect no error, got %v", err)
					}
					if diff := cmp.Diff(all, expectedInput); len(diff) > 0 {
						t.Error(diff)
					}
					writer.WriteHeader(200)
				})
			},
			Input: s3.WriteGetObjectResponseInput{
				RequestRoute: aws.String("route"),
				RequestToken: aws.String("token"),
				Body:         &readSeeker{br: bytes.NewReader([]byte("test input"))},
			},
		},
		"Content-Length Len Interface": {
			Handler: func(t *testing.T) http.Handler {
				return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					expectedInput := []byte("test input")

					if len(request.TransferEncoding) != 0 {
						t.Errorf("expect no transfer-encoding")
					}

					if diff := cmp.Diff(request.Header.Get(contentLength), fmt.Sprintf("%d", len(expectedInput))); len(diff) > 0 {
						t.Error(diff)
					}

					if diff := cmp.Diff(request.Header.Get(contentSha256), unsignedPayload); len(diff) > 0 {
						t.Error(diff)
					}

					all, err := ioutil.ReadAll(request.Body)
					if err != nil {
						t.Errorf("expect no error, got %v", err)
					}
					if diff := cmp.Diff(all, expectedInput); len(diff) > 0 {
						t.Error(diff)
					}
					writer.WriteHeader(200)
				})
			},
			Input: s3.WriteGetObjectResponseInput{
				RequestRoute: aws.String("route"),
				RequestToken: aws.String("token"),
				Body:         &lenReader{bytes.NewReader([]byte("test input"))},
			},
		},
		"Content-Length Input Parameter": {
			Handler: func(t *testing.T) http.Handler {
				return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					expectedInput := []byte("test input")

					if len(request.TransferEncoding) != 0 {
						t.Errorf("expect no transfer-encoding")
					}

					if diff := cmp.Diff(request.Header.Get(contentLength), fmt.Sprintf("%d", len(expectedInput))); len(diff) > 0 {
						t.Error(diff)
					}

					if diff := cmp.Diff(request.Header.Get(contentSha256), unsignedPayload); len(diff) > 0 {
						t.Error(diff)
					}

					all, err := ioutil.ReadAll(request.Body)
					if err != nil {
						t.Errorf("expect no error, got %v", err)
					}
					if diff := cmp.Diff(all, expectedInput); len(diff) > 0 {
						t.Error(diff)
					}
					writer.WriteHeader(200)
				})
			},
			Input: s3.WriteGetObjectResponseInput{
				RequestRoute:  aws.String("route"),
				RequestToken:  aws.String("token"),
				Body:          &readOnlyReader{bytes.NewReader([]byte("test input"))},
				ContentLength: 10,
			},
		},
		"Content-Length Not Provided": {
			Handler: func(t *testing.T) http.Handler {
				return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					expectedInput := []byte("test input")

					if diff := cmp.Diff(request.TransferEncoding, []string{"chunked"}); len(diff) > 0 {
						t.Error(diff)
					}

					if diff := cmp.Diff(request.Header.Get(contentLength), ""); len(diff) > 0 {
						t.Error(diff)
					}

					if diff := cmp.Diff(request.Header.Get(contentSha256), unsignedPayload); len(diff) > 0 {
						t.Error(diff)
					}

					all, err := ioutil.ReadAll(request.Body)
					if err != nil {
						t.Errorf("expect no error, got %v", err)
					}
					if diff := cmp.Diff(all, expectedInput); len(diff) > 0 {
						t.Error(diff)
					}
					writer.WriteHeader(200)
				})
			},
			Input: s3.WriteGetObjectResponseInput{
				RequestRoute: aws.String("route"),
				RequestToken: aws.String("token"),
				Body:         &readOnlyReader{bytes.NewReader([]byte("test input"))},
			},
		},
	}

	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewTLSServer(tt.Handler(t))
			defer server.Close()
			client := s3.New(s3.Options{
				Region: "us-west-2",
				HTTPClient: &http.Client{
					Transport: &http.Transport{
						TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
					},
				},
				EndpointResolver: s3.EndpointResolverFunc(func(region string, options s3.EndpointResolverOptions) (aws.Endpoint, error) {
					return aws.Endpoint{
						URL:               server.URL,
						SigningName:       "s3-object-lambda",
						SigningRegion:     region,
						Source:            aws.EndpointSourceCustom,
						HostnameImmutable: true,
					}, nil
				}),
			})

			_, err := client.WriteGetObjectResponse(context.Background(), &tt.Input)
			if err != nil {
				t.Fatalf("expect no error, got %v", err)
			}
		})
	}
}
