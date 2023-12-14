package customizations_test

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/internal/awstesting/unit"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type EndpointResolverFunc func(region string, options s3.EndpointResolverOptions) (aws.Endpoint, error)

func (fn EndpointResolverFunc) ResolveEndpoint(region string, options s3.EndpointResolverOptions) (endpoint aws.Endpoint, err error) {
	return fn(region, options)
}

type mockHTTPClient struct {
	r *http.Response
}

func (m *mockHTTPClient) Do(*http.Request) (*http.Response, error) {
	return m.r, nil
}

var _ s3.HTTPClient = &mockHTTPClient{}

func asReadCloser(s string) io.ReadCloser {
	return ioutil.NopCloser(strings.NewReader(s))
}

func TestErrorResponseWith200StatusCode(t *testing.T) {
	cases := map[string]struct {
		response       *http.Response
		expectedError  string
		expectedBucket string
	}{
		"200ErrorBody": {
			response: &http.Response{
				StatusCode: 200,
				Body: asReadCloser(
					`<Error>
						<Type>Sender</Type>
						<Code>InvalidGreeting</Code>
						<Message>Hi</Message>
						<AnotherSetting>setting</AnotherSetting>
						<RequestId>foo-id</RequestId>
					</Error>`,
				),
			},
			expectedError: "InvalidGreeting",
		},
		"200NoResponse": {
			response: &http.Response{
				StatusCode: 200,
				Body:       asReadCloser(""),
			},
			expectedError: "received empty response payload",
		},
		"200InvalidResponse": {
			response: &http.Response{
				StatusCode: 200,
				Body: asReadCloser(
					`<Error>
						<Type>Sender</Type>
						<Code>InvalidGreeting</Code>
						<Message>Hi</Message>
						<AnotherSetting>setting</AnotherSetting>
						<RequestId>foo-id`,
				),
			},
			expectedError: "unexpected EOF",
		},
		"200SuccessResponse": {
			response: &http.Response{
				StatusCode: 200,
				Body: asReadCloser(
					`<CompleteMultipartUploadResult>
						<Bucket>bucket</Bucket>
					</CompleteMultipartUploadResult>`,
				),
			},
			expectedError:  "",
			expectedBucket: "bucket",
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			options := s3.Options{
				Credentials:  unit.StubCredentialsProvider{},
				Retryer:      aws.NopRetryer{},
				Region:       "mock-region",
				UsePathStyle: true,
				HTTPClient:   &mockHTTPClient{c.response},
			}

			svc := s3.New(options)
			resp, err := svc.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				UploadId:     aws.String("mockID"),
				RequestPayer: "requester",
				Bucket:       aws.String("bucket"),
				Key:          aws.String("mockKey"),
			})

			if len(c.expectedError) != 0 {
				if err == nil {
					t.Fatalf("expected error, got none")
				}

				if e, a := c.expectedError, err.Error(); !strings.Contains(a, e) {
					t.Fatalf("expected %v, got %v", e, a)
				}
			}

			if len(c.expectedBucket) != 0 {
				if e, a := c.expectedBucket, *resp.Bucket; !strings.EqualFold(e, a) {
					t.Fatalf("expected bucket name to be %v, got %v", e, a)
				}
			}
		})
	}
}
