package customizations_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aws/smithy-go"
)

func Test_EmptyResponse(t *testing.T) {
	cases := map[string]struct {
		response    *http.Response
		expectError bool
	}{
		"success case with no response body": {
			response: &http.Response{
				StatusCode: 200,
				Body: asReadCloser(
					``,
				),
			},
		},
		"error case with no response body": {
			response: &http.Response{
				StatusCode: 400,
				Body: asReadCloser(
					``,
				),
			},
			expectError: true,
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {

			ctx, cancelFn := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelFn()

			cfg := aws.Config{
				Region: "mock-region",
				Retryer: func() aws.Retryer {
					return aws.NopRetryer{}
				},
			}

			client := s3.NewFromConfig(cfg,
				func(options *s3.Options) {
					options.UsePathStyle = true
					options.HTTPClient = &mockHTTPClient{c.response}
				},
			)

			params := &s3.HeadBucketInput{Bucket: aws.String("aws-sdk-go-data")}
			_, err := client.HeadBucket(ctx, params)
			if c.expectError {
				var apiErr smithy.APIError
				if !errors.As(err, &apiErr) {
					t.Fatalf("expect error to be API error, was not, %v", err)
				}
				if len(apiErr.ErrorCode()) == 0 {
					t.Errorf("expect non-empty error code")
				}
				if len(apiErr.ErrorMessage()) == 0 {
					t.Errorf("expect non-empty error message")
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got %v", err.Error())
				}
			}
		})
	}
}

func TestBucketLocationPopulation(t *testing.T) {
	cases := map[string]struct {
		response       *http.Response
		expectLocation string
		expectError    string
	}{
		"empty location": {
			response: &http.Response{
				StatusCode: 200,
				Body: asReadCloser(
					`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`,
				),
			},
			expectLocation: "",
		},
		"EU location": {
			response: &http.Response{
				StatusCode: 200,
				Body: asReadCloser(
					`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">EU</LocationConstraint>`,
				),
			},
			expectLocation: "EU",
		},
		"AfSouth1 location": {
			response: &http.Response{
				StatusCode: 200,
				Body: asReadCloser(
					`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">af-south-1</LocationConstraint>`,
				),
			},
			expectLocation: "af-south-1",
		},
		"IncompleteResponse": {
			response: &http.Response{
				Body: asReadCloser(
					`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`,
				),
			},
			expectError: "unexpected EOF",
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {

			ctx, cancelFn := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelFn()

			cfg := aws.Config{
				Region:  "us-east-1",
				Retryer: func() aws.Retryer { return aws.NopRetryer{} },
			}

			client := s3.NewFromConfig(cfg, func(options *s3.Options) {
				options.UsePathStyle = true
				options.HTTPClient = &mockHTTPClient{c.response}
			})

			params := &s3.GetBucketLocationInput{
				Bucket: aws.String("aws-sdk-go-data"),
			}
			resp, err := client.GetBucketLocation(ctx, params)
			if len(c.expectError) != 0 && err == nil {
				t.Fatal("expect error, got none")
			}

			if err != nil && len(c.expectError) == 0 {
				t.Fatalf("expect no error, got %v", err)
			} else {
				if err != nil {
					if !strings.Contains(err.Error(), c.expectError) {
						t.Fatalf("expect error to be %v, got %v", err.Error(), c.expectError)
					}
					return
				}
			}

			if e, a := c.expectLocation, resp.LocationConstraint; !strings.EqualFold(e, string(a)) {
				t.Fatalf("expected location constraint to be deserialized as %v, got %v", e, a)
			}
		})
	}

}
