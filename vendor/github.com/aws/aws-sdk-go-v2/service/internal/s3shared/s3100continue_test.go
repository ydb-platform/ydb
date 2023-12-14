package s3shared

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/internal/awstesting"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"testing"
)

// unit test for service/internal/s3shared/s3100continue.go
func TestAdd100ContinueHttpHeader(t *testing.T) {
	const HeaderKey = "Expect"
	HeaderValue := "100-continue"

	cases := map[string]struct {
		ContentLength                int64
		Body                         *awstesting.ReadCloser
		ExpectValueFound             string
		ContinueHeaderThresholdBytes int64
	}{
		"http request smaller than default 2MB": {
			ContentLength:    1,
			Body:             &awstesting.ReadCloser{Size: 1},
			ExpectValueFound: "",
		},
		"http request smaller than configured threshold": {
			ContentLength:                1024 * 1024 * 2,
			Body:                         &awstesting.ReadCloser{Size: 1024 * 1024 * 2},
			ExpectValueFound:             "",
			ContinueHeaderThresholdBytes: 1024 * 1024 * 3,
		},
		"http request larger than default 2MB": {
			ContentLength:    1024 * 1024 * 3,
			Body:             &awstesting.ReadCloser{Size: 1024 * 1024 * 3},
			ExpectValueFound: HeaderValue,
		},
		"http request larger than configured threshold": {
			ContentLength:                1024 * 1024 * 4,
			Body:                         &awstesting.ReadCloser{Size: 1024 * 1024 * 4},
			ExpectValueFound:             HeaderValue,
			ContinueHeaderThresholdBytes: 1024 * 1024 * 3,
		},
		"http put request with unknown -1 ContentLength": {
			ContentLength:    -1,
			Body:             &awstesting.ReadCloser{Size: 1024 * 1024 * 10},
			ExpectValueFound: HeaderValue,
		},
		"http put request with 0 ContentLength but unknown non-nil body": {
			ContentLength:    0,
			Body:             &awstesting.ReadCloser{Size: 1024 * 1024 * 3},
			ExpectValueFound: HeaderValue,
		},
		"http put request with unknown -1 ContentLength and configured threshold": {
			ContentLength:                -1,
			Body:                         &awstesting.ReadCloser{Size: 1024 * 1024 * 3},
			ExpectValueFound:             HeaderValue,
			ContinueHeaderThresholdBytes: 1024 * 1024 * 10,
		},
		"http put request with continue header disabled": {
			ContentLength:                1024 * 1024 * 3,
			Body:                         &awstesting.ReadCloser{Size: 1024 * 1024 * 3},
			ExpectValueFound:             "",
			ContinueHeaderThresholdBytes: -1,
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			var err error
			req := smithyhttp.NewStackRequest().(*smithyhttp.Request)

			req.ContentLength = c.ContentLength
			req.Body = c.Body
			var updatedRequest *smithyhttp.Request
			m := s3100Continue{
				continueHeaderThresholdBytes: c.ContinueHeaderThresholdBytes,
			}
			_, _, err = m.HandleBuild(context.Background(),
				middleware.BuildInput{Request: req},
				middleware.BuildHandlerFunc(func(ctx context.Context, input middleware.BuildInput) (
					out middleware.BuildOutput, metadata middleware.Metadata, err error) {
					updatedRequest = input.Request.(*smithyhttp.Request)
					return out, metadata, nil
				}),
			)
			if err != nil {
				t.Fatalf("expect no error, got %v", err)
			}

			if e, a := c.ExpectValueFound, updatedRequest.Header.Get(HeaderKey); e != a {
				t.Errorf("expect header value %v found, got %v", e, a)
			}
		})
	}
}
