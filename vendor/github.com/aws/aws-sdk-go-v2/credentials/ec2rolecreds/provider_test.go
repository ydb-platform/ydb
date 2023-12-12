package ec2rolecreds

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	sdkrand "github.com/aws/aws-sdk-go-v2/internal/rand"
	"github.com/aws/aws-sdk-go-v2/internal/sdk"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
	"github.com/aws/smithy-go/middleware"
	"github.com/google/go-cmp/cmp"
)

const credsRespTmpl = `{
  "Code": "Success",
  "Type": "AWS-HMAC",
  "AccessKeyId" : "accessKey",
  "SecretAccessKey" : "secret",
  "Token" : "token",
  "Expiration" : "%s",
  "LastUpdated" : "2009-11-23T00:00:00Z"
}`

const credsFailRespTmpl = `{
  "Code": "ErrorCode",
  "Message": "ErrorMsg",
  "LastUpdated": "2009-11-23T00:00:00Z"
}`

type mockClient struct {
	t          *testing.T
	roleName   string
	failAssume bool
	expireOn   string
}

func (c mockClient) GetMetadata(
	ctx context.Context, params *imds.GetMetadataInput, optFns ...func(*imds.Options),
) (
	*imds.GetMetadataOutput, error,
) {
	switch params.Path {
	case iamSecurityCredsPath:
		return &imds.GetMetadataOutput{
			Content: ioutil.NopCloser(strings.NewReader(c.roleName)),
		}, nil

	case iamSecurityCredsPath + c.roleName:
		var w strings.Builder
		if c.failAssume {
			fmt.Fprintf(&w, credsFailRespTmpl)
		} else {
			fmt.Fprintf(&w, credsRespTmpl, c.expireOn)
		}
		return &imds.GetMetadataOutput{
			Content: ioutil.NopCloser(strings.NewReader(w.String())),
		}, nil
	default:
		return nil, fmt.Errorf("unexpected path, %v", params.Path)
	}
}

var (
	_ aws.AdjustExpiresByCredentialsCacheStrategy   = (*Provider)(nil)
	_ aws.HandleFailRefreshCredentialsCacheStrategy = (*Provider)(nil)
)

func TestProvider(t *testing.T) {
	orig := sdk.NowTime
	defer func() { sdk.NowTime = orig }()

	p := New(func(options *Options) {
		options.Client = mockClient{
			roleName:   "RoleName",
			failAssume: false,
			expireOn:   "2014-12-16T01:51:37Z",
		}
	})

	creds, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("expect no error, got %v", err)
	}
	if e, a := "accessKey", creds.AccessKeyID; e != a {
		t.Errorf("Expect access key ID to match")
	}
	if e, a := "secret", creds.SecretAccessKey; e != a {
		t.Errorf("Expect secret access key to match")
	}
	if e, a := "token", creds.SessionToken; e != a {
		t.Errorf("Expect session token to match")
	}

	sdk.NowTime = func() time.Time {
		return time.Date(2014, 12, 16, 0, 55, 37, 0, time.UTC)
	}

	if creds.Expired() {
		t.Errorf("Expect not expired")
	}
}

func TestProvider_FailAssume(t *testing.T) {
	p := New(func(options *Options) {
		options.Client = mockClient{
			roleName:   "RoleName",
			failAssume: true,
			expireOn:   "2014-12-16T01:51:37Z",
		}
	})

	creds, err := p.Retrieve(context.Background())
	if err == nil {
		t.Fatalf("expect error, got none")
	}

	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expect %T error, got %v", apiErr, err)
	}
	if e, a := "ErrorCode", apiErr.ErrorCode(); e != a {
		t.Errorf("expect %v code, got %v", e, a)
	}
	if e, a := "ErrorMsg", apiErr.ErrorMessage(); e != a {
		t.Errorf("expect %v message, got %v", e, a)
	}

	nestedErr := errors.Unwrap(apiErr)
	if nestedErr != nil {
		t.Fatalf("expect no nested error, got %v", err)
	}

	if e, a := "", creds.AccessKeyID; e != a {
		t.Errorf("Expect access key ID to match")
	}
	if e, a := "", creds.SecretAccessKey; e != a {
		t.Errorf("Expect secret access key to match")
	}
	if e, a := "", creds.SessionToken; e != a {
		t.Errorf("Expect session token to match")
	}
}

func TestProvider_IsExpired(t *testing.T) {
	orig := sdk.NowTime
	defer func() { sdk.NowTime = orig }()

	p := New(func(options *Options) {
		options.Client = mockClient{
			roleName:   "RoleName",
			failAssume: false,
			expireOn:   "2014-12-16T01:51:37Z",
		}
	})

	sdk.NowTime = func() time.Time {
		return time.Date(2014, 12, 16, 0, 55, 37, 0, time.UTC)
	}

	creds, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("expect no error, got %v", err)
	}
	if creds.Expired() {
		t.Errorf("expect not to be expired")
	}

	sdk.NowTime = func() time.Time {
		return time.Date(2014, 12, 16, 1, 55, 37, 0, time.UTC)
	}

	if !creds.Expired() {
		t.Errorf("expect to be expired")
	}
}

type byteReader byte

func (b byteReader) Read(p []byte) (int, error) {
	for i := 0; i < len(p); i++ {
		p[i] = byte(b)
	}
	return len(p), nil
}

func TestProvider_HandleFailToRetrieve(t *testing.T) {
	origTime := sdk.NowTime
	defer func() { sdk.NowTime = origTime }()
	sdk.NowTime = func() time.Time {
		return time.Date(2014, 04, 04, 0, 1, 0, 0, time.UTC)
	}

	origRand := sdkrand.Reader
	defer func() { sdkrand.Reader = origRand }()
	sdkrand.Reader = byteReader(0)

	cases := map[string]struct {
		creds        aws.Credentials
		err          error
		randReader   io.Reader
		expectCreds  aws.Credentials
		expectErr    string
		expectLogged string
	}{
		"expired low": {
			randReader: byteReader(0),
			creds: aws.Credentials{
				CanExpire: true,
				Expires:   sdk.NowTime().Add(-5 * time.Minute),
			},
			err: fmt.Errorf("some error"),
			expectCreds: aws.Credentials{
				CanExpire: true,
				Expires:   sdk.NowTime().Add(5 * time.Minute),
			},
			expectLogged: fmt.Sprintf("again in 5 minutes"),
		},
		"expired high": {
			randReader: byteReader(0xFF),
			creds: aws.Credentials{
				CanExpire: true,
				Expires:   sdk.NowTime().Add(-5 * time.Minute),
			},
			err: fmt.Errorf("some error"),
			expectCreds: aws.Credentials{
				CanExpire: true,
				Expires:   sdk.NowTime().Add(14*time.Minute + 59*time.Second),
			},
			expectLogged: fmt.Sprintf("again in 14 minutes"),
		},
		"not expired": {
			randReader: byteReader(0xFF),
			creds: aws.Credentials{
				CanExpire: true,
				Expires:   sdk.NowTime().Add(10 * time.Minute),
			},
			err: fmt.Errorf("some error"),
			expectCreds: aws.Credentials{
				CanExpire: true,
				Expires:   sdk.NowTime().Add(10 * time.Minute),
			},
		},
		"cannot expire": {
			randReader: byteReader(0xFF),
			creds: aws.Credentials{
				CanExpire: false,
			},
			err:       fmt.Errorf("some error"),
			expectErr: "some error",
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			sdkrand.Reader = c.randReader
			if sdkrand.Reader == nil {
				sdkrand.Reader = byteReader(0)
			}

			var logBuf bytes.Buffer
			logger := logging.LoggerFunc(func(class logging.Classification, format string, args ...interface{}) {
				fmt.Fprintf(&logBuf, string(class)+" "+format, args...)
			})
			ctx := middleware.SetLogger(context.Background(), logger)

			p := New()
			creds, err := p.HandleFailToRefresh(ctx, c.creds, c.err)
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

			if len(c.expectLogged) != 0 && logBuf.Len() == 0 {
				t.Errorf("expect %v logged, got none", c.expectLogged)
			}
			if e, a := c.expectLogged, logBuf.String(); !strings.Contains(a, e) {
				t.Errorf("expect %v to be logged in %v", e, a)
			}

			// Truncate time so it can be easily compared.
			creds.Expires = creds.Expires.Truncate(time.Second)

			if diff := cmp.Diff(c.expectCreds, creds); diff != "" {
				t.Errorf("expect creds match\n%s", diff)
			}
		})
	}
}

func TestProvider_AdjustExpiresBy(t *testing.T) {
	origTime := sdk.NowTime
	defer func() { sdk.NowTime = origTime }()
	sdk.NowTime = func() time.Time {
		return time.Date(2014, 04, 04, 0, 1, 0, 0, time.UTC)
	}

	cases := map[string]struct {
		creds       aws.Credentials
		dur         time.Duration
		expectCreds aws.Credentials
	}{
		"modify expires": {
			creds: aws.Credentials{
				CanExpire: true,
				Expires:   sdk.NowTime().Add(1 * time.Hour),
			},
			dur: -5 * time.Minute,
			expectCreds: aws.Credentials{
				CanExpire: true,
				Expires:   sdk.NowTime().Add(55 * time.Minute),
			},
		},
		"expiry too soon": {
			creds: aws.Credentials{
				CanExpire: true,
				Expires:   sdk.NowTime().Add(14*time.Minute + 59*time.Second),
			},
			dur: -5 * time.Minute,
			expectCreds: aws.Credentials{
				CanExpire: true,
				Expires:   sdk.NowTime().Add(14*time.Minute + 59*time.Second),
			},
		},
		"cannot expire": {
			creds: aws.Credentials{
				CanExpire: false,
			},
			dur: 10 * time.Minute,
			expectCreds: aws.Credentials{
				CanExpire: false,
			},
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			p := New()
			creds, err := p.AdjustExpiresBy(c.creds, c.dur)

			if err != nil {
				t.Fatalf("expect no error, got %v", err)
			}

			if diff := cmp.Diff(c.expectCreds, creds); diff != "" {
				t.Errorf("expect creds match\n%s", diff)
			}
		})
	}
}
