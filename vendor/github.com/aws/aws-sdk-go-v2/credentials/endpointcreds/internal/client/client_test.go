package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	smithytesting "github.com/aws/smithy-go/testing"
)

func TestClient_GetCredentials(t *testing.T) {
	cases := map[string]struct {
		Token           string
		RelativeURI     string
		ResponseCode    int
		ResponseBody    []byte
		ExpectResult    *GetCredentialsOutput
		ExpectErr       bool
		ValidateRequest func(*testing.T, *http.Request)
		ValidateError   func(*testing.T, error) bool
	}{
		"success static": {
			ResponseCode: 200,
			ResponseBody: []byte(` {
   "AccessKeyId" : "FooKey",
   "SecretAccessKey" : "FooSecret"
 }`),
			ExpectResult: &GetCredentialsOutput{
				AccessKeyID:     "FooKey",
				SecretAccessKey: "FooSecret",
			},
		},
		"success with authorization token": {
			Token:        "MySecretAuthToken",
			ResponseCode: 200,
			ResponseBody: []byte(` {
   "AccessKeyId" : "FooKey",
   "SecretAccessKey" : "FooSecret"
 }`),
			ExpectResult: &GetCredentialsOutput{
				AccessKeyID:     "FooKey",
				SecretAccessKey: "FooSecret",
			},
		},
		"success refreshable": {
			Token:        "MySecretAuthToken",
			ResponseCode: 200,
			ResponseBody: []byte(`{
  "AccessKeyId": "FooKey",
  "SecretAccessKey": "FooSecret",
  "Token": "FooToken",
  "Expiration": "2016-02-25T06:03:31Z"
}`),
			ExpectResult: &GetCredentialsOutput{
				AccessKeyID:     "FooKey",
				SecretAccessKey: "FooSecret",
				Token:           "FooToken",
				Expiration: func() *time.Time {
					t := time.Date(2016, 2, 25, 6, 3, 31, 0, time.UTC)
					return &t
				}(),
			},
		},
		"success with additional URI components": {
			RelativeURI:  "/path/to/thing?someKey=someValue",
			ResponseCode: 200,
			ResponseBody: []byte(` {
   "AccessKeyId" : "FooKey",
   "SecretAccessKey" : "FooSecret"
 }`),
			ValidateRequest: func(t *testing.T, r *http.Request) {
				t.Helper()
				if e, a := "/path/to/thing", r.URL.Path; e != a {
					t.Errorf("expect %v, got %v", e, a)
				}
				if e, a := "someValue", r.URL.Query().Get("someKey"); e != a {
					t.Errorf("expect %v, got %v", e, a)
				}
			},
			ExpectResult: &GetCredentialsOutput{
				AccessKeyID:     "FooKey",
				SecretAccessKey: "FooSecret",
			},
		},
		"json error response client fault": {
			ResponseCode: 403,
			ResponseBody: []byte(`{
  "code": "Unauthorized",
  "message": "not authorized for endpoint"
}`),
			ExpectErr: true,
			ValidateError: func(t *testing.T, err error) (ok bool) {
				t.Helper()
				var apiError smithy.APIError
				if errors.As(err, &apiError) {
					if e, a := "Unauthorized", apiError.ErrorCode(); e != a {
						t.Errorf("expect %v, got %v", e, a)
						ok = false
					}
					if e, a := "not authorized for endpoint", apiError.ErrorMessage(); e != a {
						t.Errorf("expect %v, got %v", e, a)
						ok = false
					}
					if e, a := smithy.FaultClient, apiError.ErrorFault(); e != a {
						t.Errorf("expect %v, got %v", e, a)
						ok = false
					}
				} else {
					t.Errorf("expect %T error type, got %T: %v", apiError, err, err)
					ok = false
				}
				return ok
			},
		},
		"json error response server fault": {
			ResponseCode: 500,
			ResponseBody: []byte(`{
  "code": "InternalError",
  "message": "an error occurred"
}`),
			ExpectErr: true,
			ValidateError: func(t *testing.T, err error) (ok bool) {
				t.Helper()
				var apiError smithy.APIError
				if errors.As(err, &apiError) {
					if e, a := "InternalError", apiError.ErrorCode(); e != a {
						t.Errorf("expect %v, got %v", e, a)
						ok = false
					}
					if e, a := "an error occurred", apiError.ErrorMessage(); e != a {
						t.Errorf("expect %v, got %v", e, a)
						ok = false
					}
					if e, a := smithy.FaultServer, apiError.ErrorFault(); e != a {
						t.Errorf("expect %v, got %v", e, a)
						ok = false
					}
				} else {
					t.Errorf("expect %T error type, got %T: %v", apiError, err, err)
					ok = false
				}
				return ok
			},
		},
		"non-json error response": {
			ResponseCode: 500,
			ResponseBody: []byte(`<html><body>unexpected message format</body></html>`),
			ExpectErr:    true,
			ValidateError: func(t *testing.T, err error) (ok bool) {
				t.Helper()
				if e, a := "failed to decode error message", err.Error(); !strings.Contains(a, e) {
					t.Errorf("expect %v, got %v", e, a)
					ok = false
				}
				return ok
			},
		},
	}

	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			var actualReq *http.Request
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				actualReq = r.Clone(r.Context())
				var buf bytes.Buffer
				if _, err := io.Copy(&buf, r.Body); err != nil {
					t.Errorf("failed to read r body, %v", err)
				}

				actualReq.Body = ioutil.NopCloser(bytes.NewReader(buf.Bytes()))

				w.WriteHeader(tt.ResponseCode)
				w.Write(tt.ResponseBody)
			}))
			defer server.Close()

			client := New(Options{Endpoint: server.URL + tt.RelativeURI})

			result, err := client.GetCredentials(context.Background(), &GetCredentialsInput{AuthorizationToken: tt.Token})
			if (err != nil) != tt.ExpectErr {
				t.Fatalf("got error %v, but ExpectErr=%v", err, tt.ExpectErr)
			}
			if err != nil && tt.ValidateError != nil {
				if !tt.ValidateError(t, err) {
					return
				}
			}

			if e, a := "application/json", actualReq.Header.Get("Accept"); e != a {
				t.Errorf("expect %v, got %v", e, a)
			}

			if len(tt.Token) > 0 {
				if e, a := tt.Token, actualReq.Header.Get("Authorization"); e != a {
					t.Errorf("expect %v, got %v", e, a)
				}
			}

			if tt.ValidateRequest != nil {
				tt.ValidateRequest(t, actualReq)
			}

			if err = smithytesting.CompareValues(tt.ExpectResult, result); err != nil {
				t.Errorf("expect value match:\n%v", err)
			}
		})
	}
}

func TestClient_GetCredentials_NoEndpoint(t *testing.T) {
	client := New(Options{})

	_, err := client.GetCredentials(context.Background(), &GetCredentialsInput{})
	if err == nil {
		t.Fatalf("expect error got none")
	}
	if e, a := "endpoint not provided", err.Error(); !strings.Contains(a, e) {
		t.Errorf("expect %v, got %v", e, a)
	}
}
