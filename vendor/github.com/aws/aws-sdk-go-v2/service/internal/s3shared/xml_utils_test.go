package s3shared

import (
	"strings"
	"testing"
)

func TestGetResponseErrorCode(t *testing.T) {
	const xmlErrorResponse = `<Error>
    <Type>Sender</Type>
    <Code>InvalidGreeting</Code>
    <Message>Hi</Message>
    <HostId>bar-id</HostId>
    <RequestId>foo-id</RequestId>
</Error>`

	const wrappedXMLErrorResponse = `<ErrorResponse><Error>
    <Type>Sender</Type>
    <Code>InvalidGreeting</Code>
    <Message>Hi</Message>
</Error>
    <HostId>bar-id</HostId>
    <RequestId>foo-id</RequestId>
</ErrorResponse>`

	cases := map[string]struct {
		getErr                 func() (ErrorComponents, error)
		expectedErrorCode      string
		expectedErrorMessage   string
		expectedErrorRequestID string
		expectedErrorHostID    string
	}{
		"standard xml error": {
			getErr: func() (ErrorComponents, error) {
				errResp := strings.NewReader(xmlErrorResponse)
				return GetErrorResponseComponents(errResp, ErrorResponseDeserializerOptions{
					UseStatusCode:         false,
					StatusCode:            0,
					IsWrappedWithErrorTag: false,
				})
			},
			expectedErrorCode:      "InvalidGreeting",
			expectedErrorMessage:   "Hi",
			expectedErrorRequestID: "foo-id",
			expectedErrorHostID:    "bar-id",
		},

		"s3 no response body": {
			getErr: func() (ErrorComponents, error) {
				errResp := strings.NewReader("")
				return GetErrorResponseComponents(errResp, ErrorResponseDeserializerOptions{
					UseStatusCode: true,
					StatusCode:    400,
				})
			},
			expectedErrorCode:    "BadRequest",
			expectedErrorMessage: "Bad Request",
		},
		"s3control no response body": {
			getErr: func() (ErrorComponents, error) {
				errResp := strings.NewReader("")
				return GetErrorResponseComponents(errResp, ErrorResponseDeserializerOptions{
					IsWrappedWithErrorTag: true,
				})
			},
		},
		"s3control standard response body": {
			getErr: func() (ErrorComponents, error) {
				errResp := strings.NewReader(wrappedXMLErrorResponse)
				return GetErrorResponseComponents(errResp, ErrorResponseDeserializerOptions{
					IsWrappedWithErrorTag: true,
				})
			},
			expectedErrorCode:      "InvalidGreeting",
			expectedErrorMessage:   "Hi",
			expectedErrorRequestID: "foo-id",
			expectedErrorHostID:    "bar-id",
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			ec, err := c.getErr()
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if e, a := c.expectedErrorCode, ec.Code; !strings.EqualFold(e, a) {
				t.Fatalf("expected %v, got %v", e, a)
			}
			if e, a := c.expectedErrorMessage, ec.Message; !strings.EqualFold(e, a) {
				t.Fatalf("expected %v, got %v", e, a)
			}
			if e, a := c.expectedErrorRequestID, ec.RequestID; !strings.EqualFold(e, a) {
				t.Fatalf("expected %v, got %v", e, a)
			}
			if e, a := c.expectedErrorHostID, ec.HostID; !strings.EqualFold(e, a) {
				t.Fatalf("expected %v, got %v", e, a)
			}
		})
	}
}
