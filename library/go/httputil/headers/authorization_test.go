package headers_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb/library/go/httputil/headers"
)

func TestAuthorizationTokenType(t *testing.T) {
	testCases := []struct {
		name     string
		token    string
		expected headers.TokenType
	}{
		{"bearer", "bearer ololo.trololo", headers.TokenTypeBearer},
		{"Bearer", "Bearer ololo.trololo", headers.TokenTypeBearer},
		{"BEARER", "BEARER ololo.trololo", headers.TokenTypeBearer},
		{"mac", "mac ololo.trololo", headers.TokenTypeMAC},
		{"Mac", "Mac ololo.trololo", headers.TokenTypeMAC},
		{"MAC", "MAC ololo.trololo", headers.TokenTypeMAC},
		{"unknown", "shimba ololo.trololo", headers.TokenType("unknown")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, headers.AuthorizationTokenType(tc.token))
		})
	}
}
