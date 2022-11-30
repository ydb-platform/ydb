package headers

import "strings"

const (
	AuthorizationKey = "Authorization"

	TokenTypeBearer TokenType = "bearer"
	TokenTypeMAC    TokenType = "mac"
)

type TokenType string

// String implements stringer interface
func (tt TokenType) String() string {
	return string(tt)
}

func AuthorizationTokenType(token string) TokenType {
	if len(token) > len(TokenTypeBearer) &&
		strings.ToLower(token[:len(TokenTypeBearer)]) == TokenTypeBearer.String() {
		return TokenTypeBearer
	}

	if len(token) > len(TokenTypeMAC) &&
		strings.ToLower(token[:len(TokenTypeMAC)]) == TokenTypeMAC.String() {
		return TokenTypeMAC
	}

	return TokenType("unknown")
}
