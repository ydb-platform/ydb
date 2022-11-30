package tvm

import (
	"errors"
	"fmt"
)

// ErrNotSupported - error to be used within cgo disabled builds.
var ErrNotSupported = errors.New("ticket_parser2 is not available when building with -DCGO_ENABLED=0")

var (
	ErrTicketExpired            = &TicketError{Status: TicketExpired}
	ErrTicketInvalidBlackboxEnv = &TicketError{Status: TicketInvalidBlackboxEnv}
	ErrTicketInvalidDst         = &TicketError{Status: TicketInvalidDst}
	ErrTicketInvalidTicketType  = &TicketError{Status: TicketInvalidTicketType}
	ErrTicketMalformed          = &TicketError{Status: TicketMalformed}
	ErrTicketMissingKey         = &TicketError{Status: TicketMissingKey}
	ErrTicketSignBroken         = &TicketError{Status: TicketSignBroken}
	ErrTicketUnsupportedVersion = &TicketError{Status: TicketUnsupportedVersion}
	ErrTicketStatusOther        = &TicketError{Status: TicketStatusOther}
	ErrTicketInvalidScopes      = &TicketError{Status: TicketInvalidScopes}
	ErrTicketInvalidSrcID       = &TicketError{Status: TicketInvalidSrcID}
)

type TicketError struct {
	Status TicketStatus
	Msg    string
}

func (e *TicketError) Is(err error) bool {
	otherTickerErr, ok := err.(*TicketError)
	if !ok {
		return false
	}
	if e == nil && otherTickerErr == nil {
		return true
	}
	if e == nil || otherTickerErr == nil {
		return false
	}
	return e.Status == otherTickerErr.Status
}

func (e *TicketError) Error() string {
	if e.Msg != "" {
		return fmt.Sprintf("tvm: invalid ticket: %s: %s", e.Status, e.Msg)
	}
	return fmt.Sprintf("tvm: invalid ticket: %s", e.Status)
}

type ErrorCode int

// This constants must be in sync with code in go/tvmauth/tvm.cpp:CatchError
const (
	ErrorOK ErrorCode = iota
	ErrorMalformedSecret
	ErrorMalformedKeys
	ErrorEmptyKeys
	ErrorNotAllowed
	ErrorBrokenTvmClientSettings
	ErrorMissingServiceTicket
	ErrorPermissionDenied
	ErrorOther

	// Go-only errors below
	ErrorBadRequest
	ErrorAuthFail
)

func (e ErrorCode) String() string {
	switch e {
	case ErrorOK:
		return "OK"
	case ErrorMalformedSecret:
		return "MalformedSecret"
	case ErrorMalformedKeys:
		return "MalformedKeys"
	case ErrorEmptyKeys:
		return "EmptyKeys"
	case ErrorNotAllowed:
		return "NotAllowed"
	case ErrorBrokenTvmClientSettings:
		return "BrokenTvmClientSettings"
	case ErrorMissingServiceTicket:
		return "MissingServiceTicket"
	case ErrorPermissionDenied:
		return "PermissionDenied"
	case ErrorOther:
		return "Other"
	case ErrorBadRequest:
		return "ErrorBadRequest"
	case ErrorAuthFail:
		return "AuthFail"
	default:
		return fmt.Sprintf("Unknown%d", e)
	}
}

type Error struct {
	Code      ErrorCode
	Retriable bool
	Msg       string
}

func (e *Error) Error() string {
	return fmt.Sprintf("tvm: %s (code %s)", e.Msg, e.Code)
}
