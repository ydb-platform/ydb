package tvmtool

import (
	"fmt"

	"a.yandex-team.ru/library/go/yandex/tvm"
)

// Generic TVM errors, before retry any request it check .Retriable field.
type Error = tvm.Error

const (
	// ErrorAuthFail - auth failed, probably you provides invalid auth token
	ErrorAuthFail = tvm.ErrorAuthFail
	// ErrorBadRequest - tvmtool rejected our request, check .Msg for details
	ErrorBadRequest = tvm.ErrorBadRequest
	// ErrorOther - any other TVM-related errors, check .Msg for details
	ErrorOther = tvm.ErrorOther
)

// Ticket validation error
type TicketError = tvm.TicketError

const (
	TicketErrorInvalidScopes = tvm.TicketInvalidScopes
	TicketErrorOther         = tvm.TicketStatusOther
)

type PingCode uint32

const (
	PingCodeDie = iota
	PingCodeWarning
	PingCodeError
	PingCodeOther
)

func (e PingCode) String() string {
	switch e {
	case PingCodeDie:
		return "HttpDie"
	case PingCodeWarning:
		return "Warning"
	case PingCodeError:
		return "Error"
	case PingCodeOther:
		return "Other"
	default:
		return fmt.Sprintf("Unknown%d", e)
	}
}

// Special ping error
type PingError struct {
	Code PingCode
	Err  error
}

func (e *PingError) Error() string {
	return fmt.Sprintf("tvm: %s (code %s)", e.Err.Error(), e.Code)
}
