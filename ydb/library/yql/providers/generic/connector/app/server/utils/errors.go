package utils

import (
	"errors"
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb/library/go/core/log"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

var (
	ErrTableDoesNotExist                   = fmt.Errorf("table does not exist")
	ErrDataSourceNotSupported              = fmt.Errorf("data source not supported")
	ErrDataTypeNotSupported                = fmt.Errorf("data type not supported")
	ErrReadLimitExceeded                   = fmt.Errorf("read limit exceeded")
	ErrInvalidRequest                      = fmt.Errorf("invalid request")
	ErrValueOutOfTypeBounds                = fmt.Errorf("value is out of possible range of values for the type")
	ErrUnimplemented                       = fmt.Errorf("unimplemented")
	ErrUnimplementedTypedValue             = fmt.Errorf("unimplemented typed value")
	ErrUnimplementedExpression             = fmt.Errorf("unimplemented expression")
	ErrUnsupportedExpression               = fmt.Errorf("expression is not supported")
	ErrUnimplementedOperation              = fmt.Errorf("unimplemented operation")
	ErrUnimplementedPredicateType          = fmt.Errorf("unimplemented predicate type")
	ErrInvariantViolation                  = fmt.Errorf("implementation error (invariant violation)")
	ErrUnimplementedArithmeticalExpression = fmt.Errorf("unimplemented arithmetical expression")
	ErrEmptyTableName                      = fmt.Errorf("empty table name")
	ErrPageSizeExceeded                    = fmt.Errorf("page size exceeded, check service configuration")
)

func NewSuccess() *api_service_protos.TError {
	return &api_service_protos.TError{
		Status:  Ydb.StatusIds_SUCCESS,
		Message: "succeeded",
	}
}

func IsSuccess(apiErr *api_service_protos.TError) bool {
	if apiErr.Status == Ydb.StatusIds_STATUS_CODE_UNSPECIFIED {
		panic("status uninitialized")
	}

	return apiErr.Status == Ydb.StatusIds_SUCCESS
}

func NewAPIErrorFromStdError(err error) *api_service_protos.TError {
	var status Ydb.StatusIds_StatusCode

	switch {
	case errors.Is(err, ErrTableDoesNotExist):
		status = Ydb.StatusIds_NOT_FOUND
	case errors.Is(err, ErrReadLimitExceeded):
		// Return BAD_REQUEST to avoid retrying
		status = Ydb.StatusIds_BAD_REQUEST
	case errors.Is(err, ErrInvalidRequest):
		status = Ydb.StatusIds_BAD_REQUEST
	case errors.Is(err, ErrDataSourceNotSupported):
		status = Ydb.StatusIds_UNSUPPORTED
	case errors.Is(err, ErrDataTypeNotSupported):
		status = Ydb.StatusIds_UNSUPPORTED
	case errors.Is(err, ErrValueOutOfTypeBounds):
		status = Ydb.StatusIds_UNSUPPORTED
	case errors.Is(err, ErrUnimplementedTypedValue):
		status = Ydb.StatusIds_UNSUPPORTED
	case errors.Is(err, ErrUnimplementedExpression):
		status = Ydb.StatusIds_UNSUPPORTED
	case errors.Is(err, ErrUnimplementedOperation):
		status = Ydb.StatusIds_UNSUPPORTED
	case errors.Is(err, ErrUnimplementedPredicateType):
		status = Ydb.StatusIds_UNSUPPORTED
	case errors.Is(err, ErrUnimplemented):
		status = Ydb.StatusIds_UNSUPPORTED
	case errors.Is(err, ErrUnimplementedArithmeticalExpression):
		status = Ydb.StatusIds_UNSUPPORTED
	case errors.Is(err, ErrEmptyTableName):
		status = Ydb.StatusIds_BAD_REQUEST
	case errors.Is(err, ErrPageSizeExceeded):
		status = Ydb.StatusIds_INTERNAL_ERROR
	default:
		status = Ydb.StatusIds_INTERNAL_ERROR
	}

	return &api_service_protos.TError{
		Status:  status,
		Message: err.Error(),
	}
}

func APIErrorToLogFields(apiErr *api_service_protos.TError) []log.Field {
	return []log.Field{
		log.String("message", apiErr.Message),
		log.String("status", apiErr.Status.String()),
	}
}

func NewSTDErrorFromAPIError(apiErr *api_service_protos.TError) error {
	if IsSuccess(apiErr) {
		return nil
	}

	return errors.New(apiErr.Message)
}
