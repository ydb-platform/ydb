#include "dq.h"

#include <yql/essentials/core/issue/yql_issue.h>

namespace NYql::NDq {

Ydb::StatusIds::StatusCode DqStatusToYdbStatus(NYql::NDqProto::StatusIds::StatusCode statusCode) {
    switch (statusCode) {
    case NYql::NDqProto::StatusIds::UNSPECIFIED:
        return Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    case NYql::NDqProto::StatusIds::SUCCESS:
        return Ydb::StatusIds::SUCCESS;
    case NYql::NDqProto::StatusIds::INTERNAL_ERROR:
        return Ydb::StatusIds::INTERNAL_ERROR;
    case NYql::NDqProto::StatusIds::TIMEOUT:
        return Ydb::StatusIds::TIMEOUT;
    case NYql::NDqProto::StatusIds::ABORTED:
        return Ydb::StatusIds::ABORTED;
    case NYql::NDqProto::StatusIds::UNAVAILABLE:
        return Ydb::StatusIds::UNAVAILABLE;
    case NYql::NDqProto::StatusIds::UNDETERMINED:
        return Ydb::StatusIds::UNDETERMINED;
    case NYql::NDqProto::StatusIds::BAD_REQUEST:
        return Ydb::StatusIds::BAD_REQUEST;
    case NYql::NDqProto::StatusIds::PRECONDITION_FAILED:
        return Ydb::StatusIds::PRECONDITION_FAILED;
    case NYql::NDqProto::StatusIds::CANCELLED:
        return Ydb::StatusIds::CANCELLED;
    case NYql::NDqProto::StatusIds::OVERLOADED:
        return Ydb::StatusIds::OVERLOADED;
    case NYql::NDqProto::StatusIds::LIMIT_EXCEEDED:
        return Ydb::StatusIds::PRECONDITION_FAILED;
    case NYql::NDqProto::StatusIds::EXTERNAL_ERROR:
        return Ydb::StatusIds::EXTERNAL_ERROR;
    case NYql::NDqProto::StatusIds::SCHEME_ERROR:
        return Ydb::StatusIds::SCHEME_ERROR;
    case NYql::NDqProto::StatusIds::UNSUPPORTED:
        return Ydb::StatusIds::UNSUPPORTED;
    case NYql::NDqProto::StatusIds::UNAUTHORIZED:
        return Ydb::StatusIds::UNAUTHORIZED;
    case NYql::NDqProto::StatusIds::GENERIC_ERROR:
    default:
        return Ydb::StatusIds::GENERIC_ERROR;
    }
}

NYql::NDqProto::StatusIds::StatusCode YdbStatusToDqStatus(Ydb::StatusIds::StatusCode statusCode, EStatusCompatibilityLevel compatibility) {
    switch(statusCode) {
    case Ydb::StatusIds::STATUS_CODE_UNSPECIFIED:
        return NYql::NDqProto::StatusIds::UNSPECIFIED;
    case Ydb::StatusIds::SUCCESS:
        return NYql::NDqProto::StatusIds::SUCCESS;
    case Ydb::StatusIds::BAD_REQUEST:
        return NYql::NDqProto::StatusIds::BAD_REQUEST;
    case Ydb::StatusIds::UNAUTHORIZED:
        return compatibility >= EStatusCompatibilityLevel::WithUnauthorized
            ? NYql::NDqProto::StatusIds::UNAUTHORIZED
            : NYql::NDqProto::StatusIds::INTERNAL_ERROR;
    case Ydb::StatusIds::INTERNAL_ERROR:
        return NYql::NDqProto::StatusIds::INTERNAL_ERROR;
    case Ydb::StatusIds::ABORTED:
        return NYql::NDqProto::StatusIds::ABORTED;
    case Ydb::StatusIds::UNAVAILABLE:
        return NYql::NDqProto::StatusIds::UNAVAILABLE;
    case Ydb::StatusIds::UNDETERMINED:
        return NYql::NDqProto::StatusIds::UNDETERMINED;
    case Ydb::StatusIds::OVERLOADED:
        return NYql::NDqProto::StatusIds::OVERLOADED;
    case Ydb::StatusIds::TIMEOUT:
        return NYql::NDqProto::StatusIds::TIMEOUT;
    case Ydb::StatusIds::BAD_SESSION:
        return NYql::NDqProto::StatusIds::BAD_REQUEST;
    case Ydb::StatusIds::PRECONDITION_FAILED:
        return NYql::NDqProto::StatusIds::PRECONDITION_FAILED;
    case Ydb::StatusIds::CANCELLED:
        return NYql::NDqProto::StatusIds::CANCELLED;
    case Ydb::StatusIds::SCHEME_ERROR:
        return NYql::NDqProto::StatusIds::SCHEME_ERROR;
    case Ydb::StatusIds::UNSUPPORTED:
        return NYql::NDqProto::StatusIds::UNSUPPORTED;
    case Ydb::StatusIds::EXTERNAL_ERROR:
        return NYql::NDqProto::StatusIds::EXTERNAL_ERROR;
    case Ydb::StatusIds::GENERIC_ERROR:
    default:
        return NYql::NDqProto::StatusIds::GENERIC_ERROR;
    }
}

TMaybe<NYql::NDqProto::StatusIds::StatusCode> GetDqStatus(const TIssue& issue) {
    if (issue.GetSeverity() == TSeverityIds::S_FATAL) {
        return NYql::NDqProto::StatusIds::INTERNAL_ERROR;
    }

    switch (issue.GetCode()) {
        case NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED:
        case NYql::TIssuesIds::KIKIMR_LOCKS_ACQUIRE_FAILURE:
        case NYql::TIssuesIds::KIKIMR_OPERATION_ABORTED:
        case NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH:
            return NYql::NDqProto::StatusIds::ABORTED;

        case NYql::TIssuesIds::KIKIMR_SCHEME_ERROR:
            return NYql::NDqProto::StatusIds::SCHEME_ERROR;

        case NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE:
            return NYql::NDqProto::StatusIds::UNAVAILABLE;

        case NYql::TIssuesIds::KIKIMR_OVERLOADED:
        case NYql::TIssuesIds::KIKIMR_MULTIPLE_SCHEME_MODIFICATIONS:
            return NYql::NDqProto::StatusIds::OVERLOADED;

        case NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION:
        case NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED:
            return NYql::NDqProto::StatusIds::PRECONDITION_FAILED;

        case NYql::TIssuesIds::KIKIMR_BAD_REQUEST:
        case NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE:
        case NYql::TIssuesIds::KIKIMR_NO_COLUMN_DEFAULT_VALUE:
            return NYql::NDqProto::StatusIds::BAD_REQUEST;

        case NYql::TIssuesIds::KIKIMR_ACCESS_DENIED:
            return NYql::NDqProto::StatusIds::UNAUTHORIZED;

        case NYql::TIssuesIds::KIKIMR_TIMEOUT:
            return NYql::NDqProto::StatusIds::TIMEOUT;

        case NYql::TIssuesIds::KIKIMR_OPERATION_CANCELLED:
            return NYql::NDqProto::StatusIds::CANCELLED;

        case NYql::TIssuesIds::KIKIMR_RESULT_UNAVAILABLE:
        case NYql::TIssuesIds::KIKIMR_OPERATION_STATE_UNKNOWN:
            return NYql::NDqProto::StatusIds::UNDETERMINED;

        case NYql::TIssuesIds::KIKIMR_UNSUPPORTED:
            return NYql::NDqProto::StatusIds::UNSUPPORTED;

        default:
            break;
    }

    return Nothing();
}

} // namespace NYql::NDq
