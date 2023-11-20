#include "dq.h"

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
    case NYql::NDqProto::StatusIds::GENERIC_ERROR:
    default:
        return Ydb::StatusIds::GENERIC_ERROR;
    }
}

NYql::NDqProto::StatusIds::StatusCode YdbStatusToDqStatus(Ydb::StatusIds::StatusCode statusCode) {
    switch(statusCode) {
    case Ydb::StatusIds::STATUS_CODE_UNSPECIFIED:
        return NYql::NDqProto::StatusIds::UNSPECIFIED;
    case Ydb::StatusIds::SUCCESS:
        return NYql::NDqProto::StatusIds::SUCCESS;
    case Ydb::StatusIds::BAD_REQUEST:
        return NYql::NDqProto::StatusIds::BAD_REQUEST;
    case Ydb::StatusIds::UNAUTHORIZED:
    case Ydb::StatusIds::INTERNAL_ERROR:
        return NYql::NDqProto::StatusIds::INTERNAL_ERROR;
    case Ydb::StatusIds::ABORTED:
        return NYql::NDqProto::StatusIds::ABORTED;
    case Ydb::StatusIds::UNAVAILABLE:
        return NYql::NDqProto::StatusIds::UNAVAILABLE;
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
    case Ydb::StatusIds::GENERIC_ERROR:
        return NYql::NDqProto::StatusIds::GENERIC_ERROR;
    case Ydb::StatusIds::EXTERNAL_ERROR:
        return NYql::NDqProto::StatusIds::EXTERNAL_ERROR;
    default:
        return NYql::NDqProto::StatusIds::UNSPECIFIED;
    }
}

} // namespace NYql::NDq
