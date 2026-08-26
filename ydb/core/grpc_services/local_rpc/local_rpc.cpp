#include "local_rpc.h"

namespace NKikimr::NRpcService {

Ydb::StatusIds::StatusCode GrpcStatusToYdbStatus(grpc::StatusCode status) {
    switch (status) {
        case grpc::OK:
            return Ydb::StatusIds::SUCCESS;
        case grpc::CANCELLED:
            return Ydb::StatusIds::CANCELLED;
        case grpc::UNKNOWN:
            return Ydb::StatusIds::UNDETERMINED;
        case grpc::DEADLINE_EXCEEDED:
            return Ydb::StatusIds::TIMEOUT;
        case grpc::NOT_FOUND:
            return Ydb::StatusIds::NOT_FOUND;
        case grpc::ALREADY_EXISTS:
            return Ydb::StatusIds::ALREADY_EXISTS;
        case grpc::RESOURCE_EXHAUSTED:
            return Ydb::StatusIds::OVERLOADED;
        case grpc::ABORTED:
            return Ydb::StatusIds::ABORTED;
        case grpc::UNIMPLEMENTED:
            return Ydb::StatusIds::UNSUPPORTED;
        case grpc::UNAVAILABLE:
            return Ydb::StatusIds::UNAVAILABLE;
        case grpc::OUT_OF_RANGE:
        case grpc::INVALID_ARGUMENT:
        case grpc::FAILED_PRECONDITION:
            return Ydb::StatusIds::PRECONDITION_FAILED;
        case grpc::UNAUTHENTICATED:
        case grpc::PERMISSION_DENIED:
            return Ydb::StatusIds::UNAUTHORIZED;
        default:
            return Ydb::StatusIds::INTERNAL_ERROR;
    }
}

} // namespace NKikimr::NRpcService
