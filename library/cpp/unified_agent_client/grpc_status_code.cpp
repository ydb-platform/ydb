#include <contrib/libs/grpc/include/grpcpp/impl/codegen/status_code_enum.h>

#include <util/stream/output.h>

namespace  {
    const char* GrpcStatusCodeToString(grpc::StatusCode statusCode) {
        switch (statusCode) {
            case grpc::OK:
                return "OK";
            case grpc::CANCELLED:
                return "CANCELLED";
            case grpc::UNKNOWN:
                return "UNKNOWN";
            case grpc::INVALID_ARGUMENT:
                return "INVALID_ARGUMENT";
            case grpc::DEADLINE_EXCEEDED:
                return "DEADLINE_EXCEEDED";
            case grpc::NOT_FOUND:
                return "NOT_FOUND";
            case grpc::ALREADY_EXISTS:
                return "ALREADY_EXISTS";
            case grpc::PERMISSION_DENIED:
                return "PERMISSION_DENIED";
            case grpc::UNAUTHENTICATED:
                return "UNAUTHENTICATED";
            case grpc::RESOURCE_EXHAUSTED:
                return "RESOURCE_EXHAUSTED";
            case grpc::FAILED_PRECONDITION:
                return "FAILED_PRECONDITION";
            case grpc::ABORTED:
                return "ABORTED";
            case grpc::OUT_OF_RANGE:
                return "OUT_OF_RANGE";
            case grpc::UNIMPLEMENTED:
                return "UNIMPLEMENTED";
            case grpc::INTERNAL:
                return "INTERNAL";
            case grpc::UNAVAILABLE:
                return "UNAVAILABLE";
            case grpc::DATA_LOSS:
                return "DATA_LOSS";
            default:
                return nullptr;
        }
    }
}

template <>
void Out<grpc::StatusCode>(IOutputStream& o, grpc::StatusCode statusCode) {
    const auto* s = GrpcStatusCodeToString(statusCode);
    if (s == nullptr) {
        o << "grpc::StatusCode [" << static_cast<int>(statusCode) << "]";
    } else {
        o << s;
    }
}
