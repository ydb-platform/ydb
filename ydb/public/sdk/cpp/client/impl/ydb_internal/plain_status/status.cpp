#define INCLUDE_YDB_INTERNAL_H
#include "status.h"

#include <util/string/builder.h>

namespace NYdb {

using std::string;

TPlainStatus::TPlainStatus(
    const NYdbGrpc::TGrpcStatus& grpcStatus,
    const string& endpoint,
    std::multimap<TStringType, TStringType>&& metadata)
    : Endpoint(endpoint)
    , Metadata(std::move(metadata))
{
    TStringType msg;
    if (grpcStatus.InternalError) {
        Status = EStatus::CLIENT_INTERNAL_ERROR;
        if (grpcStatus.Msg) {
            msg = TStringBuilder() << "Internal client error: " << grpcStatus.Msg;
        } else {
            msg = "Unknown internal client error";
        }
    } else if (grpcStatus.GRpcStatusCode != grpc::StatusCode::OK) {
        switch (grpcStatus.GRpcStatusCode) {
            case grpc::StatusCode::UNAVAILABLE:
                Status = EStatus::TRANSPORT_UNAVAILABLE;
                break;
            case grpc::StatusCode::CANCELLED:
                Status = EStatus::CLIENT_CANCELLED;
                break;
            case grpc::StatusCode::UNAUTHENTICATED:
                Status = EStatus::CLIENT_UNAUTHENTICATED;
                break;
            case grpc::StatusCode::UNIMPLEMENTED:
                Status = EStatus::CLIENT_CALL_UNIMPLEMENTED;
                break;
            case grpc::StatusCode::RESOURCE_EXHAUSTED:
                Status = EStatus::CLIENT_RESOURCE_EXHAUSTED;
                break;
            case grpc::StatusCode::DEADLINE_EXCEEDED:
                Status = EStatus::CLIENT_DEADLINE_EXCEEDED;
                break;
            case grpc::StatusCode::OUT_OF_RANGE:
                Status = EStatus::CLIENT_OUT_OF_RANGE;
                break;
            default:
                Status = EStatus::CLIENT_INTERNAL_ERROR;
                break;
        }
        msg = TStringBuilder() << "GRpc error: (" << grpcStatus.GRpcStatusCode << "): " << grpcStatus.Msg;
    } else {
        Status = EStatus::SUCCESS;
    }
    if (msg) {
        Issues.AddIssue(NYql::TIssue(msg));
    }
    for (const auto& [name, value] : grpcStatus.ServerTrailingMetadata) {
        Metadata.emplace(
            TStringType(name.begin(), name.end()),
            TStringType(value.begin(), value.end())
        );
    }
}

TPlainStatus TPlainStatus::Internal(const TStringType& message) {
    return { EStatus::CLIENT_INTERNAL_ERROR, "Internal client error: " + message };
}

} // namespace NYdb
