#define INCLUDE_YDB_INTERNAL_H
#include "status.h"

#include <ydb-cpp-sdk/client/resources/ydb_resources.h>

#include <util/string/builder.h>

namespace NYdb::inline V3 {

using std::string;

TPlainStatus::TPlainStatus(
    const NYdbGrpc::TGrpcStatus& grpcStatus,
    const string& endpoint,
    std::multimap<std::string, std::string>&& metadata)
    : Endpoint(endpoint)
    , Metadata(std::move(metadata))
{
    std::string msg;
    if (grpcStatus.InternalError) {
        Status = EStatus::CLIENT_INTERNAL_ERROR;
        if (!grpcStatus.Msg.empty()) {
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
    if (!msg.empty()) {
        Issues.AddIssue(NYdb::NIssue::TIssue(msg));
    }
    for (const auto& [name, value] : grpcStatus.ServerTrailingMetadata) {
        Metadata.emplace(
            std::string(name.begin(), name.end()),
            std::string(value.begin(), value.end())
        );
    }

    InitCostInfo();
}

TPlainStatus TPlainStatus::Internal(const std::string& message) {
    return { EStatus::CLIENT_INTERNAL_ERROR, "Internal client error: " + message };
}

void TPlainStatus::InitCostInfo() {
    if (auto metaIt = Metadata.find(YDB_CONSUMED_UNITS_HEADER); metaIt != Metadata.end()) {
        try {
            CostInfo.set_consumed_units(std::stod(metaIt->second));
        } catch (std::exception& e) {
            if (Ok()) {
                Status = EStatus::CLIENT_INTERNAL_ERROR;
            }

            Issues.AddIssue(NIssue::TIssue{"Failed to parse CostInfo from Metadata: " + std::string{e.what()}});
        }
    }
}

} // namespace NYdb
