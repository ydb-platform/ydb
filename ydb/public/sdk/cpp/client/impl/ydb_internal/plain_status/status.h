#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/internal_header.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/common/type_switcher.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>

#include <ydb/public/api/protos/ydb_operation.pb.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/library/grpc/client/grpc_client_low.h>

namespace NYdb {

struct TPlainStatus {
    EStatus Status;
    NYql::TIssues Issues;
    TStringType Endpoint;
    std::multimap<TStringType, TStringType> Metadata;
    Ydb::CostInfo ConstInfo;

    TPlainStatus()
        : Status(EStatus::SUCCESS)
    { }

    TPlainStatus(EStatus status, NYql::TIssues&& issues)
        : Status(status)
        , Issues(std::move(issues))
    { }

    TPlainStatus(EStatus status, NYql::TIssues&& issues, const std::string& endpoint,
        std::multimap<TStringType, TStringType>&& metadata)
        : Status(status)
        , Issues(std::move(issues))
        , Endpoint(endpoint)
        , Metadata(std::move(metadata))
    { }

    TPlainStatus(EStatus status, const TStringType& message)
        : Status(status)
    {
        if (!message.empty()) {
            Issues.AddIssue(NYql::TIssue(message));
        }
    }

    TPlainStatus(
        const NYdbGrpc::TGrpcStatus& grpcStatus, const std::string& endpoint = std::string(),
        std::multimap<TStringType, TStringType>&& metadata = {});

    template<class T>
    void SetCostInfo(T&& costInfo) {
        ConstInfo = std::forward<T>(costInfo);
    }

    bool Ok() const {
        return Status == EStatus::SUCCESS;
    }

    static TPlainStatus Internal(const TStringType& message);

    bool IsTransportError() const {
        auto status = static_cast<size_t>(Status);
        return TRANSPORT_STATUSES_FIRST <= status && status <= TRANSPORT_STATUSES_LAST;
    }

    TStringBuilder ToDebugString() const {
        TStringBuilder ret;
        ret << "Status: " << Status;
        if(!Ok())
            ret << ", Description: " << SubstGlobalCopy(Issues.ToString(), '\n', ' ');
        return ret;
    }


};

} // namespace NYdb
