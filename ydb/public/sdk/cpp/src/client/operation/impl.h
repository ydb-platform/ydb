#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>


namespace NYdb::inline Dev::NOperation {

constexpr TDuration OPERATION_CLIENT_TIMEOUT = TDuration::Seconds(5);

class TOperationClient::TImpl : public TClientImplCommon<TOperationClient::TImpl> {
    template <typename TRequest, typename TResponse>
    using TSimpleRpc = TGRpcConnectionsImpl::TSimpleRpc<Ydb::Operation::V1::OperationService, TRequest, TResponse>;

    template <typename TRequest, typename TResponse>
    TAsyncStatus Run(TRequest&& request, TSimpleRpc<TRequest, TResponse> rpc) {
        auto promise = NThreading::NewPromise<TStatus>();

        auto extractor = [promise]
            (TResponse* response, TPlainStatus status) mutable {
                if (response) {
                    NYdb::NIssue::TIssues opIssues;
                    NYdb::NIssue::IssuesFromMessage(response->issues(), opIssues);
                    TStatus st(static_cast<EStatus>(response->status()), std::move(opIssues));
                    promise.SetValue(std::move(st));
                } else {
                    TStatus st(std::move(status));
                    promise.SetValue(std::move(st));
                }
            };

        TRpcRequestSettings rpcSettings;
        rpcSettings.ClientTimeout = OPERATION_CLIENT_TIMEOUT;

        Connections_->Run<Ydb::Operation::V1::OperationService, TRequest, TResponse>(
            std::move(request),
            extractor,
            rpc,
            DbDriverState_,
            rpcSettings);

        return promise.GetFuture();
    }

public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {
    }

    template <typename TOp>
    NThreading::TFuture<TOp> Get(Ydb::Operations::GetOperationRequest&& request) {
        return RunOperation<Ydb::Operation::V1::OperationService,
                            Ydb::Operations::GetOperationRequest,
                            Ydb::Operations::GetOperationResponse, TOp>(
            std::move(request),
            &Ydb::Operation::V1::OperationService::Stub::AsyncGetOperation);
    }

    TAsyncStatus Cancel(Ydb::Operations::CancelOperationRequest&& request) {
        return Run<Ydb::Operations::CancelOperationRequest, Ydb::Operations::CancelOperationResponse>(std::move(request),
            &Ydb::Operation::V1::OperationService::Stub::AsyncCancelOperation);
    }

    TAsyncStatus Forget(Ydb::Operations::ForgetOperationRequest&& request) {
        return Run<Ydb::Operations::ForgetOperationRequest, Ydb::Operations::ForgetOperationResponse>(std::move(request),
            &Ydb::Operation::V1::OperationService::Stub::AsyncForgetOperation);
    }

    template <typename TOp>
    NThreading::TFuture<TOperationsList<TOp>> List(Ydb::Operations::ListOperationsRequest&& request) {
        auto promise = NThreading::NewPromise<TOperationsList<TOp>>();

        auto extractor = [promise]
            (Ydb::Operations::ListOperationsResponse* response, TPlainStatus status) mutable {
                if (response) {
                    NYdb::NIssue::TIssues opIssues;
                    NYdb::NIssue::IssuesFromMessage(response->issues(), opIssues);
                    TStatus st(static_cast<EStatus>(response->status()), std::move(opIssues));

                    std::vector<TOp> operations;
                    operations.reserve(response->operations_size());
                    for (auto& operation : *response->mutable_operations()) {
                        NYdb::NIssue::TIssues opIssues;
                        NYdb::NIssue::IssuesFromMessage(operation.issues(), opIssues);
                        operations.emplace_back(
                            TStatus(static_cast<EStatus>(operation.status()), std::move(opIssues)),
                            std::move(operation));
                    }

                    promise.SetValue(TOperationsList<TOp>(std::move(st), std::move(operations), response->next_page_token()));
                } else {
                    TStatus st(std::move(status));
                    promise.SetValue(TOperationsList<TOp>(std::move(st)));
                }
            };

        TRpcRequestSettings rpcSettings;
        rpcSettings.ClientTimeout = OPERATION_CLIENT_TIMEOUT;

        Connections_->Run<Ydb::Operation::V1::OperationService, 
                          Ydb::Operations::ListOperationsRequest,
                          Ydb::Operations::ListOperationsResponse>(
            std::move(request),
            extractor,
            &Ydb::Operation::V1::OperationService::Stub::AsyncListOperations,
            DbDriverState_,
            rpcSettings);

        return promise.GetFuture();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TOp>
NThreading::TFuture<TOp> TOperationClient::Get(const TOperation::TOperationId& id) {
    auto request = MakeRequest<Ydb::Operations::GetOperationRequest>();
    request.set_id(TStringType{id.ToString()});

    return Impl_->Get<TOp>(std::move(request));
}

template <typename TOp>
NThreading::TFuture<TOperationsList<TOp>> TOperationClient::List(const std::string& kind, std::uint64_t pageSize, const std::string& pageToken) {
    auto request = MakeRequest<Ydb::Operations::ListOperationsRequest>();

    request.set_kind(TStringType{kind});
    if (pageSize) {
        request.set_page_size(pageSize);
    }
    if (!pageToken.empty()) {
        request.set_page_token(TStringType{pageToken});
    }

    return Impl_->List<TOp>(std::move(request));
}

}
