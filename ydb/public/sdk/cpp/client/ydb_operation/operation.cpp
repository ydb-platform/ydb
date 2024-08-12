#include "operation.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

/* Headers below used to instantiate concrete 'Get' & 'List' methods */
#include <ydb/public/sdk/cpp/client/ydb_query/query.h>
#include <ydb/public/sdk/cpp/client/ydb_export/export.h>
#include <ydb/public/sdk/cpp/client/ydb_import/import.h>
#include <ydb/public/sdk/cpp/client/ydb_ss_tasks/task.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <ydb/public/lib/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

namespace NYdb {
namespace NOperation {

constexpr TDuration OPERATION_CLIENT_TIMEOUT = TDuration::Seconds(5);

using namespace NThreading;
using namespace Ydb::Operation;
using namespace Ydb::Operations;

class TOperationClient::TImpl : public TClientImplCommon<TOperationClient::TImpl> {
    template <typename TRequest, typename TResponse>
    using TSimpleRpc = TGRpcConnectionsImpl::TSimpleRpc<V1::OperationService, TRequest, TResponse>;

    template <typename TRequest, typename TResponse>
    TAsyncStatus Run(TRequest&& request, TSimpleRpc<TRequest, TResponse> rpc) {
        auto promise = NewPromise<TStatus>();

        auto extractor = [promise]
            (TResponse* response, TPlainStatus status) mutable {
                if (response) {
                    NYql::TIssues opIssues;
                    NYql::IssuesFromMessage(response->issues(), opIssues);
                    TStatus st(static_cast<EStatus>(response->status()), std::move(opIssues));
                    promise.SetValue(std::move(st));
                } else {
                    TStatus st(std::move(status));
                    promise.SetValue(std::move(st));
                }
            };

        TRpcRequestSettings rpcSettings;
        rpcSettings.ClientTimeout = OPERATION_CLIENT_TIMEOUT;

        Connections_->Run<V1::OperationService, TRequest, TResponse>(
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
    TFuture<TOp> Get(GetOperationRequest&& request) {
        return RunOperation<V1::OperationService, GetOperationRequest, GetOperationResponse, TOp>(
            std::move(request),
            &V1::OperationService::Stub::AsyncGetOperation);
    }

    TAsyncStatus Cancel(CancelOperationRequest&& request) {
        return Run<CancelOperationRequest, CancelOperationResponse>(std::move(request),
            &V1::OperationService::Stub::AsyncCancelOperation);
    }

    TAsyncStatus Forget(ForgetOperationRequest&& request) {
        return Run<ForgetOperationRequest, ForgetOperationResponse>(std::move(request),
            &V1::OperationService::Stub::AsyncForgetOperation);
    }

    template <typename TOp>
    TFuture<TOperationsList<TOp>> List(ListOperationsRequest&& request) {
        auto promise = NewPromise<TOperationsList<TOp>>();

        auto extractor = [promise]
            (ListOperationsResponse* response, TPlainStatus status) mutable {
                if (response) {
                    NYql::TIssues opIssues;
                    NYql::IssuesFromMessage(response->issues(), opIssues);
                    TStatus st(static_cast<EStatus>(response->status()), std::move(opIssues));

                    TVector<TOp> operations(Reserve(response->operations_size()));
                    for (auto& operation : *response->mutable_operations()) {
                        NYql::TIssues opIssues;
                        NYql::IssuesFromMessage(operation.issues(), opIssues);
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

        Connections_->Run<V1::OperationService, ListOperationsRequest, ListOperationsResponse>(
            std::move(request),
            extractor,
            &V1::OperationService::Stub::AsyncListOperations,
            DbDriverState_,
            rpcSettings);

        return promise.GetFuture();
    }

};

////////////////////////////////////////////////////////////////////////////////

TOperationClient::TOperationClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{
}

template <typename TOp>
TFuture<TOp> TOperationClient::Get(const TOperation::TOperationId& id) {
    auto request = MakeRequest<GetOperationRequest>();
    request.set_id(NKikimr::NOperationId::ProtoToString(id));

    return Impl_->Get<TOp>(std::move(request));
}

TAsyncStatus TOperationClient::Cancel(const TOperation::TOperationId& id) {
    auto request = MakeRequest<CancelOperationRequest>();
    request.set_id(NKikimr::NOperationId::ProtoToString(id));

    return Impl_->Cancel(std::move(request));
}

TAsyncStatus TOperationClient::Forget(const TOperation::TOperationId& id) {
    auto request = MakeRequest<ForgetOperationRequest>();
    request.set_id(NKikimr::NOperationId::ProtoToString(id));

    return Impl_->Forget(std::move(request));
}

template <typename TOp>
TFuture<TOperationsList<TOp>> TOperationClient::List(const TString& kind, ui64 pageSize, const TString& pageToken) {
    auto request = MakeRequest<ListOperationsRequest>();

    request.set_kind(kind);
    if (pageSize) {
        request.set_page_size(pageSize);
    }
    if (pageToken) {
        request.set_page_token(pageToken);
    }

    return Impl_->List<TOp>(std::move(request));
}

// Instantiations
template TFuture<NSchemeShard::TBackgroundProcessesResponse> TOperationClient::Get(const TOperation::TOperationId& id);
template <>
TFuture<TOperationsList<NSchemeShard::TBackgroundProcessesResponse>> TOperationClient::List(ui64 pageSize, const TString& pageToken) {
    return List<NSchemeShard::TBackgroundProcessesResponse>("ss/backgrounds", pageSize, pageToken);
}

template TFuture<NExport::TExportToYtResponse> TOperationClient::Get(const TOperation::TOperationId& id);
template <>
TFuture<TOperationsList<NExport::TExportToYtResponse>> TOperationClient::List(ui64 pageSize, const TString& pageToken) {
    // TODO: export -> export/yt
    return List<NExport::TExportToYtResponse>("export", pageSize, pageToken);
}

template TFuture<NExport::TExportToS3Response> TOperationClient::Get(const TOperation::TOperationId& id);
template <>
TFuture<TOperationsList<NExport::TExportToS3Response>> TOperationClient::List(ui64 pageSize, const TString& pageToken) {
    return List<NExport::TExportToS3Response>("export/s3", pageSize, pageToken);
}

template TFuture<NImport::TImportFromS3Response> TOperationClient::Get(const TOperation::TOperationId& id);
template <>
TFuture<TOperationsList<NImport::TImportFromS3Response>> TOperationClient::List(ui64 pageSize, const TString& pageToken) {
    return List<NImport::TImportFromS3Response>("import/s3", pageSize, pageToken);
}

template TFuture<NTable::TBuildIndexOperation> TOperationClient::Get(const TOperation::TOperationId& id);
template <>
TFuture<TOperationsList<NTable::TBuildIndexOperation>> TOperationClient::List(ui64 pageSize, const TString& pageToken) {
    return List<NTable::TBuildIndexOperation>("buildindex", pageSize, pageToken);
}

template TFuture<NQuery::TScriptExecutionOperation> TOperationClient::Get(const TOperation::TOperationId& id);
template <>
TFuture<TOperationsList<NQuery::TScriptExecutionOperation>> TOperationClient::List(ui64 pageSize, const TString& pageToken) {
    return List<NQuery::TScriptExecutionOperation>("scriptexec", pageSize, pageToken);
}

} // namespace NOperation
} // namespace NYdb
