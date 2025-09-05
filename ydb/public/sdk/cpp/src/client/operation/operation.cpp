#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

/* Headers below used to instantiate concrete 'Get' & 'List' methods */
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/query.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/export/export.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h>
#include <ydb/public/sdk/cpp/src/client/ss_tasks/task.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include "impl.h"


namespace NYdb::inline Dev::NOperation {

TOperationClient::TOperationClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{
}

TAsyncStatus TOperationClient::Cancel(const TOperation::TOperationId& id) {
    auto request = MakeRequest<Ydb::Operations::CancelOperationRequest>();
    request.set_id(TStringType{id.ToString()});

    return Impl_->Cancel(std::move(request));
}

TAsyncStatus TOperationClient::Forget(const TOperation::TOperationId& id) {
    auto request = MakeRequest<Ydb::Operations::ForgetOperationRequest>();
    request.set_id(TStringType{id.ToString()});

    return Impl_->Forget(std::move(request));
}

// Instantiations
template NThreading::TFuture<NSchemeShard::TBackgroundProcessesResponse> TOperationClient::Get(const TOperation::TOperationId& id);
template <>
NThreading::TFuture<TOperationsList<NSchemeShard::TBackgroundProcessesResponse>> TOperationClient::List(std::uint64_t pageSize, const std::string& pageToken) {
    return List<NSchemeShard::TBackgroundProcessesResponse>("ss/backgrounds", pageSize, pageToken);
}

template NThreading::TFuture<NExport::TExportToYtResponse> TOperationClient::Get(const TOperation::TOperationId& id);
template <>
NThreading::TFuture<TOperationsList<NExport::TExportToYtResponse>> TOperationClient::List(std::uint64_t pageSize, const std::string& pageToken) {
    // TODO: export -> export/yt
    return List<NExport::TExportToYtResponse>("export", pageSize, pageToken);
}

template NThreading::TFuture<NExport::TExportToS3Response> TOperationClient::Get(const TOperation::TOperationId& id);
template <>
NThreading::TFuture<TOperationsList<NExport::TExportToS3Response>> TOperationClient::List(std::uint64_t pageSize, const std::string& pageToken) {
    return List<NExport::TExportToS3Response>("export/s3", pageSize, pageToken);
}

template NThreading::TFuture<NImport::TImportFromS3Response> TOperationClient::Get(const TOperation::TOperationId& id);
template <>
NThreading::TFuture<TOperationsList<NImport::TImportFromS3Response>> TOperationClient::List(std::uint64_t pageSize, const std::string& pageToken) {
    return List<NImport::TImportFromS3Response>("import/s3", pageSize, pageToken);
}

template NThreading::TFuture<NTable::TBuildIndexOperation> TOperationClient::Get(const TOperation::TOperationId& id);
template <>
NThreading::TFuture<TOperationsList<NTable::TBuildIndexOperation>> TOperationClient::List(std::uint64_t pageSize, const std::string& pageToken) {
    return List<NTable::TBuildIndexOperation>("buildindex", pageSize, pageToken);
}

template NThreading::TFuture<NQuery::TScriptExecutionOperation> TOperationClient::Get(const TOperation::TOperationId& id);
template <>
NThreading::TFuture<TOperationsList<NQuery::TScriptExecutionOperation>> TOperationClient::List(std::uint64_t pageSize, const std::string& pageToken) {
    return List<NQuery::TScriptExecutionOperation>("scriptexec", pageSize, pageToken);
}

} // namespace NYdb::NOperation
