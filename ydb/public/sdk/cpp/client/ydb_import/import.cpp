#include "import.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_import_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_import.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NYdb {
namespace NImport {

using namespace NThreading;
using namespace Ydb::Import;

/// Common
namespace {

TVector<TImportItemProgress> ItemsProgressFromProto(const google::protobuf::RepeatedPtrField<Ydb::Import::ImportItemProgress>& proto) {
    TVector<TImportItemProgress> result(Reserve(proto.size()));

    for (const auto& protoItem : proto) {
        auto& item = result.emplace_back();
        item.PartsTotal = protoItem.parts_total();
        item.PartsCompleted = protoItem.parts_completed();
        item.StartTime = ProtoTimestampToInstant(protoItem.start_time());
        item.EndTime = ProtoTimestampToInstant(protoItem.end_time());
    }

    return result;
}

} // anonymous

/// S3
TImportFromS3Response::TImportFromS3Response(TStatus&& status, Ydb::Operations::Operation&& operation)
    : TOperation(std::move(status), std::move(operation))
{
    ImportFromS3Metadata metadata;
    GetProto().metadata().UnpackTo(&metadata);

    // settings
    Metadata_.Settings.Endpoint(metadata.settings().endpoint());
    Metadata_.Settings.Scheme(TProtoAccessor::FromProto<ImportFromS3Settings>(metadata.settings().scheme()));
    Metadata_.Settings.Bucket(metadata.settings().bucket());
    Metadata_.Settings.AccessKey(metadata.settings().access_key());
    Metadata_.Settings.SecretKey(metadata.settings().secret_key());

    for (const auto& item : metadata.settings().items()) {
        Metadata_.Settings.AppendItem({item.source_prefix(), item.destination_path()});
    }

    Metadata_.Settings.Description(metadata.settings().description());
    Metadata_.Settings.NumberOfRetries(metadata.settings().number_of_retries());

    // progress
    Metadata_.Progress = TProtoAccessor::FromProto(metadata.progress());
    Metadata_.ItemsProgress = ItemsProgressFromProto(metadata.items_progress());
}

const TImportFromS3Response::TMetadata& TImportFromS3Response::Metadata() const {
    return Metadata_;
}

/// Data
TImportDataResult::TImportDataResult(TStatus&& status)
    : TStatus(std::move(status))
{}

////////////////////////////////////////////////////////////////////////////////

class TImportClient::TImpl : public TClientImplCommon<TImportClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {
    }

    TFuture<TImportFromS3Response> ImportFromS3(ImportFromS3Request&& request, const TImportFromS3Settings& settings) {
        return RunOperation<V1::ImportService, ImportFromS3Request, ImportFromS3Response, TImportFromS3Response>(
            std::move(request),
            &V1::ImportService::Stub::AsyncImportFromS3,
            TRpcRequestSettings::Make(settings));
    }

    template <typename TSettings>
    TAsyncImportDataResult ImportData(ImportDataRequest&& request, const TSettings& settings) {
        auto promise = NThreading::NewPromise<TImportDataResult>();

        auto extractor = [promise]
            (google::protobuf::Any*, TPlainStatus status) mutable {
                TImportDataResult result(TStatus(std::move(status)));
                promise.SetValue(std::move(result));
            };

        Connections_->RunDeferred<V1::ImportService, ImportDataRequest, ImportDataResponse>(
            std::move(request),
            extractor,
            &V1::ImportService::Stub::AsyncImportData,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    template <typename TData>
    TAsyncImportDataResult ImportData(const TString& table, TData&& data, const TImportYdbDumpDataSettings& settings) {
        auto request = MakeOperationRequest<ImportDataRequest>(settings);

        request.set_path(table);
        request.set_data(std::forward<TData>(data));

        for (const auto& column : settings.Columns_) {
            request.mutable_ydb_dump()->add_columns(column);
        }

        return ImportData(std::move(request), settings);
    }

};

////////////////////////////////////////////////////////////////////////////////

TImportClient::TImportClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{
}

TFuture<TImportFromS3Response> TImportClient::ImportFromS3(const TImportFromS3Settings& settings) {
    auto request = MakeOperationRequest<ImportFromS3Request>(settings);

    request.mutable_settings()->set_endpoint(settings.Endpoint_);
    request.mutable_settings()->set_scheme(TProtoAccessor::GetProto<ImportFromS3Settings>(settings.Scheme_));
    request.mutable_settings()->set_bucket(settings.Bucket_);
    request.mutable_settings()->set_access_key(settings.AccessKey_);
    request.mutable_settings()->set_secret_key(settings.SecretKey_);

    for (const auto& item : settings.Item_) {
        auto& protoItem = *request.mutable_settings()->mutable_items()->Add();
        protoItem.set_source_prefix(item.Src);
        protoItem.set_destination_path(item.Dst);
    }

    if (settings.Description_) {
        request.mutable_settings()->set_description(settings.Description_.GetRef());
    }

    if (settings.NumberOfRetries_) {
        request.mutable_settings()->set_number_of_retries(settings.NumberOfRetries_.GetRef());
    }

    request.mutable_settings()->set_disable_virtual_addressing(!settings.UseVirtualAddressing_);

    return Impl_->ImportFromS3(std::move(request), settings);
}

TAsyncImportDataResult TImportClient::ImportData(const TString& table, TString&& data, const TImportYdbDumpDataSettings& settings) {
    return Impl_->ImportData(table, std::move(data), settings);
}

TAsyncImportDataResult TImportClient::ImportData(const TString& table, const TString& data, const TImportYdbDumpDataSettings& settings) {
    return Impl_->ImportData(table, data, settings);
}

} // namespace NImport
} // namespace NYdb
