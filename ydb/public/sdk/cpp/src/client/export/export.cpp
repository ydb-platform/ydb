#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/export/export.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_export_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <google/protobuf/repeated_field.h>
#include <google/protobuf/timestamp.pb.h>

#include <util/stream/str.h>

namespace NYdb::inline Dev {
namespace NExport {

using namespace NThreading;
using namespace Ydb::Export;

/// Common
namespace {

std::vector<TExportItemProgress> ItemsProgressFromProto(const google::protobuf::RepeatedPtrField<ExportItemProgress>& proto) {
    std::vector<TExportItemProgress> result(proto.size());

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

/// YT
TExportToYtResponse::TExportToYtResponse(TStatus&& status, Ydb::Operations::Operation&& operation)
    : TOperation(std::move(status), std::move(operation))
{
    ExportToYtMetadata metadata;
    GetProto().metadata().UnpackTo(&metadata);

    // settings
    Metadata_.Settings.Host(metadata.settings().host());
    Metadata_.Settings.Port(metadata.settings().port());
    Metadata_.Settings.Token(metadata.settings().token());

    for (const auto& item : metadata.settings().items()) {
        Metadata_.Settings.AppendItem({item.source_path(), item.destination_path()});
    }

    Metadata_.Settings.Description(metadata.settings().description());
    Metadata_.Settings.NumberOfRetries(metadata.settings().number_of_retries());
    Metadata_.Settings.UseTypeV3(metadata.settings().use_type_v3());

    // progress
    Metadata_.Progress = TProtoAccessor::FromProto(metadata.progress());
    Metadata_.ItemsProgress = ItemsProgressFromProto(metadata.items_progress());
}

const TExportToYtResponse::TMetadata& TExportToYtResponse::Metadata() const {
    return Metadata_;
}

/// S3
TExportToS3Response::TExportToS3Response(TStatus&& status, Ydb::Operations::Operation&& operation)
    : TOperation(std::move(status), std::move(operation))
{
    ExportToS3Metadata metadata;
    GetProto().metadata().UnpackTo(&metadata);

    // settings
    Metadata_.Settings.Endpoint(metadata.settings().endpoint());
    Metadata_.Settings.Scheme(TProtoAccessor::FromProto<ExportToS3Settings>(metadata.settings().scheme()));
    Metadata_.Settings.StorageClass(TProtoAccessor::FromProto(metadata.settings().storage_class()));
    Metadata_.Settings.Bucket(metadata.settings().bucket());
    Metadata_.Settings.AccessKey(metadata.settings().access_key());
    Metadata_.Settings.SecretKey(metadata.settings().secret_key());

    for (const auto& item : metadata.settings().items()) {
        Metadata_.Settings.AppendItem({item.source_path(), item.destination_prefix()});
    }

    Metadata_.Settings.Description(metadata.settings().description());
    Metadata_.Settings.NumberOfRetries(metadata.settings().number_of_retries());

    if (!metadata.settings().compression().empty()) {
        Metadata_.Settings.Compression(metadata.settings().compression());
    }

    // progress
    Metadata_.Progress = TProtoAccessor::FromProto(metadata.progress());
    Metadata_.ItemsProgress = ItemsProgressFromProto(metadata.items_progress());
}

const TExportToS3Response::TMetadata& TExportToS3Response::Metadata() const {
    return Metadata_;
}

////////////////////////////////////////////////////////////////////////////////

class TExportClient::TImpl : public TClientImplCommon<TExportClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {
    }

    TFuture<TExportToYtResponse> ExportToYt(ExportToYtRequest&& request,
        const TExportToYtSettings& settings)
    {
        return RunOperation<V1::ExportService, ExportToYtRequest, ExportToYtResponse, TExportToYtResponse>(
            std::move(request),
            &V1::ExportService::Stub::AsyncExportToYt,
            TRpcRequestSettings::Make(settings));
    }

    TFuture<TExportToS3Response> ExportToS3(ExportToS3Request&& request,
        const TExportToS3Settings& settings)
    {
        return RunOperation<V1::ExportService, ExportToS3Request, ExportToS3Response, TExportToS3Response>(
            std::move(request),
            &V1::ExportService::Stub::AsyncExportToS3,
            TRpcRequestSettings::Make(settings));
    }

};

////////////////////////////////////////////////////////////////////////////////

TExportClient::TExportClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{
}

TFuture<TExportToYtResponse> TExportClient::ExportToYt(const TExportToYtSettings& settings) {
    auto request = MakeOperationRequest<ExportToYtRequest>(settings);

    request.mutable_settings()->set_host(TStringType{settings.Host_});
    request.mutable_settings()->set_port(settings.Port_.value_or(80));
    request.mutable_settings()->set_token(TStringType{settings.Token_});

    for (const auto& item : settings.Item_) {
        auto& protoItem = *request.mutable_settings()->mutable_items()->Add();
        protoItem.set_source_path(TStringType{item.Src});
        protoItem.set_destination_path(TStringType{item.Dst});
    }

    if (settings.Description_) {
        request.mutable_settings()->set_description(TStringType{settings.Description_.value()});
    }

    if (settings.NumberOfRetries_) {
        request.mutable_settings()->set_number_of_retries(settings.NumberOfRetries_.value());
    }

    request.mutable_settings()->set_use_type_v3(settings.UseTypeV3_);

    return Impl_->ExportToYt(std::move(request), settings);
}

TFuture<TExportToS3Response> TExportClient::ExportToS3(const TExportToS3Settings& settings) {
    auto request = MakeOperationRequest<ExportToS3Request>(settings);

    request.mutable_settings()->set_endpoint(TStringType{settings.Endpoint_});
    request.mutable_settings()->set_scheme(TProtoAccessor::GetProto<ExportToS3Settings>(settings.Scheme_));
    request.mutable_settings()->set_storage_class(TProtoAccessor::GetProto(settings.StorageClass_));
    request.mutable_settings()->set_bucket(TStringType{settings.Bucket_});
    request.mutable_settings()->set_access_key(TStringType{settings.AccessKey_});
    request.mutable_settings()->set_secret_key(TStringType{settings.SecretKey_});

    for (const auto& item : settings.Item_) {
        auto& protoItem = *request.mutable_settings()->mutable_items()->Add();
        protoItem.set_source_path(TStringType{item.Src});
        protoItem.set_destination_prefix(TStringType{item.Dst});
    }

    if (settings.Description_) {
        request.mutable_settings()->set_description(TStringType{settings.Description_.value()});
    }

    if (settings.NumberOfRetries_) {
        request.mutable_settings()->set_number_of_retries(settings.NumberOfRetries_.value());
    }

    if (settings.Compression_) {
        request.mutable_settings()->set_compression(TStringType{settings.Compression_.value()});
    }

    request.mutable_settings()->set_disable_virtual_addressing(!settings.UseVirtualAddressing_);

    return Impl_->ExportToS3(std::move(request), settings);
}

} // namespace NExport
} // namespace NYdb
