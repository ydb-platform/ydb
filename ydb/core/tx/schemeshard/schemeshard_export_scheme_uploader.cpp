#include "schemeshard.h"
#include "schemeshard_export_scheme_uploader.h"

#include <ydb/core/tx/datashard/export_common.h>
#include <ydb/core/tx/schemeshard/schemeshard_export_helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/s3_storage_config.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/lib/ydb_cli/dump/util/view_utils.h>

namespace NKikimr::NSchemeShard {

class TSchemeUploader: public TActorBootstrapped<TSchemeUploader> {

    using TS3ExternalStorageConfig = NWrappers::NExternalStorage::TS3ExternalStorageConfig;
    using TEvExternalStorage = NWrappers::TEvExternalStorage;
    using TPutObjectResult = Aws::Utils::Outcome<Aws::S3::Model::PutObjectResult, Aws::S3::S3Error>;

    void GetDescription() {
        Send(SchemeShard, new TEvSchemeShard::TEvDescribeScheme(SourcePathId));
        Become(&TThis::StateDescribe);
    }

    static TString BuildViewScheme(const TString& path, const NKikimrSchemeOp::TViewDescription& viewDescription, const TString& backupRoot, TString& error) {
        NYql::TIssues issues;
        auto scheme = NYdb::NDump::BuildCreateViewQuery(viewDescription.GetName(), path, viewDescription.GetQueryText(), backupRoot, issues);
        if (!scheme) {
            error = issues.ToString();
        }
        return scheme;
    }

    bool BuildSchemeToUpload(const NKikimrScheme::TEvDescribeSchemeResult& describeResult, TString& error) {
        const auto pathType = describeResult.GetPathDescription().GetSelf().GetPathType();
        switch (pathType) {
            case NKikimrSchemeOp::EPathTypeView: {
                Scheme = BuildViewScheme(describeResult.GetPath(), describeResult.GetPathDescription().GetViewDescription(), DatabaseRoot, error);
                return !Scheme.empty();
            }
            default:
                error = TStringBuilder() << "unsupported path type: " << pathType;
                return false;
        }
    }

    void HandleSchemeDescription(TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        const auto& describeResult = ev->Get()->GetRecord();

        LOG_D("HandleSchemeDescription"
            << ", self: " << SelfId()
            << ", status: " << describeResult.GetStatus()
        );

        if (describeResult.GetStatus() != TEvSchemeShard::EStatus::StatusSuccess) {
            return Finish(false, describeResult.GetReason());
        }

        TString error;
        if (!BuildSchemeToUpload(describeResult, error)) {
            return Finish(false, error);
        }

        if (auto permissions = NDataShard::GenYdbPermissions(describeResult.GetPathDescription())) {
            google::protobuf::TextFormat::PrintToString(permissions.GetRef(), &Permissions);
        } else {
            return Finish(false, "cannot infer permissions");
        }

        UploadScheme();
    }

    void UploadScheme() {
        Y_ABORT_UNLESS(!SchemeUploaded);

        if (!Scheme) {
            return Finish(false, "cannot infer scheme");
        }
        if (Attempt == 0) {
            StorageOperator = RegisterWithSameMailbox(
                NWrappers::CreateS3Wrapper(ExternalStorageConfig->ConstructStorageOperator())
            );
        }

        auto request = Aws::S3::Model::PutObjectRequest()
            .WithKey(Sprintf("%s/create_view.sql", DestinationPrefix->c_str()));

        Send(StorageOperator, new TEvExternalStorage::TEvPutObjectRequest(request, TString(Scheme)));
        Become(&TThis::StateUploadScheme);
    }

    void HandleSchemePutResponse(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandleSchemePutResponse"
            << ", self: " << SelfId()
            << ", result: " << result
        );

        if (!CheckResult(result, TStringBuf("PutObject (scheme)"))) {
            return;
        }
        SchemeUploaded = true;
        UploadPermissions();
    }

    void UploadPermissions() {
        Y_ABORT_UNLESS(!PermissionsUploaded);

        if (!Permissions) {
            return Finish(false, "cannot infer permissions");
        }
        auto request = Aws::S3::Model::PutObjectRequest()
            .WithKey(Sprintf("%s/permissions.pb", DestinationPrefix->c_str()));

        Send(StorageOperator, new TEvExternalStorage::TEvPutObjectRequest(request, TString(Permissions)));
        Become(&TThis::StateUploadPermissions);
    }

    void HandlePermissionsPutResponse(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandlePermissionsPutResponse"
            << ", self: " << SelfId()
            << ", result: " << result
        );

        if (!CheckResult(result, TStringBuf("PutObject (permissions)"))) {
            return;
        }
        PermissionsUploaded = true;
        UploadMetadata();
    }

    void UploadMetadata() {
        Y_ABORT_UNLESS(!MetadataUploaded);

        if (!Metadata) {
            return Finish(false, "empty metadata");
        }
        auto request = Aws::S3::Model::PutObjectRequest()
            .WithKey(Sprintf("%s/metadata.json", DestinationPrefix->c_str()));

        Send(StorageOperator, new TEvExternalStorage::TEvPutObjectRequest(request, TString(Metadata)));
        Become(&TThis::StateUploadMetadata);
    }

    void HandleMetadataPutResponse(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandleMetadataPutResponse"
            << ", self: " << SelfId()
            << ", result: " << result
        );

        if (!CheckResult(result, TStringBuf("PutObject (metadata)"))) {
            return;
        }
        MetadataUploaded = true;
        Finish();
    }

    bool CheckResult(const TPutObjectResult& result, const TStringBuf marker) {
        if (result.IsSuccess()) {
            return true;
        }

        LOG_E("Error at '" << marker << "'"
            << ", self: " << SelfId()
            << ", error: " << result
        );

        RetryOrFinish(result.GetError());
        return false;
    }

    void RetryOrFinish(const Aws::S3::S3Error& error) {
        if (Attempt < Retries && ShouldRetry(error)) {
            Retry();
        } else {
            Finish(false, TStringBuilder() << "S3 error: " << error.GetMessage());
        }
    }

    static bool ShouldRetry(const Aws::S3::S3Error& error) {
        if (error.ShouldRetry()) {
            return true;
        }
        return error.GetExceptionName() == "TooManyRequests";
    }

    void Retry() {
        Delay = Min(Delay * ++Attempt, MaxDelay);
        const TDuration random = TDuration::FromValue(TAppData::RandomProvider->GenRand64() % Delay.MicroSeconds());
        Schedule(Delay + random, new TEvents::TEvWakeup());
    }

    void Finish(bool success = true, const TString& error = TString()) {
        LOG_I("Finish"
            << ", self: " << SelfId()
            << ", success: " << success
            << ", error: " << error
        );

        Send(SchemeShard, new TEvPrivate::TEvExportSchemeUploadResult(ExportId, ItemIdx, success, error));
        PassAway();
    }

    void PassAway() override {
        Send(StorageOperator, new TEvents::TEvPoisonPill());
        IActor::PassAway();
    }

public:

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::EXPORT_S3_UPLOADER_ACTOR;
    }

    TSchemeUploader(
        TActorId schemeShard,
        ui64 exportId,
        ui32 itemIdx,
        TPathId sourcePathId,
        const Ydb::Export::ExportToS3Settings& settings,
        const TString& databaseRoot,
        const TString& metadata
    )
        : SchemeShard(schemeShard)
        , ExportId(exportId)
        , ItemIdx(itemIdx)
        , SourcePathId(sourcePathId)
        , ExternalStorageConfig(new TS3ExternalStorageConfig(settings))
        , Retries(settings.number_of_retries())
        , DatabaseRoot(databaseRoot)
        , Metadata(metadata)
    {
        if (itemIdx < ui32(settings.items_size())) {
            DestinationPrefix = settings.items(itemIdx).destination_prefix();
        }
    }

    void Bootstrap() {
        if (!DestinationPrefix) {
            return Finish(false, TStringBuilder() << "cannot determine destination prefix, item index: " << ItemIdx << " out of range");
        }
        if (!Scheme || !Permissions) {
            return GetDescription();
        }
        if (!SchemeUploaded) {
            return UploadScheme();
        }
        if (!PermissionsUploaded) {
            return UploadPermissions();
        }
        if (!MetadataUploaded) {
            return UploadMetadata();
        }
        Finish();
    }

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvWakeup, Bootstrap);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    STATEFN(StateDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvDescribeSchemeResult, HandleSchemeDescription);
        default:
            return StateBase(ev);
        }
    }

    STATEFN(StateUploadScheme) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvPutObjectResponse, HandleSchemePutResponse);
        default:
            return StateBase(ev);
        }
    }

    STATEFN(StateUploadPermissions) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvPutObjectResponse, HandlePermissionsPutResponse);
        default:
            return StateBase(ev);
        }
    }

    STATEFN(StateUploadMetadata) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvPutObjectResponse, HandleMetadataPutResponse);
        default:
            return StateBase(ev);
        }
    }

private:

    TActorId SchemeShard;

    ui64 ExportId;
    ui32 ItemIdx;
    TPathId SourcePathId;

    NWrappers::IExternalStorageConfig::TPtr ExternalStorageConfig;
    TMaybe<TString> DestinationPrefix;

    ui32 Attempt = 0;
    const ui32 Retries;

    TString DatabaseRoot;

    TActorId StorageOperator;

    TDuration Delay = TDuration::Minutes(1);
    static constexpr TDuration MaxDelay = TDuration::Minutes(10);

    TString Scheme;
    bool SchemeUploaded = false;

    TString Permissions;
    bool PermissionsUploaded = false;

    TString Metadata;
    bool MetadataUploaded = false;

}; // TSchemeUploader

IActor* CreateSchemeUploader(TActorId schemeShard, ui64 exportId, ui32 itemIdx, TPathId sourcePathId,
    const Ydb::Export::ExportToS3Settings& settings, const TString& databaseRoot, const TString& metadata
) {
    return new TSchemeUploader(schemeShard, exportId, itemIdx, sourcePathId, settings, databaseRoot, metadata);
}

} // NKikimr::NSchemeShard
