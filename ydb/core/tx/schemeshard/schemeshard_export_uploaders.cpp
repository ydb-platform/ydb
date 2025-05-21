#include "schemeshard.h"
#include "schemeshard_export_uploaders.h"

#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
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

#include <library/cpp/json/json_writer.h>

namespace NKikimr::NSchemeShard {

namespace {

bool ShouldRetry(const Aws::S3::S3Error& error) {
    if (error.ShouldRetry()) {
        return true;
    }
    return error.GetExceptionName() == "TooManyRequests";
}

} // anonymous

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
        if (EnablePermissions) {
            UploadPermissions();
        } else {
            UploadMetadata();
        }
    }

    void UploadPermissions() {
        Y_ABORT_UNLESS(EnablePermissions && !PermissionsUploaded);

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
        const TString& metadata,
        bool enablePermissions
    )
        : SchemeShard(schemeShard)
        , ExportId(exportId)
        , ItemIdx(itemIdx)
        , SourcePathId(sourcePathId)
        , ExternalStorageConfig(new TS3ExternalStorageConfig(settings))
        , Retries(settings.number_of_retries())
        , DatabaseRoot(databaseRoot)
        , EnablePermissions(enablePermissions)
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
        if (EnablePermissions && !PermissionsUploaded) {
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

    bool EnablePermissions = false;
    TString Permissions;
    bool PermissionsUploaded = false;

    TString Metadata;
    bool MetadataUploaded = false;

}; // TSchemeUploader

class TExportMetadataUploader: public TActorBootstrapped<TExportMetadataUploader> {
    using TS3ExternalStorageConfig = NWrappers::NExternalStorage::TS3ExternalStorageConfig;
    using TEvExternalStorage = NWrappers::TEvExternalStorage;
    using TPutObjectResult = Aws::Utils::Outcome<Aws::S3::Model::PutObjectResult, Aws::S3::S3Error>;

    struct TFileUpload {
        TString Path;
        TString Content;
        size_t Attempt = 0;
    };

public:
    TExportMetadataUploader(
        TActorId schemeShard,
        ui64 exportId,
        const Ydb::Export::ExportToS3Settings& settings,
        const NKikimrSchemeOp::TExportMetadata& exportMetadata
    )
        : SchemeShard(schemeShard)
        , ExportId(exportId)
        , Settings(settings)
        , ExportMetadata(exportMetadata)
        , ExternalStorageConfig(new TS3ExternalStorageConfig(Settings))
    {
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::EXPORT_S3_UPLOADER_ACTOR;
    }

    void Bootstrap() {
        Become(&TExportMetadataUploader::StateFunc);

        StorageOperator = RegisterWithSameMailbox(
            NWrappers::CreateS3Wrapper(ExternalStorageConfig->ConstructStorageOperator())
        );

        if (ExportMetadata.HasIV()) {
            IV = NBackup::TEncryptionIV::FromBinaryString(ExportMetadata.GetIV());
            Key = NBackup::TEncryptionKey(Settings.encryption_settings().symmetric_key().key());
        }

        if (!AddBackupMetadata() || !AddSchemaMappingMetadata() || !AddSchemaMappingJson()) {
            return;
        }

        ProcessQueue();
    }

private:
    bool AddFile(
        TString filePath,
        TString content,
        const TMaybe<NBackup::TEncryptionIV>& iv = Nothing(),
        const TMaybe<NBackup::TEncryptionKey>& key = Nothing())
    {
        if (iv && key) {
            filePath += ".enc";
            try {
                TBuffer encContent = NBackup::TEncryptedFileSerializer::EncryptFullFile(
                    ExportMetadata.GetEncryptionAlgorithm(),
                    *key, *iv,
                    content);
                content.assign(encContent.Data(), encContent.Size());
            } catch (const std::exception& ex) {
                Fail(TStringBuilder() << "Failed to encrypt " << filePath << ": " << ex.what());
                return false;
            }
        }
        Files.emplace_back(TFileUpload{
            .Path = filePath,
            .Content = content
        });
        return true;
    }

    bool AddBackupMetadata() {
        TString content;
        TStringOutput ss(content);
        NJson::TJsonWriter writer(&ss, false);

        writer.OpenMap();
        writer.Write("kind", "SimpleExportV0");
        if (const TString& compression = ExportMetadata.GetCompressionAlgorithm()) {
            writer.Write("compression", compression);
        }
        if (const TString& encryption = ExportMetadata.GetEncryptionAlgorithm()) {
            writer.Write("encryption", encryption);
        }
        writer.CloseMap();

        writer.Flush();
        ss.Flush();

        return AddFile("metadata.json", content);
    }

    bool AddSchemaMappingMetadata() {
        TString content;
        TStringOutput ss(content);
        NJson::TJsonWriter writer(&ss, false);

        writer.OpenMap();
        writer.Write("kind", "SchemaMappingV0");
        writer.CloseMap();

        writer.Flush();
        ss.Flush();

        return AddFile("SchemaMapping/metadata.json", content, IV, Key);
    }

    bool AddSchemaMappingJson() {
        NBackup::TSchemaMapping schemaMapping;
        for (const auto& item : ExportMetadata.GetSchemaMapping()) {
            schemaMapping.Items.emplace_back(NBackup::TSchemaMapping::TItem{
                .ExportPrefix = item.GetDestinationPrefix(),
                .ObjectPath = item.GetSourcePath(),
                .IV = item.HasIV() ? TMaybe<NBackup::TEncryptionIV>(NBackup::TEncryptionIV::FromBinaryString(item.GetIV())) : Nothing()
            });
        }

        TMaybe<NBackup::TEncryptionIV> iv;
        if (IV) {
            iv = NBackup::TEncryptionIV::Combine(*IV, NBackup::EBackupFileType::SchemaMapping, 0, 0);
        }

        return AddFile("SchemaMapping/mapping.json", schemaMapping.Serialize(), iv, Key);
    }

    void ProcessQueue() {
        if (Files.empty()) {
            return Success();
        }

        const TFileUpload& upload = Files.front();

        TStringBuilder path;
        path << Settings.destination_prefix();
        if (path.back() != '/') {
            path << '/';
        }
        path << upload.Path;

        auto request = Aws::S3::Model::PutObjectRequest().WithKey(path);

        Send(StorageOperator, new TEvExternalStorage::TEvPutObjectRequest(request, TString(upload.Content)));
    }

    void Handle(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;
        TFileUpload& upload = Files.front();

        LOG_D("Put file response " << upload.Path
            << ", self: " << SelfId()
            << ", result: " << result
        );

        if (!result.IsSuccess()) {
            return RetryOrFinish(result.GetError(), upload);
        }

        Files.pop_front();
        ProcessQueue();
    }

    void RetryOrFinish(const Aws::S3::S3Error& error, TFileUpload& upload) {
        if (upload.Attempt < Settings.number_of_retries() && ShouldRetry(error)) {
            Retry(upload);
        } else {
            Fail(TStringBuilder() << upload.Path << ". S3 error: " << error.GetMessage());
        }
    }

    void Retry(TFileUpload& upload) {
        Delay = Min(Delay * ++upload.Attempt, MaxDelay);
        const TDuration random = TDuration::FromValue(TAppData::RandomProvider->GenRand64() % Delay.MicroSeconds());
        Schedule(Delay + random, new TEvents::TEvWakeup());
    }

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvWakeup, ProcessQueue);
            sFunc(TEvents::TEvPoisonPill, PassAway);
            hFunc(TEvExternalStorage::TEvPutObjectResponse, Handle);
        }
    }

    void Success() {
        Finish(true, {});
    }

    void Fail(const TString& error) {
        Finish(false, error);
    }

    void Finish(bool success, const TString& error) {
        LOG_I("Finish uploading export metadata"
            << ", self: " << SelfId()
            << ", success: " << success
            << ", error: " << error
        );

        Send(SchemeShard, new TEvPrivate::TEvExportUploadMetadataResult(ExportId, success, error));
        PassAway();
    }

    void PassAway() override {
        Send(StorageOperator, new TEvents::TEvPoisonPill());
        IActor::PassAway();
    }

private:
    TActorId SchemeShard;
    ui64 ExportId;

    Ydb::Export::ExportToS3Settings Settings;
    NKikimrSchemeOp::TExportMetadata ExportMetadata;

    NWrappers::IExternalStorageConfig::TPtr ExternalStorageConfig;
    TActorId StorageOperator;
    TMaybe<NBackup::TEncryptionIV> IV;
    TMaybe<NBackup::TEncryptionKey> Key;

    std::deque<TFileUpload> Files;

    TDuration Delay = TDuration::Minutes(1);
    static constexpr TDuration MaxDelay = TDuration::Minutes(10);
};

IActor* CreateSchemeUploader(TActorId schemeShard, ui64 exportId, ui32 itemIdx, TPathId sourcePathId,
    const Ydb::Export::ExportToS3Settings& settings, const TString& databaseRoot, const TString& metadata,
    bool enablePermissions
) {
    return new TSchemeUploader(schemeShard, exportId, itemIdx, sourcePathId, settings, databaseRoot,
        metadata, enablePermissions);
}

NActors::IActor* CreateExportMetadataUploader(NActors::TActorId schemeShard, ui64 exportId,
    const Ydb::Export::ExportToS3Settings& settings, const NKikimrSchemeOp::TExportMetadata& exportMetadata
) {
    return new TExportMetadataUploader(schemeShard, exportId, settings, exportMetadata);
}

} // NKikimr::NSchemeShard
