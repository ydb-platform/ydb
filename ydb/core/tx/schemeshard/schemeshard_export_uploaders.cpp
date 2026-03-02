#include "schemeshard_export_uploaders.h"

#include "schemeshard.h"
#include "schemeshard_xxport__helpers.h"

#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/lib/ydb_cli/dump/files/files.h>

#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/backup/common/fields_wrappers.h>
#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/datashard/export_common.h>
#include <ydb/core/tx/schemeshard/schemeshard_export_helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/schemeshard/schemeshard_scheme_builders.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/retry_policy.h>
#include <ydb/core/wrappers/s3_storage_config.h>
#include <ydb/core/wrappers/s3_wrapper.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/json/json_writer.h>

namespace NKikimr::NSchemeShard {

template <class TDerived, class TSettings>
class TExportFilesUploader: public TActorBootstrapped<TDerived> {
protected:
    using TEvExternalStorage = NWrappers::TEvExternalStorage;
    using TPutObjectResult = Aws::Utils::Outcome<Aws::S3::Model::PutObjectResult, Aws::S3::S3Error>;

    struct TFileUpload {
        TString Path;
        TString Content;
        size_t Attempt = 0;
    };

protected:
    TExportFilesUploader(const TSettings& settings, const TString& destinationPrefix)
        : Settings(settings)
        , DestinationPrefix(destinationPrefix)
        , ExternalStorageConfig(NWrappers::IExternalStorageConfig::Construct(AppData()->AwsClientConfig, settings))
    {
        if (Settings.has_encryption_settings()) {
            Key = NBackup::TEncryptionKey(Settings.encryption_settings().symmetric_key().key());
        }
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::EXPORT_UPLOADER_ACTOR;
    }

    // Adds a file to queue.
    // filePath is relative to DestinationPrefix
    // iv if file is needed to be encrypted
    bool AddFile(
        TString filePath,
        TString content,
        const TMaybe<NBackup::TEncryptionIV>& iv = Nothing())
    {
        if (iv) {
            if (!Key) {
                Fail(TStringBuilder() << "Internal error: no encryption key");
                return false;
            }
            filePath += ".enc";
            try {
                TBuffer encContent = NBackup::TEncryptedFileSerializer::EncryptFullFile(
                    Settings.encryption_settings().encryption_algorithm(),
                    *Key, *iv,
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

    // Starting function for upload already added files
    void UploadFiles() {
        if (!StorageOperator) {
            StorageOperator = this->RegisterWithSameMailbox(
                NWrappers::CreateStorageWrapper(ExternalStorageConfig->ConstructStorageOperator())
            );
        }

        this->Become(&TExportFilesUploader::UploadStateFunc);

        ProcessQueue();
    }

    void ProcessQueue() {
        if (Files.empty()) {
            return Success();
        }

        const TFileUpload& upload = Files.front();

        TStringBuilder path;
        path << NBackup::NormalizeExportPrefix(DestinationPrefix) << '/' << upload.Path;

        auto request = Aws::S3::Model::PutObjectRequest().WithKey(path);

        this->Send(StorageOperator, new TEvExternalStorage::TEvPutObjectRequest(request, TString(upload.Content)));
    }

    void Handle(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;
        TFileUpload& upload = Files.front();

        LOG_D("Put file response " << upload.Path
            << ", self: " << this->SelfId()
            << ", result: " << result
        );

        if (!result.IsSuccess()) {
            return RetryOrFinish(result.GetError(), upload);
        }

        Files.pop_front();
        ProcessQueue();
    }

    void RetryOrFinish(const Aws::S3::S3Error& error, TFileUpload& upload) {
        if (upload.Attempt < Settings.number_of_retries() && NWrappers::ShouldRetry(error)) {
            Retry(upload);
        } else {
            Fail(TStringBuilder() << upload.Path << ". S3 error: " << error.GetMessage());
        }
    }

    void Retry(TFileUpload& upload) {
        Delay = Min(Delay * ++upload.Attempt, MaxDelay);
        const TDuration random = TDuration::FromValue(TAppData::RandomProvider->GenRand64() % Delay.MicroSeconds());
        this->Schedule(Delay + random, new TEvents::TEvWakeup());
    }

    STATEFN(UploadStateFunc) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvWakeup, ProcessQueue);
            sFunc(TEvents::TEvPoisonPill, PassAway);
            hFunc(TEvExternalStorage::TEvPutObjectResponse, Handle);
        }
    }

    void Success() {
        OnFilesUploaded(true, {});
    }

    void Fail(const TString& error) {
        OnFilesUploaded(false, error);
    }

    virtual void OnFilesUploaded(bool success, const TString& error) = 0;

    void PassAway() override {
        this->Send(StorageOperator, new TEvents::TEvPoisonPill());
        IActor::PassAway();
    }

private:
    TSettings Settings;
    TString DestinationPrefix;
    TMaybe<NBackup::TEncryptionKey> Key;
    NWrappers::IExternalStorageConfig::TPtr ExternalStorageConfig;
    TActorId StorageOperator;

    std::deque<TFileUpload> Files;

    TDuration Delay = TDuration::Minutes(1);
    static constexpr TDuration MaxDelay = TDuration::Minutes(10);
};

template <typename TSettings>
class TSchemeUploader: public TExportFilesUploader<TSchemeUploader<TSettings>, TSettings> {
    using TThis = TSchemeUploader;

    void GetDescription() {
        this->Send(SchemeShard, new TEvSchemeShard::TEvDescribeScheme(SourcePathId));
        this->Become(&TThis::StateDescribe);
    }

    bool FillExportProperties(const NKikimrScheme::TEvDescribeSchemeResult& describeResult, TString& error) {
        PathType = GetPathType(describeResult);

        auto properties = PathTypeToXxportProperties(PathType);
        if (!properties) {
            error = TStringBuilder() << "unable to find file properties for " << PathType;
            return false;
        }

        FileName = properties->FileName;
        SchemeFileType = properties->FileType;
        return true;
    }

    bool BuildSchemeToUpload(const NKikimrScheme::TEvDescribeSchemeResult& describeResult, TString& error) {
        if (!FillExportProperties(describeResult, error)) {
            return false;
        }

        return BuildScheme(describeResult, Scheme, DatabaseRoot, error);
    }

    void HandleSchemeDescription(TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        const auto& describeResult = ev->Get()->GetRecord();

        LOG_D("HandleSchemeDescription"
            << ", self: " << this->SelfId()
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

        StartUploadFiles();
    }

    void StartUploadFiles() {
        if (!Scheme) {
            return Finish(false, "cannot infer scheme");
        }

        if (!this->AddFile(FileName, Scheme, MakeIV(SchemeFileType))) {
            return;
        }

        if (EnableChecksums) {
            if (!this->AddFile(NBackup::ChecksumKey(FileName), NBackup::ComputeChecksum(Scheme))) {
                return;
            }
        }

        if (EnablePermissions) {
            if (!Permissions) {
                return Finish(false, "cannot infer permissions");
            }

            if (!this->AddFile("permissions.pb", Permissions, MakeIV(NBackup::EBackupFileType::Permissions))) {
                return;
            }

            if (EnableChecksums) {
                if (!this->AddFile(NBackup::ChecksumKey("permissions.pb"), NBackup::ComputeChecksum(Permissions))) {
                    return;
                }
            }
        }

        if (!Metadata) {
            return Finish(false, "empty metadata");
        }
        if (!this->AddFile("metadata.json", Metadata, IV)) {
            return;
        }

        if (EnableChecksums) {
            if (!this->AddFile(NBackup::ChecksumKey("metadata.json"), NBackup::ComputeChecksum(Metadata))) {
                return;
            }
        }

        this->UploadFiles();
    }

    TMaybe<NBackup::TEncryptionIV> MakeIV(NBackup::EBackupFileType fileType) {
        TMaybe<NBackup::TEncryptionIV> iv;
        if (IV) {
            iv = NBackup::TEncryptionIV::Combine(*IV, fileType, 0 /* backupItemNumber: already combined */, 0 /* shardNumber */);
        }
        return iv;
    }

    void Finish(bool success = true, const TString& error = TString()) {
        LOG_I("Finish"
            << ", self: " << this->SelfId()
            << ", success: " << success
            << ", error: " << error
        );

        this->Send(SchemeShard, new TEvPrivate::TEvExportSchemeUploadResult(ExportId, ItemIdx, success, error));
        this->PassAway();
    }

    void OnFilesUploaded(bool success, const TString& error) override {
        Finish(success, error);
    }

    static TString GetDestinationPrefix(const TSettings& settings, ui32 itemIdx) {
        if (itemIdx < ui32(settings.items_size())) {
            return NBackup::NFieldsWrappers::GetItemDestination(settings.items(itemIdx));
        }
        return NBackup::NFieldsWrappers::GetCommonDestination(settings);
    }

public:
    TSchemeUploader(
        TActorId schemeShard,
        ui64 exportId,
        ui32 itemIdx,
        TPathId sourcePathId,
        const TSettings& settings,
        const TString& databaseRoot,
        const TString& metadata,
        bool enablePermissions,
        bool enableChecksums,
        const TMaybe<NBackup::TEncryptionIV>& iv
    )
        : TExportFilesUploader<TSchemeUploader, TSettings>(settings, GetDestinationPrefix(settings, itemIdx))
        , SchemeShard(schemeShard)
        , ExportId(exportId)
        , ItemIdx(itemIdx)
        , SourcePathId(sourcePathId)
        , IV(iv)
        , DatabaseRoot(databaseRoot)
        , EnablePermissions(enablePermissions)
        , EnableChecksums(enableChecksums)
        , Metadata(metadata)
    {
    }

    void Bootstrap() {
        GetDescription();
    }

    STATEFN(StateDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvDescribeSchemeResult, HandleSchemeDescription);
            sFunc(TEvents::TEvPoisonPill, this->PassAway);
        }
    }

private:
    TActorId SchemeShard;

    ui64 ExportId;
    ui32 ItemIdx;
    TPathId SourcePathId;
    NKikimrSchemeOp::EPathType PathType;
    NBackup::EBackupFileType SchemeFileType;
    TString FileName;

    TMaybe<NBackup::TEncryptionIV> IV;

    TString DatabaseRoot;
    TString Scheme;
    const bool EnablePermissions;
    const bool EnableChecksums;
    TString Permissions;
    TString Metadata;
}; // TSchemeUploader

template <typename TSettings>
class TExportMetadataUploader: public TExportFilesUploader<TExportMetadataUploader<TSettings>, TSettings> {
public:
    TExportMetadataUploader(
        TActorId schemeShard,
        ui64 exportId,
        const TSettings& settings,
        const NKikimrSchemeOp::TExportMetadata& exportMetadata,
        bool enableChecksums
    )
        : TExportFilesUploader<TExportMetadataUploader, TSettings>(settings, NBackup::NFieldsWrappers::GetCommonDestination(settings))
        , SchemeShard(schemeShard)
        , ExportId(exportId)
        , EnableChecksums(enableChecksums)
        , ExportMetadata(exportMetadata)
    {
    }

    void Bootstrap() {
        if (ExportMetadata.HasIV()) {
            IV = NBackup::TEncryptionIV::FromBinaryString(ExportMetadata.GetIV());
        }

        if (!AddBackupMetadata() || !AddSchemaMappingMetadata() || !AddSchemaMappingJson()) {
            return;
        }

        this->UploadFiles();
    }

private:
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
        if (EnableChecksums) {
            writer.Write("checksum", "sha256");
        }
        writer.CloseMap();

        writer.Flush();
        ss.Flush();

        return this->AddFile("metadata.json", content)
            && (!EnableChecksums || this->AddFile(NBackup::ChecksumKey("metadata.json"), NBackup::ComputeChecksum(content)));
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

        return this->AddFile("SchemaMapping/metadata.json", content, IV)
            && (!EnableChecksums || this->AddFile(NBackup::ChecksumKey("SchemaMapping/metadata.json"), NBackup::ComputeChecksum(content)));
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

        const TString content = schemaMapping.Serialize();
        return this->AddFile("SchemaMapping/mapping.json", content, iv)
            && (!EnableChecksums || this->AddFile(NBackup::ChecksumKey("SchemaMapping/mapping.json"), NBackup::ComputeChecksum(content)));
    }

    void OnFilesUploaded(bool success, const TString& error) override {
        LOG_I("Finish uploading export metadata"
            << ", self: " << this->SelfId()
            << ", success: " << success
            << ", error: " << error
        );

        this->Send(SchemeShard, new TEvPrivate::TEvExportUploadMetadataResult(ExportId, success, error));
        this->PassAway();
    }

private:
    TActorId SchemeShard;
    ui64 ExportId;
    bool EnableChecksums = false;

    NKikimrSchemeOp::TExportMetadata ExportMetadata;

    TMaybe<NBackup::TEncryptionIV> IV;
};

template <typename TSettings>
IActor* CreateSchemeUploader(TActorId schemeShard, ui64 exportId, ui32 itemIdx, TPathId sourcePathId,
    const TSettings& settings, const TString& databaseRoot, const TString& metadata,
    bool enablePermissions, bool enableChecksums, const TMaybe<NBackup::TEncryptionIV>& iv
) {
    return new TSchemeUploader(schemeShard, exportId, itemIdx, sourcePathId, settings, databaseRoot,
        metadata, enablePermissions, enableChecksums, iv);
}

template <typename TSettings>
NActors::IActor* CreateExportMetadataUploader(NActors::TActorId schemeShard, ui64 exportId,
    const TSettings& settings, const NKikimrSchemeOp::TExportMetadata& exportMetadata,
    bool enableChecksums
) {
    return new TExportMetadataUploader(schemeShard, exportId, settings, exportMetadata, enableChecksums);
}

template IActor* CreateSchemeUploader<Ydb::Export::ExportToS3Settings>(
    TActorId schemeShard, ui64 exportId, ui32 itemIdx, TPathId sourcePathId,
    const Ydb::Export::ExportToS3Settings& settings, const TString& databaseRoot, const TString& metadata,
    bool enablePermissions, bool enableChecksums, const TMaybe<NBackup::TEncryptionIV>& iv
);

template IActor* CreateSchemeUploader<Ydb::Export::ExportToFsSettings>(
    TActorId schemeShard, ui64 exportId, ui32 itemIdx, TPathId sourcePathId,
    const Ydb::Export::ExportToFsSettings& settings, const TString& databaseRoot, const TString& metadata,
    bool enablePermissions, bool enableChecksums, const TMaybe<NBackup::TEncryptionIV>& iv
);

template NActors::IActor* CreateExportMetadataUploader<Ydb::Export::ExportToS3Settings>(
    NActors::TActorId schemeShard, ui64 exportId,
    const Ydb::Export::ExportToS3Settings& settings, const NKikimrSchemeOp::TExportMetadata& exportMetadata,
    bool enableChecksums
);

template NActors::IActor* CreateExportMetadataUploader<Ydb::Export::ExportToFsSettings>(
    NActors::TActorId schemeShard, ui64 exportId,
    const Ydb::Export::ExportToFsSettings& settings, const NKikimrSchemeOp::TExportMetadata& exportMetadata,
    bool enableChecksums
);

} // NKikimr::NSchemeShard
