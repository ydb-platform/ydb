#include "schemeshard_export_uploaders.h"

#include "schemeshard.h"
#include "schemeshard_xxport__helpers.h"

#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/lib/ydb_cli/dump/files/files.h>

#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/kesus/tablet/events.h>
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

namespace {

TString GetDestinationPrefix(const Ydb::Export::ExportToS3Settings& settings, ui32 itemIdx) {
    if (itemIdx < ui32(settings.items_size())) {
        return settings.items(itemIdx).destination_prefix();
    }
    return settings.destination_prefix();
}

TMaybe<NBackup::TEncryptionIV> MakeIV(
    const TMaybe<NBackup::TEncryptionIV>& IV,
    NBackup::EBackupFileType fileType)
{
    TMaybe<NBackup::TEncryptionIV> iv;
    if (IV) {
        iv = NBackup::TEncryptionIV::Combine(*IV, fileType, 0 /* backupItemNumber: already combined */, 0 /* shardNumber */);
    }
    return iv;
}

}

template <class TDerived>
class TExportFilesUploader: public TActorBootstrapped<TDerived> {
protected:
    using TS3ExternalStorageConfig = NWrappers::NExternalStorage::TS3ExternalStorageConfig;
    using TEvExternalStorage = NWrappers::TEvExternalStorage;
    using TPutObjectResult = Aws::Utils::Outcome<Aws::S3::Model::PutObjectResult, Aws::S3::S3Error>;

    struct TFileUpload {
        TString Path;
        TString Content;
        size_t Attempt = 0;
    };

protected:
    TExportFilesUploader(const Ydb::Export::ExportToS3Settings& settings, const TString& destinationPrefix)
        : Settings(settings)
        , DestinationPrefix(destinationPrefix)
        , ExternalStorageConfig(new TS3ExternalStorageConfig(
            AppData()->AwsClientConfig,
            Settings))
    {
        if (Settings.has_encryption_settings()) {
            Key = NBackup::TEncryptionKey(Settings.encryption_settings().symmetric_key().key());
        }
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::EXPORT_S3_UPLOADER_ACTOR;
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
                NWrappers::CreateS3Wrapper(ExternalStorageConfig->ConstructStorageOperator())
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
        ScheduleRetry(Delay);
    }

    void ScheduleRetry(TDuration delay) {
        const TDuration random = TDuration::FromValue(TAppData::RandomProvider->GenRand64() % delay.MicroSeconds());
        this->Schedule(delay + random, new TEvents::TEvWakeup());
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

    const Ydb::Export::ExportToS3Settings& GetSettings() {
        return Settings;
    }

private:
    Ydb::Export::ExportToS3Settings Settings;
    TString DestinationPrefix;
    TMaybe<NBackup::TEncryptionKey> Key;
    NWrappers::IExternalStorageConfig::TPtr ExternalStorageConfig;
    TActorId StorageOperator;

    std::deque<TFileUpload> Files;

    TDuration Delay = TDuration::Minutes(1);
    static constexpr TDuration MaxDelay = TDuration::Minutes(10);
};

class TKesusResourcesUploader : public TExportFilesUploader<TKesusResourcesUploader> {
    void CreatePipe() {
        NTabletPipe::TClientConfig cfg;
        cfg.RetryPolicy = {
            .RetryLimitCount = 3u
        };
        KesusPipeClient = Register(NTabletPipe::CreateClient(this->SelfId(), KesusTabletId, cfg));
    }

    void ClosePipe() {
        if (KesusPipeClient) {
            NTabletPipe::CloseClient(this->SelfId(), KesusPipeClient);
            KesusPipeClient = {};
        }
    }

    void GetAllResources() {
        using namespace NKesus;
        if (!KesusPipeClient) {
            CreatePipe();
        }
        THolder<TEvKesus::TEvDescribeQuoterResources> req = MakeHolder<TEvKesus::TEvDescribeQuoterResources>();
        req->Record.SetRecursive(true);

        NTabletPipe::SendData(SelfId(), KesusPipeClient, req.Release());
        Become(&TThis::StateDescribeResources);
    }

    void HandleConnected(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            Finish(false, "failed to connect to kesus");
        }
    }

    void HandleDestroyed(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        Finish(false, "connection to kesus was lost");
    }

    void HandleResourcesDescription(NKesus::TEvKesus::TEvDescribeQuoterResourcesResult::TPtr ev) {
        auto& record = ev->Get()->Record;
        LOG_D("HandleResourcesDescription"
            << ", self: " << this->SelfId()
            << ", status: " << record.GetError().GetStatus());

        if (record.GetError().GetStatus() != Ydb::StatusIds::SUCCESS) {
            return Finish(false, "cannot get rate limiter resources describe");
        }

        Resources = std::move(*record.MutableResources());
        ResourcesKeys.reserve(Resources.size());

        ClosePipe();
        UploadBatch();
    }

    bool AddFiles(const TString& fileName, const TString& content) {
        if (!AddFile(fileName, content, MakeIV(IV, NBackup::EBackupFileType::CoordinationNodeCreateRateLimiter))) {
            return false;
        }

        return !EnableChecksums
            || AddFile(NBackup::ChecksumKey(fileName), NBackup::ComputeChecksum(content));
    }

    void UploadBatch() {
        LOG_D("UploadBatch"
            << ", self: " << this->SelfId()
            << ", ExportId: " << ExportId
            << ", ItemIdx: " << ItemIdx
            << ", Offset: " << Offset);

        i32 batchEnd = std::min(Offset + BatchSize, Resources.size());
        for (i32 resourceIdx = Offset; resourceIdx < batchEnd; ++resourceIdx) {
            auto& resource = Resources[resourceIdx];
            TString scheme;
            BuildRateLimiterResourceScheme(resource, scheme);

            TStringBuilder fileName;
            fileName << resource.GetResourcePath() << '/' << NYdb::NDump::NFiles::CreateRateLimiter().FileName;
            ResourcesKeys.push_back(resource.GetResourcePath());

            if (!AddFiles(fileName, scheme)) {
                return;
            }
        }

        Offset += BatchSize;

        UploadFiles();
    }

    void OnFilesUploaded(bool success, const TString& error) override {
        if (!success || Offset >= Resources.size()) {
            return Finish(success, error);
        }
        UploadBatch();
    }

    void Finish(bool success = true, const TString& error = TString()) {
        LOG_I("Finish"
            << ", self: " << SelfId()
            << ", success: " << success
            << ", error: " << error
        );

        Send(ReplyTo, new TEvPrivate::TEvExportUploadKesusResourcesResult(ExportId, ItemIdx, success, error, ResourcesKeys));
        PassAway();
    }

    void PassAway() override {
        ClosePipe();
        TExportFilesUploader<TKesusResourcesUploader>::PassAway();
    }

public:
    TKesusResourcesUploader(
        ui64 kesusTabletId,
        TActorId replyTo,
        ui64 exportId,
        ui32 itemIdx,
        const Ydb::Export::ExportToS3Settings& settings,
        TMaybe<NBackup::TEncryptionIV> iv,
        const bool enableChecksums
    )
        : TExportFilesUploader<TKesusResourcesUploader>(settings, GetDestinationPrefix(settings, itemIdx))
        , KesusTabletId(kesusTabletId)
        , ReplyTo(replyTo)
        , ExportId(exportId)
        , ItemIdx(itemIdx)
        , IV(iv)
        , EnableChecksums(enableChecksums)
    {
    }

    void Bootstrap() {
        GetAllResources();
    }

    STATEFN(StateDescribeResources) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKesus::TEvKesus::TEvDescribeQuoterResourcesResult, HandleResourcesDescription);
            hFunc(TEvTabletPipe::TEvClientConnected, HandleConnected);
            hFunc(TEvTabletPipe::TEvClientDestroyed, HandleDestroyed);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

private:
    ui64 KesusTabletId = 0;
    TActorId KesusPipeClient;
    TActorId ReplyTo;

    ui64 ExportId;
    ui32 ItemIdx;

    TMaybe<NBackup::TEncryptionIV> IV;

    const bool EnableChecksums;

    i32 BatchSize = 100;
    i32 Offset = 0;

    google::protobuf::RepeatedPtrField<NKikimrKesus::TStreamingQuoterResource> Resources;
    TVector<TString> ResourcesKeys;
}; // TKesusResourcesUploader

IActor* CreateKesusResourcesUploader(
    ui64 kesusTabletId, TActorId replyTo,
    ui64 exportId, ui32 itemIdx,
    const Ydb::Export::ExportToS3Settings &settings,
    TMaybe<NBackup::TEncryptionIV> iv,
    const bool enableChecksums
) {
    return new TKesusResourcesUploader(kesusTabletId, replyTo, exportId, itemIdx, settings, iv, enableChecksums);
}

class TSchemeUploader: public TExportFilesUploader<TSchemeUploader> {
    void GetDescription() {
        Send(SchemeShard, new TEvSchemeShard::TEvDescribeScheme(SourcePathId));
        Become(&TThis::StateDescribe);
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

        if (describeResult.GetPathDescription().GetSelf().GetPathType() == NKikimrSchemeOp::EPathTypeKesus) {
            // Upload resources before to store them in metadata
            Y_ABORT_UNLESS(describeResult.GetPathDescription().HasKesus());
            StartUploadKesusResources(describeResult.GetPathDescription().GetKesus().GetKesusTabletId());
        } else {
            StartUploadFiles();
        }
    }

    void StartUploadKesusResources(ui64 kesusTabletId) {
        KesusResourcesUploader = Register(CreateKesusResourcesUploader(
            kesusTabletId,
            SelfId(),
            ExportId,
            ItemIdx,
            GetSettings(),
            IV,
            EnableChecksums
        ));
        Become(&TThis::StateUploadKesusResources);
    }

    void HandleResourcesUploaded(TEvPrivate::TEvExportUploadKesusResourcesResult::TPtr ev) {
        const auto& record = ev->Get();
        LOG_D("HandleResourcesUploaded"
            << ", self: " << SelfId()
            << ", success: " << record->Success
            << ", error: " << record->Error);

        if (!record->Success) {
            return RetryResourceUploadOrFail(record->Error);
        }

        // Fill metadata with rate limiter resources
        ui32 excryptedIdx = 0;
        auto metadata = NBackup::TMetadata::Deserialize(Metadata);
        for (const auto& rateLimiter : record->ResourcesKeys) {
            NBackup::TRateLimiterResourceMetadata resourceData;
            resourceData.Name = rateLimiter;
            if (IV) {
                std::stringstream prefix;
                prefix << std::setfill('0') << std::setw(3) << std::right << ++excryptedIdx;
                resourceData.ExportPrefix = prefix.str();
            } else {
                resourceData.ExportPrefix = rateLimiter;
            }
            metadata.AddRateLimiterResource(resourceData);
        }

        Metadata = metadata.Serialize();

        // Upload everything else
        StartUploadFiles();
    }

    void RetryResourceUploadOrFail(const TString& error) {
        LOG_D("RetryResourceUploadOrFail"
            << ", self: " << SelfId()
            << ", attempts " << ResourceUploadAttempts + 1
            << ", max attempts" << GetSettings().number_of_retries()
            << ", error " << error);

        if (++ResourceUploadAttempts >= MaxResourceUploadAttempts) {
            return Finish(false, error);
        }

        if (auto uploader = std::exchange(KesusResourcesUploader, {})) {
            Send(uploader, new TEvents::TEvPoisonPill());
        }

        ScheduleRetry(TDuration::Seconds(2));
    }

    void StartUploadFiles() {
        if (!Scheme) {
            return Finish(false, "cannot infer scheme");
        }

        if (!AddFile(FileName, Scheme, MakeIV(IV, SchemeFileType))) {
            return;
        }

        if (EnableChecksums) {
            if (!AddFile(NBackup::ChecksumKey(FileName), NBackup::ComputeChecksum(Scheme))) {
                return;
            }
        }

        if (EnablePermissions) {
            if (!Permissions) {
                return Finish(false, "cannot infer permissions");
            }

            if (!AddFile("permissions.pb", Permissions, MakeIV(IV, NBackup::EBackupFileType::Permissions))) {
                return;
            }

            if (EnableChecksums) {
                if (!AddFile(NBackup::ChecksumKey("permissions.pb"), NBackup::ComputeChecksum(Permissions))) {
                    return;
                }
            }
        }

        if (!Metadata) {
            return Finish(false, "empty metadata");
        }

        if (!AddFile("metadata.json", Metadata, IV)) {
            return;
        }

        if (EnableChecksums) {
            if (!AddFile(NBackup::ChecksumKey("metadata.json"), NBackup::ComputeChecksum(Metadata))) {
                return;
            }
        }

        UploadFiles();
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

    void OnFilesUploaded(bool success, const TString& error) override {
        Finish(success, error);
    }

    void PassAway() override {
        Send(KesusResourcesUploader, new TEvents::TEvPoisonPill());
        TExportFilesUploader<TSchemeUploader>::PassAway();
    }

public:
    TSchemeUploader(
        TActorId schemeShard,
        ui64 exportId,
        ui32 itemIdx,
        TPathId sourcePathId,
        const Ydb::Export::ExportToS3Settings& settings,
        const TString& databaseRoot,
        const TString& metadata,
        bool enablePermissions,
        bool enableChecksums,
        const TMaybe<NBackup::TEncryptionIV>& iv
    )
        : TExportFilesUploader<TSchemeUploader>(settings, GetDestinationPrefix(settings, itemIdx))
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
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    STATEFN(StateUploadKesusResources) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvExportUploadKesusResourcesResult, HandleResourcesUploaded);
            sFunc(TEvents::TEvWakeup, GetDescription)
            sFunc(TEvents::TEvPoisonPill, PassAway);
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

    TActorId KesusResourcesUploader;
    ui32 ResourceUploadAttempts = 0;
    ui32 MaxResourceUploadAttempts = 10;
}; // TSchemeUploader

class TExportMetadataUploader: public TExportFilesUploader<TExportMetadataUploader> {
public:
    TExportMetadataUploader(
        TActorId schemeShard,
        ui64 exportId,
        const Ydb::Export::ExportToS3Settings& settings,
        const NKikimrSchemeOp::TExportMetadata& exportMetadata,
        bool enableChecksums
    )
        : TExportFilesUploader<TExportMetadataUploader>(settings, settings.destination_prefix())
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

        UploadFiles();
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

        return AddFile("metadata.json", content)
            && (!EnableChecksums || AddFile(NBackup::ChecksumKey("metadata.json"), NBackup::ComputeChecksum(content)));
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

        return AddFile("SchemaMapping/metadata.json", content, IV)
            && (!EnableChecksums || AddFile(NBackup::ChecksumKey("SchemaMapping/metadata.json"), NBackup::ComputeChecksum(content)));
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
        return AddFile("SchemaMapping/mapping.json", content, iv)
            && (!EnableChecksums || AddFile(NBackup::ChecksumKey("SchemaMapping/mapping.json"), NBackup::ComputeChecksum(content)));
    }

    void OnFilesUploaded(bool success, const TString& error) override {
        LOG_I("Finish uploading export metadata"
            << ", self: " << this->SelfId()
            << ", success: " << success
            << ", error: " << error
        );

        Send(SchemeShard, new TEvPrivate::TEvExportUploadMetadataResult(ExportId, success, error));
        PassAway();
    }

private:
    TActorId SchemeShard;
    ui64 ExportId;
    bool EnableChecksums = false;

    NKikimrSchemeOp::TExportMetadata ExportMetadata;

    TMaybe<NBackup::TEncryptionIV> IV;
};

IActor* CreateSchemeUploader(TActorId schemeShard, ui64 exportId, ui32 itemIdx, TPathId sourcePathId,
    const Ydb::Export::ExportToS3Settings& settings, const TString& databaseRoot, const TString& metadata,
    bool enablePermissions, bool enableChecksums, const TMaybe<NBackup::TEncryptionIV>& iv
) {
    return new TSchemeUploader(schemeShard, exportId, itemIdx, sourcePathId, settings, databaseRoot,
        metadata, enablePermissions, enableChecksums, iv);
}

NActors::IActor* CreateExportMetadataUploader(NActors::TActorId schemeShard, ui64 exportId,
    const Ydb::Export::ExportToS3Settings& settings, const NKikimrSchemeOp::TExportMetadata& exportMetadata,
    bool enableChecksums
) {
    return new TExportMetadataUploader(schemeShard, exportId, settings, exportMetadata, enableChecksums);
}

} // NKikimr::NSchemeShard
