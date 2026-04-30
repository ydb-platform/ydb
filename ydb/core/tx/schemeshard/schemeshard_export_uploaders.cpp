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
#include <ydb/core/base/path.h>
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

#include <type_traits>

namespace NKikimr::NSchemeShard {

namespace {

template <typename TSettings>
TString GetDestinationPrefix(const TSettings& settings, ui32 itemIdx) {
    if (itemIdx < ui32(settings.items_size())) {
        const TString itemDest = NBackup::NFieldsWrappers::GetItemDestination(settings.items(itemIdx));
        if constexpr (std::is_same_v<TSettings, Ydb::Export::ExportToFsSettings>) {
            return CanonizePath(TStringBuilder() << settings.base_path() << "/" << itemDest);
        }
        return itemDest;
    }
    return NBackup::NFieldsWrappers::GetCommonDestination(settings);
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

    const TSettings& GetSettings() const {
        return Settings;
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

template <class TDerived, class TSettings>
class TSchemeWithPipeUploader : public TExportFilesUploader<TDerived, TSettings> {
    using TBase = TExportFilesUploader<TDerived, TSettings>;
protected:
    void CreatePipe() {
        if (PipeClient) {
            return; // Already open
        }
        NTabletPipe::TClientConfig cfg;
        cfg.RetryPolicy = {
            .RetryLimitCount = 3u
        };
        PipeClient = TBase::Register(NTabletPipe::CreateClient(this->SelfId(), PipeTabletId, cfg));
    }

    void ClosePipe() {
        if (PipeClient) {
            NTabletPipe::CloseClient(this->SelfId(), PipeClient);
            PipeClient = {};
        }
    }

    void HandleConnected(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            Finish(false, TStringBuilder() << "failed to connect to tablet " << ev->Get()->TabletId);
        }
    }

    void HandleDestroyed(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        Finish(false, TStringBuilder() << "connection lost to tablet " << ev->Get()->TabletId);
    }

    virtual void Finish(bool success = true, const TString& error = TString()) = 0;

    void PassAway() override {
        ClosePipe();
        TBase::PassAway();
    }

public:
    TSchemeWithPipeUploader(
        ui64 pipeTabletId,
        TActorId replyTo,
        ui32 itemIdx,
        const TSettings& settings
    )
        : TBase(settings, GetDestinationPrefix(settings, itemIdx))
        , PipeTabletId(pipeTabletId)
        , ReplyTo(replyTo)
    {
    }

protected:
    ui64 PipeTabletId = 0;
    TActorId PipeClient;
    TActorId ReplyTo;
}; // TSchemeWithPipeUploader

template <typename TSettings>
class TKesusResourcesUploader : public TSchemeWithPipeUploader<TKesusResourcesUploader<TSettings>, TSettings> {
    using TBase = TSchemeWithPipeUploader<TKesusResourcesUploader<TSettings>, TSettings>;
    using TThis = TKesusResourcesUploader;

    void GetAllResources() {
        using namespace NKesus;
        if (!this->PipeClient) {
            this->CreatePipe();
        }
        THolder<TEvKesus::TEvDescribeQuoterResources> req = MakeHolder<TEvKesus::TEvDescribeQuoterResources>();
        req->Record.SetRecursive(true);

        NTabletPipe::SendData(this->SelfId(), this->PipeClient, req.Release());
        this->Become(&TThis::StateDescribeResources);
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
        ResourcesMetadata.reserve(Resources.size());

        this->ClosePipe();
        UploadBatch();
    }

    bool AddFiles(const TString& fileName, const TString& content) {
        if (!this->AddFile(fileName, content, MakeIV(IV, NBackup::EBackupFileType::CoordinationNodeCreateRateLimiter))) {
            return false;
        }

        return !EnableChecksums
            || this->AddFile(NBackup::ChecksumKey(fileName), NBackup::ComputeChecksum(content));
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
            if (!BuildRateLimiterResourceScheme(resource, scheme)) {
                return Finish(false, "failed to build rate limiter scheme");
            }

            std::stringstream prefix;
            if (IV) {
                prefix << std::setfill('0') << std::setw(3) << std::right << ++EncryptedIdx;
            } else {
                prefix << resource.GetResourcePath();
            }
            ResourcesMetadata.push_back({prefix.str(), resource.GetResourcePath()});
            prefix << '/' << NYdb::NDump::NFiles::CreateRateLimiter().FileName;

            if (!AddFiles(prefix.str(), scheme)) {
                return;
            }
        }

        Offset += BatchSize;

        this->UploadFiles();
    }

    void OnFilesUploaded(bool success, const TString& error) override {
        if (!success || Offset >= Resources.size()) {
            return Finish(success, error);
        }
        UploadBatch();
    }

    void Finish(bool success = true, const TString& error = TString()) override {
        LOG_I("Finish"
            << ", self: " << this->SelfId()
            << ", success: " << success
            << ", error: " << error
        );

        this->Send(this->ReplyTo, new TEvPrivate::TEvExportUploadKesusResourcesResult(ExportId, ItemIdx, success, error, ResourcesMetadata));
        this->PassAway();
    }

public:
    TKesusResourcesUploader(
        ui64 kesusTabletId,
        TActorId replyTo,
        ui64 exportId,
        ui32 itemIdx,
        const TSettings& settings,
        TMaybe<NBackup::TEncryptionIV> iv,
        const bool enableChecksums
    )
        : TBase(kesusTabletId, replyTo, itemIdx, settings)
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
            hFunc(TEvTabletPipe::TEvClientConnected, TBase::HandleConnected);
            hFunc(TEvTabletPipe::TEvClientDestroyed, TBase::HandleDestroyed);
            sFunc(TEvents::TEvPoisonPill, TBase::PassAway);
        }
    }

private:
    ui64 ExportId;
    ui32 ItemIdx;

    TMaybe<NBackup::TEncryptionIV> IV;
    ui32 EncryptedIdx = 0;

    const bool EnableChecksums;

    const i32 BatchSize = 100;
    i32 Offset = 0;

    google::protobuf::RepeatedPtrField<NKikimrKesus::TStreamingQuoterResource> Resources;
    TVector<NBackup::TRateLimiterResourceMetadata> ResourcesMetadata;
}; // TKesusResourcesUploader

template <typename TSettings>
IActor* CreateKesusResourcesUploader(
    ui64 kesusTabletId, TActorId replyTo,
    ui64 exportId, ui32 itemIdx,
    const TSettings &settings,
    TMaybe<NBackup::TEncryptionIV> iv,
    const bool enableChecksums)
{
    return new TKesusResourcesUploader<TSettings>(kesusTabletId, replyTo, exportId, itemIdx, settings, iv, enableChecksums);
}

template <typename TSettings>
class TSchemeUploader: public TExportFilesUploader<TSchemeUploader<TSettings>, TSettings> {
    using TThis = TSchemeUploader;
    using TBase = TExportFilesUploader<TSchemeUploader, TSettings>;

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

        const auto& desc = describeResult.GetPathDescription();
        if (desc.GetSelf().GetPathType() == NKikimrSchemeOp::EPathTypeKesus) {
            // Upload resources before to store them in metadata
            Y_ABORT_UNLESS(desc.HasKesus());
            StartUploadKesusResources(desc.GetKesus().GetKesusTabletId());
        } else {
            StartUploadFiles();
        }
    }

    void StartUploadKesusResources(ui64 kesusTabletId) {
        KesusResourcesUploader = this->Register(CreateKesusResourcesUploader(
            kesusTabletId,
            this->SelfId(),
            ExportId,
            ItemIdx,
            this->GetSettings(),
            IV,
            EnableChecksums
        ));
        this->Become(&TThis::StateUploadKesusResources);
    }

    void HandleResourcesUploaded(TEvPrivate::TEvExportUploadKesusResourcesResult::TPtr ev) {
        const auto& record = ev->Get();
        LOG_D("HandleResourcesUploaded"
            << ", self: " << this->SelfId()
            << ", success: " << record->Success
            << ", error: " << record->Error);

        if (!record->Success) {
            return RetryResourcesUploadOrFail(record->Error);
        }

        // Fill metadata with rate limiter resources
        for (auto& rateLimiterMetadata : record->ResourcesMetadata) {
            Metadata.AddRateLimiterResource(rateLimiterMetadata);
        }

        // Upload everything else
        StartUploadFiles();
    }

    void RetryResourcesUploadOrFail(const TString& error) {
        LOG_D("RetryResourcesUploadOrFail"
            << ", self: " << this->SelfId()
            << ", attempts " << KesusResourcesUploadAttempts + 1
            << ", max attempts" << MaxKesusResourcesUploadAttempts
            << ", error " << error);

        if (++KesusResourcesUploadAttempts >= MaxKesusResourcesUploadAttempts) {
            return Finish(false, error);
        }

        if (auto uploader = std::exchange(KesusResourcesUploader, {})) {
            this->Send(uploader, new TEvents::TEvPoisonPill());
        }

        this->ScheduleRetry(TDuration::Seconds(2));
    }

    void StartUploadFiles() {
        if (!Scheme) {
            return Finish(false, "cannot infer scheme");
        }

        if (!this->AddFile(FileName, Scheme, MakeIV(IV, SchemeFileType))) {
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

            if (!this->AddFile("permissions.pb", Permissions, MakeIV(IV, NBackup::EBackupFileType::Permissions))) {
                return;
            }

            if (EnableChecksums) {
                if (!this->AddFile(NBackup::ChecksumKey("permissions.pb"), NBackup::ComputeChecksum(Permissions))) {
                    return;
                }
            }
        }

        const auto serializedMetadata = Metadata.Serialize();
        if (!serializedMetadata) {
            return Finish(false, "empty metadata");
        }

        if (!this->AddFile("metadata.json", serializedMetadata, IV)) {
            return;
        }

        if (EnableChecksums) {
            if (!this->AddFile(NBackup::ChecksumKey("metadata.json"), NBackup::ComputeChecksum(serializedMetadata))) {
                return;
            }
        }

        this->UploadFiles();
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

    void PassAway() override {
        if (KesusResourcesUploader) {
            this->Send(KesusResourcesUploader, new TEvents::TEvPoisonPill());
        }
        TBase::PassAway();
    }

public:
    TSchemeUploader(
        TActorId schemeShard,
        ui64 exportId,
        ui32 itemIdx,
        TPathId sourcePathId,
        const TSettings& settings,
        const TString& databaseRoot,
        NBackup::TMetadata metadata,
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

    STATEFN(StateUploadKesusResources) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvExportUploadKesusResourcesResult, HandleResourcesUploaded);
            sFunc(TEvents::TEvWakeup, GetDescription)
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
    NBackup::TMetadata Metadata;

    TActorId KesusResourcesUploader;
    ui32 KesusResourcesUploadAttempts = 0;
    const ui32 MaxKesusResourcesUploadAttempts = 10;
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
    const TSettings& settings, const TString& databaseRoot, NBackup::TMetadata metadata,
    bool enablePermissions, bool enableChecksums, const TMaybe<NBackup::TEncryptionIV>& iv
) {
    return new TSchemeUploader<TSettings>(schemeShard, exportId, itemIdx, sourcePathId, settings, databaseRoot,
        std::move(metadata), enablePermissions, enableChecksums, iv);
}

template <typename TSettings>
NActors::IActor* CreateExportMetadataUploader(NActors::TActorId schemeShard, ui64 exportId,
    const TSettings& settings, const NKikimrSchemeOp::TExportMetadata& exportMetadata,
    bool enableChecksums
) {
    return new TExportMetadataUploader<TSettings>(schemeShard, exportId, settings, exportMetadata, enableChecksums);
}

template IActor* CreateSchemeUploader<Ydb::Export::ExportToS3Settings>(
    TActorId schemeShard, ui64 exportId, ui32 itemIdx, TPathId sourcePathId,
    const Ydb::Export::ExportToS3Settings& settings, const TString& databaseRoot, NBackup::TMetadata metadata,
    bool enablePermissions, bool enableChecksums, const TMaybe<NBackup::TEncryptionIV>& iv
);

template IActor* CreateSchemeUploader<Ydb::Export::ExportToFsSettings>(
    TActorId schemeShard, ui64 exportId, ui32 itemIdx, TPathId sourcePathId,
    const Ydb::Export::ExportToFsSettings& settings, const TString& databaseRoot, NBackup::TMetadata metadata,
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
