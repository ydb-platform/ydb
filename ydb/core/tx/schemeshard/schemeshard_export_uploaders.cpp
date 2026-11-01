#include "schemeshard_export_uploaders.h"

#include "schemeshard.h"
#include "schemeshard_xxport__helpers.h"

#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/api/protos/ydb_rate_limiter.pb.h>

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
#include <ydb/core/ydb_convert/kesus_description.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/backup/proto/proto.h>

#include <library/cpp/json/json_writer.h>

#include <util/string/cast.h>

#include <algorithm>
#include <type_traits>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::EXPORT

namespace NKikimr::NSchemeShard {

namespace {

template <typename TSettings>
TString GetSchemeDestinationPrefix(const TSettings& settings, ui32 itemIdx) {
    if (itemIdx < ui32(settings.items_size())) {
        const TString itemDest = NBackup::NFieldsWrappers::GetItemDestination(settings.items(itemIdx));
        if constexpr (std::is_same_v<TSettings, Ydb::Export::ExportToFsSettings>) {
            return CanonizePath(TStringBuilder() << settings.base_path() << "/" << itemDest);
        }
        return itemDest;
    }
    return NBackup::NFieldsWrappers::GetCommonDestination(settings);
}

TMaybe<NBackup::TEncryptionIV> MakeSchemeFileIV(
    const TMaybe<NBackup::TEncryptionIV>& itemIV, NBackup::EBackupFileType fileType)
{
    if (itemIV) {
        return NBackup::TEncryptionIV::Combine(*itemIV, fileType, 0 /* backupItemNumber: already combined */, 0 /* shardNumber */);
    }
    return Nothing();
}

} // anonymous namespace

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

    TString GetObjectKey(TStringBuf path, bool addEncryptionSuffix = false) const {
        return TStringBuilder() << NBackup::NormalizeExportPrefix(DestinationPrefix) << '/' << path
            << (addEncryptionSuffix ? ".enc" : "");
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
                Fail(TStringBuilder() << GetObjectKey(filePath, true) << ": internal error: no encryption key");
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
                Fail(TStringBuilder() << GetObjectKey(filePath) << ": failed to encrypt: " << ex.what());
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

        auto request = Aws::S3::Model::PutObjectRequest().WithKey(GetObjectKey(upload.Path));

        this->Send(StorageOperator, new TEvExternalStorage::TEvPutObjectRequest(request, TString(upload.Content)));
    }

    void Handle(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;
        TFileUpload& upload = Files.front();

        YDB_LOG_DEBUG("Put file response " << upload.Path,
            {"self", this->SelfId()},
            {"key", GetObjectKey(upload.Path)},
            {"result", result},
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
            Fail(TStringBuilder() << GetObjectKey(upload.Path) << ": " << LogPrefix() << " error: " << error.GetMessage());
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

public:
    static constexpr TStringBuf LogPrefix() {
        return NBackup::NFieldsWrappers::GetStorageName<TSettings>();
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

class TExportCompletionUploader: public TExportFilesUploader<TExportCompletionUploader, Ydb::Export::ExportToS3Settings> {
public:
    TExportCompletionUploader(
            TActorId schemeShard,
            ui64 exportId,
            ui32 itemIdx,
            const Ydb::Export::ExportToS3Settings& settings,
            const TString& destinationPrefix)
        : TExportFilesUploader(settings, destinationPrefix)
        , SchemeShard(schemeShard)
        , ExportId(exportId)
        , ItemIdx(itemIdx)
    {}

    void Bootstrap() {
        Become(&TExportCompletionUploader::StateWait);
    }

private:
    void Upload() {
        const TString fileName(ItemIdx == Max<ui32>() ? ExportCompleteFileName : ExportDataCompleteFileName);
        NJson::TJsonValue content(NJson::JSON_MAP);
        content["version"] = 1;
        content["export_id"] = ToString(ExportId);
        if (AddFile(fileName, NJson::WriteJson(content, false, true))) {
            UploadFiles();
        }
    }

    void OnFilesUploaded(bool success, const TString& error) override {
        TString issue;
        if (!success) {
            issue = TStringBuilder() << "Data uploaded, but completion marker publication failed: " << error;
        }

        Send(SchemeShard, new TEvPrivate::TEvExportCompletionUploadResult(ExportId, ItemIdx, success, issue));
        PassAway();
    }

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvWakeup, Upload);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    const TActorId SchemeShard;
    const ui64 ExportId;
    const ui32 ItemIdx;
};

IActor* CreateExportCompletionUploader(
    TActorId schemeShard,
    ui64 exportId,
    ui32 itemIdx,
    const Ydb::Export::ExportToS3Settings& settings,
    const TString& destinationPrefix)
{
    return new TExportCompletionUploader(schemeShard, exportId, itemIdx, settings, destinationPrefix);
}

class TKesusResourcesDescriber: public TActorBootstrapped<TKesusResourcesDescriber> {
public:
    TKesusResourcesDescriber(TActorId owner, ui64 kesusTabletId)
        : Owner(owner)
        , KesusTabletId(kesusTabletId)
    {}

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::EXPORT_UPLOADER_ACTOR;
    }

    void Bootstrap() {
        Deadline = Now() + TDuration::Minutes(1);
        Schedule(TDuration::Minutes(1), new TEvPrivate::TEvExportKesusTimeout());
        DescribeResources();
    }

private:
    void DescribeResources() {
        if (Now() >= Deadline) {
            return HandleTimeout();
        }

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {.RetryLimitCount = 3};
        KesusPipe = Register(NTabletPipe::CreateClient(SelfId(), KesusTabletId, pipeConfig));

        // An empty path/id filter with Recursive=true describes the entire resource tree
        // in one Kesus transaction, including children and their original properties.
        auto request = MakeHolder<NKesus::TEvKesus::TEvDescribeQuoterResources>();
        request->Record.SetRecursive(true);
        Become(&TKesusResourcesDescriber::StateDescribeResources);
        NTabletPipe::SendData(SelfId(), KesusPipe, request.Release(), ++RequestCookie);
    }

    void HandleResources(NKesus::TEvKesus::TEvDescribeQuoterResourcesResult::TPtr& ev) {
        // A response sent before a pipe failure may arrive after reconnection.
        if (ev->Cookie != RequestCookie) {
            return;
        }
        ClosePipe();

        const auto& record = ev->Get()->Record;
        if (record.GetError().GetStatus() != Ydb::StatusIds::SUCCESS) {
            return Finish(false, {}, TStringBuilder() << "Cannot describe coordination node resources: "
                << record.GetError().ShortDebugString());
        }

        Ydb::RateLimiter::Resources resources;
        for (const auto& resource : record.GetResources()) {
            FillRateLimiterDescription(*resources.add_resources(), resource);
        }

        auto& items = *resources.mutable_resources();
        // A parent path is a proper prefix of its children, so it sorts before them.
        std::sort(items.begin(), items.end(), [](const auto& lhs, const auto& rhs) {
            return lhs.resource_path() < rhs.resource_path();
        });

        TString content;
        if (items.empty()) {
            // PrintProto omits empty repeated fields. Keep a nonempty, valid textproto
            // so a retry also overwrites a previously exported nonempty resource set.
            content = "resources: []\n";
        } else if (!NYdb::NBackup::PrintProto(resources, content)) {
            return Finish(false, {}, "Cannot serialize coordination node resources");
        }

        Finish(true, std::move(content), {});
    }

    void HandleConnected(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->ClientId == KesusPipe && ev->Get()->Status != NKikimrProto::OK) {
            if (ev->Get()->Dead) {
                return Finish(false, {}, "Coordination node tablet does not exist");
            }
            Retry("Failed to connect to coordination node");
        }
    }

    void HandleDestroyed(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (ev->Get()->ClientId == KesusPipe) {
            Retry("Connection to coordination node was lost");
        }
    }

    void Retry(TStringBuf reason) {
        ClosePipe();
        const auto now = Now();
        if (now >= Deadline) {
            return HandleTimeout();
        }

        const auto delay = Min(RetryDelay, Deadline - now);
        YDB_LOG_WARN("Retry describing coordination node resources",
            {"self", SelfId()},
            {"kesusTabletId", KesusTabletId},
            {"reason", reason},
            {"delay", delay},
        );
        Become(&TKesusResourcesDescriber::StateWaitRetry);
        Schedule(delay, new TEvents::TEvWakeup());
        RetryDelay = Min(RetryDelay * 2, TDuration::Seconds(5));
    }

    void HandleTimeout() {
        Finish(false, {}, "Timed out waiting for coordination node resources");
    }

    void ClosePipe() {
        if (KesusPipe) {
            NTabletPipe::CloseClient(SelfId(), KesusPipe);
            KesusPipe = {};
        }
    }

    void PassAway() override {
        ClosePipe();
        IActor::PassAway();
    }

    void Finish(bool success, TString content, TString error) {
        YDB_LOG_INFO("Finish describing coordination node resources",
            {"self", SelfId()},
            {"kesusTabletId", KesusTabletId},
            {"success", success},
            {"error", error},
        );
        Send(Owner, new TEvPrivate::TEvKesusResourcesDescribeResult(success, std::move(content), std::move(error)));
        PassAway();
    }

    STATEFN(StateDescribeResources) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKesus::TEvKesus::TEvDescribeQuoterResourcesResult, HandleResources);
            hFunc(TEvTabletPipe::TEvClientConnected, HandleConnected);
            hFunc(TEvTabletPipe::TEvClientDestroyed, HandleDestroyed);
            sFunc(TEvPrivate::TEvExportKesusTimeout, HandleTimeout);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    STATEFN(StateWaitRetry) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvWakeup, DescribeResources);
            sFunc(TEvPrivate::TEvExportKesusTimeout, HandleTimeout);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    const TActorId Owner;
    const ui64 KesusTabletId;
    TActorId KesusPipe;
    ui64 RequestCookie = 0;
    TInstant Deadline;
    TDuration RetryDelay = TDuration::MilliSeconds(100);
}; // TKesusResourcesDescriber

template <typename TSettings>
class TSchemeUploader: public TExportFilesUploader<TSchemeUploader<TSettings>, TSettings> {
    using TThis = TSchemeUploader;
    using TBase = TExportFilesUploader<TThis, TSettings>;

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

        YDB_LOG_DEBUG("HandleSchemeDescription",
            {"self", this->SelfId()},
            {"status", describeResult.GetStatus()},
        );

        if (describeResult.GetStatus() != TEvSchemeShard::EStatus::StatusSuccess) {
            return Finish(false, describeResult.GetReason());
        }

        TString error;
        if (!BuildSchemeToUpload(describeResult, error)) {
            return Finish(false, error);
        }

        if (auto permissions = NDataShard::GenYdbPermissions(describeResult.GetPathDescription())) {
            Y_ENSURE(NYdb::NBackup::PrintProto(permissions.GetRef(), Permissions));
        } else {
            return Finish(false, TStringBuilder() << this->GetObjectKey("permissions.pb", IV.Defined()) << ": cannot infer permissions");
        }

        if (PathType == NKikimrSchemeOp::EPathTypeKesus) {
            const ui64 kesusTabletId = describeResult.GetPathDescription().GetKesus().GetKesusTabletId();
            if (!kesusTabletId) {
                return Finish(false, "Coordination node tablet id is missing");
            }

            ResourcesDescriber = this->Register(new TKesusResourcesDescriber(this->SelfId(), kesusTabletId));
            this->Become(&TThis::StateWaitResources);
        } else {
            StartUploadFiles();
        }
    }

    void HandleResources(TEvPrivate::TEvKesusResourcesDescribeResult::TPtr& ev) {
        if (ev->Sender != ResourcesDescriber) {
            return;
        }
        ResourcesDescriber = {};

        if (!ev->Get()->Success) {
            return Finish(false, ev->Get()->Error);
        }

        KesusResources = std::move(ev->Get()->Content);
        StartUploadFiles();
    }

    bool AddFileWithChecksum(const TString& name, const TString& content, NBackup::EBackupFileType fileType) {
        return this->AddFile(name, content, MakeSchemeFileIV(IV, fileType))
            && (!EnableChecksums || this->AddFile(NBackup::ChecksumKey(name), NBackup::ComputeChecksum(content)));
    }

    void StartUploadFiles() {
        if (!Scheme) {
            return Finish(false, TStringBuilder() << this->GetObjectKey(FileName, IV.Defined()) << ": cannot infer scheme");
        }

        if (!AddFileWithChecksum(FileName, Scheme, SchemeFileType)) {
            return;
        }

        if (KesusResources && !AddFileWithChecksum("rate_limiter_resources.pb", *KesusResources,
                NBackup::EBackupFileType::CoordinationNodeCreateRateLimiter))
        {
            return;
        }

        if (EnablePermissions) {
            if (!Permissions) {
                return Finish(false, TStringBuilder() << this->GetObjectKey("permissions.pb", IV.Defined()) << ": cannot infer permissions");
            }

            if (!AddFileWithChecksum("permissions.pb", Permissions, NBackup::EBackupFileType::Permissions)) {
                return;
            }
        }

        if (!Metadata) {
            return Finish(false, TStringBuilder() << this->GetObjectKey("metadata.json", IV.Defined()) << ": empty metadata");
        }
        if (!AddFileWithChecksum("metadata.json", Metadata, NBackup::EBackupFileType::Metadata)) {
            return;
        }

        this->UploadFiles();
    }

    void PassAway() override {
        if (ResourcesDescriber) {
            this->Send(ResourcesDescriber, new TEvents::TEvPoisonPill());
            ResourcesDescriber = {};
        }
        TBase::PassAway();
    }

    void Finish(bool success = true, const TString& error = TString()) {
        YDB_LOG_INFO("Finish",
            {"self", this->SelfId()},
            {"success", success},
            {"error", error},
        );

        this->Send(SchemeShard, new TEvPrivate::TEvExportSchemeUploadResult(ExportId, ItemIdx, success, error));
        this->PassAway();
    }

    void OnFilesUploaded(bool success, const TString& error) override {
        Finish(success, error);
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
        : TExportFilesUploader<TSchemeUploader, TSettings>(settings, GetSchemeDestinationPrefix(settings, itemIdx))
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

    STATEFN(StateWaitResources) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvKesusResourcesDescribeResult, HandleResources);
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
    TActorId ResourcesDescriber;
    TMaybe<TString> KesusResources;
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
        YDB_LOG_INFO("Finish uploading export metadata",
            {"self", this->SelfId()},
            {"success", success},
            {"error", error},
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

#undef YDB_LOG_THIS_FILE_COMPONENT
