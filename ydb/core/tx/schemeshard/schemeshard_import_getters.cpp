#include "schemeshard_import_getters.h"

#include "schemeshard_import_helpers.h"
#include "schemeshard_private.h"
#include "schemeshard_xxport__helpers.h"

#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/backup/regexp/regexp.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/wrappers/retry_policy.h>
#include <ydb/core/wrappers/s3_storage_config.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/api/protos/ydb_import.pb.h>
#include <ydb/public/lib/ydb_cli/dump/files/files.h>

#include <library/cpp/json/json_reader.h>

#include <google/protobuf/text_format.h>

#include <util/stream/file.h>
#include <util/system/fs.h>
#include <util/folder/path.h>
#include <util/string/subst.h>

#include <algorithm>

namespace NKikimr {
namespace NSchemeShard {

using namespace NWrappers;

using namespace Aws::Auth;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws;

static constexpr TDuration MaxDelay = TDuration::Minutes(10);

struct TGetterSettings {
    NWrappers::IExternalStorageConfig::TPtr ExternalStorageConfig;
    ui32 Retries;
    TMaybe<NBackup::TEncryptionKey> Key;
    TMaybe<NBackup::TEncryptionIV> IV;

    static TGetterSettings FromImportInfo(const TImportInfo::TPtr& importInfo, TMaybe<NBackup::TEncryptionIV> iv) {
        TGetterSettings settings;
        std::visit([&settings, &iv](const auto& s) {
            settings.ExternalStorageConfig = NWrappers::IExternalStorageConfig::Construct(AppData()->AwsClientConfig,s);
            settings.Retries = s.number_of_retries();
            if (s.has_encryption_settings()) {
                settings.Key = NBackup::TEncryptionKey(s.encryption_settings().symmetric_key().key());
            }
            settings.IV = std::move(iv);
        }, importInfo->Settings);
        return settings;
    }

    static TGetterSettings FromRequest(const TEvImport::TEvListObjectsInS3ExportRequest::TPtr& ev) {
        TGetterSettings settings;
        settings.ExternalStorageConfig.reset(new NWrappers::NExternalStorage::TS3ExternalStorageConfig(
            AppData()->AwsClientConfig,
            ev->Get()->Record.settings()));
        settings.Retries = ev->Get()->Record.settings().number_of_retries();
        if (ev->Get()->Record.settings().has_encryption_settings()) {
            settings.Key = NBackup::TEncryptionKey(ev->Get()->Record.settings().encryption_settings().symmetric_key().key());
        }
        return settings;
    }
};

template <class TDerived>
class TGetterFromS3 : public TActorBootstrapped<TDerived> {
protected:
    explicit TGetterFromS3(TGetterSettings&& settings)
        : ExternalStorageConfig(std::move(settings.ExternalStorageConfig))
        , Key(std::move(settings.Key))
        , IV(std::move(settings.IV))
        , Retries(settings.Retries)
    {
    }

    void HeadObject(const TString& key, bool autoAddEncSuffix = true) {
        auto request = Model::HeadObjectRequest()
            .WithKey(GetKey(key, autoAddEncSuffix));

        this->Send(Client, new TEvExternalStorage::TEvHeadObjectRequest(request));
    }

    void GetObject(const TString& key, const std::pair<ui64, ui64>& range, bool autoAddEncSuffix = true) {
        auto request = Model::GetObjectRequest()
            .WithKey(GetKey(key, autoAddEncSuffix))
            .WithRange(TStringBuilder() << "bytes=" << range.first << "-" << range.second);

        this->Send(Client, new TEvExternalStorage::TEvGetObjectRequest(request));
    }

    void GetObject(const TString& key, ui64 contentLength, bool autoAddEncSuffix = true) {
        GetObject(key, std::make_pair(0, contentLength - 1), autoAddEncSuffix);
    }

    void ListObjects(const TString& prefix) {
        auto request = Model::ListObjectsRequest()
            .WithPrefix(prefix);

        this->Send(Client, new TEvExternalStorage::TEvListObjectsRequest(request));
    }

    void Download(const TString& key, bool autoAddEncSuffix = true) {
        CreateClient();
        HeadObject(key, autoAddEncSuffix);
    }

    void CreateClient() {
        if (Client) {
            this->Send(Client, new TEvents::TEvPoisonPill());
        }
        Client = this->RegisterWithSameMailbox(CreateStorageWrapper(ExternalStorageConfig->ConstructStorageOperator()));
    }

    void PassAway() override {
        this->Send(Client, new TEvents::TEvPoisonPill());
        TActorBootstrapped<TDerived>::PassAway();
    }

    TString GetKey(TString key, bool autoAddEncSuffix = true) {
        if (autoAddEncSuffix && Key) {
            key += ".enc";
        }
        return key;
    }

    template <typename TResult>
    bool CheckResult(const TResult& result, const TStringBuf marker) {
        if (result.IsSuccess()) {
            return true;
        }

        LOG_E("Error at '" << marker << "'"
            << ": self# " << this->SelfId()
            << ", error# " << result);
        MaybeRetry(result.GetError());

        return false;
    }

    void MaybeRetry(const Aws::S3::S3Error& error) {
        const auto shouldRetry = NWrappers::ShouldRetry(error);
        if (Attempt < Retries && shouldRetry) {
            Delay = Min(Delay * ++Attempt, MaxDelay);
            this->Schedule(Delay, new TEvents::TEvWakeup());
        } else {
            Reply(shouldRetry ? Ydb::StatusIds::EXTERNAL_ERROR : Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "S3 error: " << error.GetMessage().c_str());
        }
    }

    template <typename TResult>
    bool IsNoSuchKeyError(const TResult& result) {
        if (result.IsSuccess()) {
            return false;
        }
        const auto& err = result.GetError();
        if (err.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY) {
            return true;
        }
        if (err.GetErrorType() == Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
            return true;
        }
        if (err.GetExceptionName() == "NoSuchKey") {
            return true;
        }
        return false;
    }

    void ResetRetries() {
        Attempt = 0;
    }

    // If export is encrypted, decrypts and gets export IV,
    // else returns true
    bool MaybeDecryptAndSaveIV(const TString& content, TString& result) {
        if (Key) {
            try {
                auto [buffer, iv] = NBackup::TEncryptedFileDeserializer::DecryptFullFile(*Key, TBuffer(content.data(), content.size()));
                IV = iv;
                result.assign(buffer.Data(), buffer.Size());
                return true;
            } catch (const std::exception& ex) {
                Reply(Ydb::StatusIds::BAD_REQUEST, ex.what());
                return false;
            }
        }
        result = content;
        return true;
    }

    bool MaybeDecrypt(const TString& content, TString& result, NBackup::EBackupFileType fileType, ui32 shardNumber = 0) {
        if (Key && IV) {
            try {
                NBackup::TEncryptionIV expectedIV = NBackup::TEncryptionIV::Combine(*IV, fileType, 0 /* backupItemNumber: already combined */, shardNumber);
                auto buffer = NBackup::TEncryptedFileDeserializer::DecryptFullFile(*Key, expectedIV, TBuffer(content.data(), content.size()));
                result.assign(buffer.Data(), buffer.Size());
                return true;
            } catch (const std::exception& ex) {
                Reply(Ydb::StatusIds::BAD_REQUEST, ex.what());
                return false;
            }
        }
        result = content;
        return true;
    }

    virtual void Reply(Ydb::StatusIds::StatusCode statusCode = Ydb::StatusIds::SUCCESS, const TString& error = TString()) = 0;

    void HandleChecksum(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandleChecksum TEvExternalStorage::TEvHeadObjectResponse"
            << ": self# " << this->SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "HeadObject")) {
            return;
        }

        GetObject(NBackup::ChecksumKey(CurrentObjectKey), result.GetResult().GetContentLength(), false);
    }

    void HandleChecksum(TEvExternalStorage::TEvGetObjectResponse::TPtr& ev) {
        const auto& msg = *ev->Get();
        const auto& result = msg.Result;

        LOG_D("HandleChecksum TEvExternalStorage::TEvGetObjectResponse"
            << ": self# " << this->SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "GetObject")) {
            return;
        }

        TString expectedChecksum = msg.Body.substr(0, msg.Body.find(' '));
        if (expectedChecksum != CurrentObjectChecksum) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Checksum mismatch for " << CurrentObjectKey
                << " expected# " << expectedChecksum
                << ", got# " << CurrentObjectChecksum);
        }

        ChecksumValidatedCallback();
    }

    void DownloadChecksum() {
        Download(NBackup::ChecksumKey(CurrentObjectKey), false);
    }

    void StartValidatingChecksum(const TString& key, const TString& object, std::function<void()> checksumValidatedCallback) {
        CurrentObjectKey = key;
        CurrentObjectChecksum = NBackup::ComputeChecksum(object);
        ChecksumValidatedCallback = checksumValidatedCallback;

        ResetRetries();
        DownloadChecksum();
        this->Become(&TGetterFromS3<TDerived>::StateDownloadChecksum);
    }

    STATEFN(StateDownloadChecksum) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvHeadObjectResponse, HandleChecksum);
            hFunc(TEvExternalStorage::TEvGetObjectResponse, HandleChecksum);

            sFunc(TEvents::TEvWakeup, DownloadChecksum);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

protected:
    NWrappers::IExternalStorageConfig::TPtr ExternalStorageConfig;
    TActorId Client;
    TMaybe<NBackup::TEncryptionKey> Key;
    TMaybe<NBackup::TEncryptionIV> IV;

    const ui32 Retries;
    ui32 Attempt = 0;

    TDuration Delay = TDuration::Minutes(1);

    TString CurrentObjectChecksum;
    TString CurrentObjectKey;
    std::function<void()> ChecksumValidatedCallback;
};

// Downloads scheme-related objects from S3
class TSchemeGetter: public TGetterFromS3<TSchemeGetter> {
    static TString GetItemSource(const TImportInfo& importInfo, ui32 itemIdx) {
        TString srcPrefix = importInfo.GetItemSrcPrefix(itemIdx);

        // Absolute path in the prefix is possible if the backup with SchemaMapping
        if (importInfo.Kind == TImportInfo::EKind::FS && !srcPrefix.empty() && srcPrefix[0] != '/') {
            return CanonizePath(TStringBuilder() << importInfo.GetFsSettings().base_path() << "/" << srcPrefix);
        }

        return srcPrefix;
    }

    static TString MetadataKeyFromSettings(const TImportInfo& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        return TStringBuilder() << GetItemSource(importInfo, itemIdx) << "/metadata.json";
    }

    static TString SchemeKeyFromSettings(const TImportInfo& importInfo, ui32 itemIdx, TStringBuf filename) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        return TStringBuilder() << GetItemSource(importInfo, itemIdx) << '/' << filename;
    }

    static TString PermissionsKeyFromSettings(const TImportInfo& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        return TStringBuilder() << GetItemSource(importInfo, itemIdx) << "/permissions.pb";
    }

    static TString MaterializedIndexSchemeKeyFromSettings(const TImportInfo& importInfo, ui32 itemIdx, const TString& indexImplTablePrefix) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        return TStringBuilder() << GetItemSource(importInfo, itemIdx) << "/" << indexImplTablePrefix << "/scheme.pb";
    }

    static TString ChangefeedDescriptionKeyFromSettings(const TImportInfo& importInfo, ui32 itemIdx, const TString& changefeedPrefix) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        return TStringBuilder() << GetItemSource(importInfo, itemIdx) << "/" << changefeedPrefix << "/changefeed_description.pb";
    }

    static TString TopicDescriptionKeyFromSettings(const TImportInfo& importInfo, ui32 itemIdx, const TString& changefeedPrefix) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        return TStringBuilder() << GetItemSource(importInfo, itemIdx) << "/" << changefeedPrefix << "/topic_description.pb";
    }

    static bool IsView(TStringBuf schemeKey) {
        return schemeKey.EndsWith(NYdb::NDump::NFiles::CreateView().FileName);
    }

    static bool IsTable(TStringBuf schemeKey) {
        return schemeKey.EndsWith(NYdb::NDump::NFiles::TableScheme().FileName);
    }

    static bool IsTopic(TStringBuf schemeKey) {
        return schemeKey.EndsWith(NYdb::NDump::NFiles::CreateTopic().FileName);
    }

    static bool IsReplication(TStringBuf schemeKey) {
        return schemeKey.EndsWith(NYdb::NDump::NFiles::CreateAsyncReplication().FileName);
    }

    static bool IsTransfer(TStringBuf schemeKey) {
        return schemeKey.EndsWith(NYdb::NDump::NFiles::CreateTransfer().FileName);
    }

    static bool IsExternalDataSource(TStringBuf schemeKey) {
        return schemeKey.EndsWith(NYdb::NDump::NFiles::CreateExternalDataSource().FileName);
    }

    static bool IsExternalTable(TStringBuf schemeKey) {
        return schemeKey.EndsWith(NYdb::NDump::NFiles::CreateExternalTable().FileName);
    }

    static bool IsSysView(TStringBuf schemeKey) {
        return schemeKey.EndsWith(NYdb::NDump::NFiles::SystemView().FileName);
    }

    static bool IsCreatedByQuery(TStringBuf schemeKey) {
        return IsView(schemeKey)
            || IsReplication(schemeKey)
            || IsTransfer(schemeKey)
            || IsExternalDataSource(schemeKey)
            || IsExternalTable(schemeKey);
    }

    static bool NoObjectFound(Aws::S3::S3Errors errorType) {
        return errorType == S3Errors::RESOURCE_NOT_FOUND || errorType == S3Errors::NO_SUCH_KEY;
    }

    void HandleMetadata(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandleMetadata TEvExternalStorage::TEvHeadObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "HeadObject")) {
            return;
        }

        GetObject(MetadataKey, result.GetResult().GetContentLength());
    }

    bool CheckAvailableInImport(NKikimrSchemeOp::EPathType pathType) {
        switch (pathType) {
            case NKikimrSchemeOp::EPathTypeView:
                return AppData()->FeatureFlags.GetEnableViewExport();
            case NKikimrSchemeOp::EPathTypeColumnTable:
                return AppData()->FeatureFlags.GetEnableColumnTablesBackup();
            case NKikimrSchemeOp::EPathTypeReplication:
                return AppData()->Icb->BackupControls.S3Controls.EnableAsyncReplicationImport.AtomicLoad()->Get();
            case NKikimrSchemeOp::EPathTypeTransfer:
                return AppData()->Icb->BackupControls.S3Controls.EnableTransferImport.AtomicLoad()->Get();
            case NKikimrSchemeOp::EPathTypeExternalDataSource:
                return AppData()->Icb->BackupControls.S3Controls.EnableExternalDataSourceImport.AtomicLoad()->Get();
            case NKikimrSchemeOp::EPathTypeExternalTable:
                return AppData()->Icb->BackupControls.S3Controls.EnableExternalTableImport.AtomicLoad()->Get();
            case NKikimrSchemeOp::EPathTypeSysView:
                return AppData()->FeatureFlags.GetEnableSysViewPermissionsExport();
            case NKikimrSchemeOp::EPathTypePersQueueGroup:
            case NKikimrSchemeOp::EPathTypeTable:
                return true;
            default:
                return false;
        }
    }

    void HeadNextScheme() {
        while (++SchemePropertiesIdx < GetXxportProperties().size()) {
            const auto& properties = GetXxportProperties()[SchemePropertiesIdx];
            LOG_D("HeadNextScheme"
                << ": self# " << SelfId()
                << ", file name# " << properties.FileName);
            if (!CheckAvailableInImport(properties.PathType)) {
                LOG_D(TStringBuilder() << properties.FileName << " not available in imports");
                continue;
            }

            SchemeKey = SchemeKeyFromSettings(*ImportInfo, ItemIdx, properties.FileName);
            SchemeFileType = properties.FileType;
            return HeadObject(SchemeKey);
        }

        return Reply(Ydb::StatusIds::BAD_REQUEST, "Unsupported scheme object type");
    }

    void HandleScheme(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandleScheme TEvExternalStorage::TEvHeadObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (NoObjectFound(result.GetError().GetErrorType())) {
            HeadNextScheme();
            return;
        }

        if (!CheckResult(result, "HeadObject")) {
            return;
        }

        GetObject(SchemeKey, result.GetResult().GetContentLength());
    }

    void HandlePermissions(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandlePermissions TEvExternalStorage::TEvHeadObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (NoObjectFound(result.GetError().GetErrorType())) {
            Y_ABORT_UNLESS(ItemIdx < ImportInfo->Items.size());
            auto& item = ImportInfo->Items.at(ItemIdx);
            if (!item.Metadata.HasEnablePermissions()) {
                return StartCheckingMaterializedIndexes(); // permissions are optional if we don't know if they were created during export
            } else {
                return Reply(Ydb::StatusIds::BAD_REQUEST, "No permissions file found");
            }
            return;
        } else if (!CheckResult(result, "HeadObject")) {
            return;
        }

        GetObject(PermissionsKey, result.GetResult().GetContentLength());
    }

    void HandleIndex(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandleIndex TEvExternalStorage::TEvHeadObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        const bool canSkip = IndexPopulationMode != Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_IMPORT;
        if (canSkip && NoObjectFound(result.GetError().GetErrorType())) {
            Y_ABORT_UNLESS(ItemIdx < ImportInfo->Items.size());
            auto& item = ImportInfo->Items.at(ItemIdx);
            item.MaterializedIndexes.clear();
            return StartDownloadingChangefeeds();
        } else if (!CheckResult(result, "HeadObject")) {
            return;
        }

        Y_ABORT_UNLESS(IndexCheckedMaterializedIndexImplTable < IndexImplTablePrefixes.size());
        GetObject(MaterializedIndexSchemeKeyFromSettings(*ImportInfo, ItemIdx,
            IndexImplTablePrefixes[IndexCheckedMaterializedIndexImplTable].ExportPrefix), result.GetResult().GetContentLength());
    }

    void HandleChangefeed(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandleChangefeed TEvExternalStorage::TEvHeadObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "HeadObject")) {
            return;
        }

        Y_ABORT_UNLESS(IndexDownloadedChangefeed < ChangefeedsPrefixes.size());
        GetObject(ChangefeedDescriptionKeyFromSettings(*ImportInfo, ItemIdx, ChangefeedsPrefixes[IndexDownloadedChangefeed]), result.GetResult().GetContentLength());
    }

    void HandleTopic(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandleTopic TEvExternalStorage::TEvHeadObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "HeadObject")) {
            return;
        }

        Y_ABORT_UNLESS(IndexDownloadedChangefeed < ChangefeedsPrefixes.size());
        GetObject(TopicDescriptionKeyFromSettings(*ImportInfo, ItemIdx, ChangefeedsPrefixes[IndexDownloadedChangefeed]), result.GetResult().GetContentLength());
    }

    void HandleMetadata(TEvExternalStorage::TEvGetObjectResponse::TPtr& ev) {
        const auto& msg = *ev->Get();
        const auto& result = msg.Result;

        LOG_D("HandleMetadata TEvExternalStorage::TEvGetObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "GetObject")) {
            return;
        }

        TString content;
        if (!MaybeDecrypt(msg.Body, content, NBackup::EBackupFileType::Metadata)) {
            return;
        }

        Y_ABORT_UNLESS(ItemIdx < ImportInfo->Items.size());
        auto& item = ImportInfo->Items.at(ItemIdx);

        LOG_T("Trying to parse metadata"
            << ": self# " << SelfId()
            << ", body# " << SubstGlobalCopy(content, "\n", "\\n"));
        try {
            item.Metadata = NBackup::TMetadata::Deserialize(content);
        } catch (const std::exception& e) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Failed to parse metadata: " << e.what());
        }

        if (item.Metadata.HasVersion() && item.Metadata.GetVersion() == 0) {
            NeedValidateChecksums = false;
        }
        if (item.Metadata.HasEnablePermissions() && !item.Metadata.GetEnablePermissions()) {
            NeedDownloadPermissions = false;
        }

        auto nextStep = [this]() {
            StartDownloadingScheme();
        };

        if (NeedValidateChecksums) {
            StartValidatingChecksum(MetadataKey, content, nextStep);
        } else {
            nextStep();
        }
    }

    void HandleScheme(TEvExternalStorage::TEvGetObjectResponse::TPtr& ev) {
        const auto& msg = *ev->Get();
        const auto& result = msg.Result;

        LOG_D("HandleScheme TEvExternalStorage::TEvGetObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "GetObject")) {
            return;
        }

        TString content;
        if (!MaybeDecrypt(msg.Body, content, SchemeFileType)) {
            return;
        }

        Y_ABORT_UNLESS(ItemIdx < ImportInfo->Items.size());
        auto& item = ImportInfo->Items.at(ItemIdx);

        LOG_T("Trying to parse scheme"
            << ": self# " << SelfId()
            << ", itemIdx# " << ItemIdx
            << ", schemeKey# " << SchemeKey
            << ", body# " << SubstGlobalCopy(content, "\n", "\\n"));

        if (IsCreatedByQuery(SchemeKey)) {
            item.CreationQuery = content;
        } else if (IsTopic(SchemeKey)) {
            Ydb::Topic::CreateTopicRequest request;
            if (!google::protobuf::TextFormat::ParseFromString(content, &request)) {
                return Reply(Ydb::StatusIds::BAD_REQUEST, "Cannot parse topic scheme");
            }
            item.Topic = request;
        } else if (IsSysView(SchemeKey)) {
            Ydb::Table::DescribeSystemViewResult sysView;
            if (!google::protobuf::TextFormat::ParseFromString(content, &sysView)) {
                return Reply(Ydb::StatusIds::BAD_REQUEST, "Cannot parse system view description");
            }
            item.SysView = sysView;
        } else if (IsTable(SchemeKey)) {
            Ydb::Table::CreateTableRequest request;
            if (!google::protobuf::TextFormat::ParseFromString(content, &request)) {
                return Reply(Ydb::StatusIds::BAD_REQUEST, "Cannot parse scheme");
            }
            item.Table = request;
        } else {
            return Reply(Ydb::StatusIds::BAD_REQUEST, "Unsupported scheme object type");
        }

        auto nextStep = [this]() {
            if (NeedDownloadPermissions) {
                StartDownloadingPermissions();
            } else {
                StartCheckingMaterializedIndexes();
            }
        };

        if (NeedValidateChecksums) {
            StartValidatingChecksum(SchemeKey, content, nextStep);
        } else {
            nextStep();
        }
    }

    void HandlePermissions(TEvExternalStorage::TEvGetObjectResponse::TPtr& ev) {
        const auto& msg = *ev->Get();
        const auto& result = msg.Result;

        LOG_D("HandlePermissions TEvExternalStorage::TEvGetObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "GetObject")) {
            return;
        }

        TString content;
        if (!MaybeDecrypt(msg.Body, content, NBackup::EBackupFileType::Permissions)) {
            return;
        }

        Y_ABORT_UNLESS(ItemIdx < ImportInfo->Items.size());
        auto& item = ImportInfo->Items.at(ItemIdx);

        LOG_T("Trying to parse permissions"
            << ": self# " << SelfId()
            << ", body# " << SubstGlobalCopy(content, "\n", "\\n"));

        Ydb::Scheme::ModifyPermissionsRequest permissions;
        if (!google::protobuf::TextFormat::ParseFromString(content, &permissions)) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, "Cannot parse permissions");
        }
        item.Permissions = std::move(permissions);

        auto nextStep = [this]() {
            StartCheckingMaterializedIndexes();
        };

        if (NeedValidateChecksums) {
            StartValidatingChecksum(PermissionsKey, content, nextStep);
        } else {
            nextStep();
        }
    }

    void HandleIndex(TEvExternalStorage::TEvGetObjectResponse::TPtr& ev) {
        const auto& msg = *ev->Get();
        const auto& result = msg.Result;

        LOG_D("HandleIndex TEvExternalStorage::TEvGetObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "GetObject")) {
            return;
        }

        TString content;
        if (!MaybeDecrypt(msg.Body, content, NBackup::EBackupFileType::TableSchema, IndexCheckedMaterializedIndexImplTable)) {
            return;
        }

        Y_ABORT_UNLESS(ItemIdx < ImportInfo->Items.size());
        auto& item = ImportInfo->Items.at(ItemIdx);

        LOG_T("Trying to parse index"
            << ": self# " << SelfId()
            << ", body# " << SubstGlobalCopy(content, "\n", "\\n"));

        Ydb::Table::CreateTableRequest request;
        if (!google::protobuf::TextFormat::ParseFromString(content, &request)) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, "Cannot parse index");
        }

        Y_ABORT_UNLESS(IndexCheckedMaterializedIndexImplTable < IndexImplTablePrefixes.size());
        const auto& indexImplTablePrefix = IndexImplTablePrefixes[IndexCheckedMaterializedIndexImplTable];
        item.MaterializedIndexes.emplace_back(indexImplTablePrefix, std::move(request));

        auto nextStep = [this]() {
            if (++IndexCheckedMaterializedIndexImplTable >= IndexImplTablePrefixes.size()) {
                StartDownloadingChangefeeds();
            } else {
                Become(&TThis::StateCheckIndexes);
                HeadObject(MaterializedIndexSchemeKeyFromSettings(*ImportInfo, ItemIdx,
                    IndexImplTablePrefixes[IndexCheckedMaterializedIndexImplTable].ExportPrefix));
            }
        };

        if (NeedValidateChecksums) {
            StartValidatingChecksum(MaterializedIndexSchemeKeyFromSettings(*ImportInfo, ItemIdx,
                IndexImplTablePrefixes[IndexCheckedMaterializedIndexImplTable].ExportPrefix), content, nextStep);
        } else {
            nextStep();
        }
    }

    void HandleChangefeed(TEvExternalStorage::TEvGetObjectResponse::TPtr& ev) {
        const auto& msg = *ev->Get();
        const auto& result = msg.Result;

        LOG_D("HandleChangefeed TEvExternalStorage::TEvGetObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "GetObject")) {
            return;
        }

        TString content;
        if (!MaybeDecrypt(msg.Body, content, NBackup::EBackupFileType::TableChangefeed, IndexDownloadedChangefeed)) {
            return;
        }

        Y_ABORT_UNLESS(ItemIdx < ImportInfo->Items.size());
        auto& item = ImportInfo->Items.at(ItemIdx);

        LOG_T("Trying to parse changefeed"
            << ": self# " << SelfId()
            << ", body# " << SubstGlobalCopy(content, "\n", "\\n"));

        Ydb::Table::ChangefeedDescription changefeed;
        if (!google::protobuf::TextFormat::ParseFromString(content, &changefeed)) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, "Cannot parse changefeed");
        }

        *item.Changefeeds.MutableChangefeeds(IndexDownloadedChangefeed)->MutableChangefeed() = std::move(changefeed);

        auto nextStep = [this]() {
            Become(&TThis::StateDownloadTopics);
            HeadObject(TopicDescriptionKeyFromSettings(*ImportInfo, ItemIdx, ChangefeedsPrefixes[IndexDownloadedChangefeed]));
        };

        if (NeedValidateChecksums) {
            StartValidatingChecksum(ChangefeedDescriptionKeyFromSettings(*ImportInfo, ItemIdx, ChangefeedsPrefixes[IndexDownloadedChangefeed]), content, nextStep);
        } else {
            nextStep();
        }
    }

    void HandleTopic(TEvExternalStorage::TEvGetObjectResponse::TPtr& ev) {
        const auto& msg = *ev->Get();
        const auto& result = msg.Result;

        LOG_D("HandleTopic TEvExternalStorage::TEvGetObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "GetObject")) {
            return;
        }

        TString content;
        if (!MaybeDecrypt(msg.Body, content, NBackup::EBackupFileType::TableTopic, IndexDownloadedChangefeed)) {
            return;
        }

        Y_ABORT_UNLESS(ItemIdx < ImportInfo->Items.size());
        auto& item = ImportInfo->Items.at(ItemIdx);

        LOG_T("Trying to parse topic"
            << ": self# " << SelfId()
            << ", body# " << SubstGlobalCopy(content, "\n", "\\n"));

        Ydb::Topic::DescribeTopicResult topic;
        if (!google::protobuf::TextFormat::ParseFromString(content, &topic)) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, "Cannot parse topic");
        }
        *item.Changefeeds.MutableChangefeeds(IndexDownloadedChangefeed)->MutableTopic() = std::move(topic);

        auto nextStep = [this]() {
            if (++IndexDownloadedChangefeed >= ChangefeedsPrefixes.size()) {
                Reply();
            } else {
                Become(&TThis::StateDownloadChangefeeds);
                HeadObject(ChangefeedDescriptionKeyFromSettings(*ImportInfo, ItemIdx, ChangefeedsPrefixes[IndexDownloadedChangefeed]));
            }
        };

        if (NeedValidateChecksums) {
            StartValidatingChecksum(TopicDescriptionKeyFromSettings(*ImportInfo, ItemIdx, ChangefeedsPrefixes[IndexDownloadedChangefeed]), content, nextStep);
        } else {
            nextStep();
        }
    }

    template <typename T>
    static void Resize(::google::protobuf::RepeatedPtrField<T>* repeatedField, ui64 size) {
        while (size--) repeatedField->Add();
    }

    void HandleChangefeeds(TEvExternalStorage::TEvListObjectsResponse::TPtr& ev) {
        const auto& result = ev.Get()->Get()->Result;
        LOG_D("HandleChangefeeds TEvExternalStorage::TEvListObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "ListObjects")) {
            return;
        }

        const auto& objects = result.GetResult().GetContents();
        ChangefeedsPrefixes.clear();
        ChangefeedsPrefixes.reserve(objects.size());

        for (const auto& obj : objects) {
            const TFsPath path = obj.GetKey();
            if (path.GetName() == "changefeed_description.pb") {
                ChangefeedsPrefixes.push_back(path.Parent().GetName());
            }
        }

        DownloadChangefeedsData();
    }

    void Reply(Ydb::StatusIds::StatusCode statusCode = Ydb::StatusIds::SUCCESS, const TString& error = TString()) override {
        const bool success = (statusCode == Ydb::StatusIds::SUCCESS);
        LOG_I("Reply"
            << ": self# " << SelfId()
            << ", success# " << success
            << ", error# " << error);

        Send(ReplyTo, new TEvPrivate::TEvImportSchemeReady(ImportInfo->Id, ItemIdx, success, error));
        PassAway();
    }

    void ListChangefeeds() {
        CreateClient();
        ListObjects(ImportInfo->GetItemSrcPrefix(ItemIdx) + "/");
    }

    void DownloadMetadata() {
        Download(MetadataKey);
    }

    void DownloadScheme() {
        Download(SchemeKey);
    }

    void DownloadPermissions() {
        Download(PermissionsKey);
    }

    static bool NeedToCheckMaterializedIndexes(Ydb::Import::ImportFromS3Settings::IndexPopulationMode mode) {
        switch (mode) {
        case Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_IMPORT:
        case Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_AUTO:
            return true;
        default:
            return false;
        }
    }

    void CheckMaterializedIndexes() {
        Become(&TThis::StateCheckIndexes);

        Y_ABORT_UNLESS(ItemIdx < ImportInfo->Items.size());
        auto& item = ImportInfo->Items.at(ItemIdx);

        IndexImplTablePrefixes.clear();
        IndexCheckedMaterializedIndexImplTable = 0;
        item.MaterializedIndexes.clear();

        if (const auto& indexes = item.Metadata.GetIndexes()) {
            IndexImplTablePrefixes.reserve(indexes->size());
            for (const auto& index : *indexes) {
                IndexImplTablePrefixes.push_back(index);
            }

            DownloadMaterializedIndexes();
        } else {
            if (!Key) { // not encrypted
                if (item.Table) {
                    IndexImplTablePrefixes.reserve(item.Table->indexes_size());

                    for (const auto& index : item.Table->indexes()) {
                        const auto indexType = NTableIndex::TryConvertIndexType(index.type_case());
                        if (!indexType) {
                            return Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Unsupported index"
                                << ": name# " << index.name()
                                << ": type# " << static_cast<int>(index.type_case()));
                        }

                        const TVector<TString> indexColumns(index.index_columns().begin(), index.index_columns().end());

                        for (const auto& implTable : NTableIndex::GetImplTables(*indexType, indexColumns)) {
                            const TString implTablePrefix = TStringBuilder() << index.name() << "/" << implTable;
                            IndexImplTablePrefixes.push_back({implTablePrefix, implTablePrefix});
                        }
                    }
                }

                DownloadMaterializedIndexes();
            } else {
                Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "No indexes described in table metadata");
            }
        }
    }

    void DownloadMaterializedIndexes() {
        if (!IndexImplTablePrefixes.empty()) {
            Y_ABORT_UNLESS(IndexCheckedMaterializedIndexImplTable < IndexImplTablePrefixes.size());
            HeadObject(MaterializedIndexSchemeKeyFromSettings(*ImportInfo, ItemIdx,
                IndexImplTablePrefixes[IndexCheckedMaterializedIndexImplTable].ExportPrefix));
        } else {
            StartDownloadingChangefeeds();
        }
    }

    void DownloadChangefeeds() {
        Become(&TThis::StateDownloadChangefeeds);
        if (const auto& maybeChangefeeds = ImportInfo->Items[ItemIdx].Metadata.GetChangefeeds()) {
            ChangefeedsPrefixes.clear();
            ChangefeedsPrefixes.reserve(maybeChangefeeds->size());
            for (const auto& changefeed : *maybeChangefeeds) {
                ChangefeedsPrefixes.push_back(changefeed.ExportPrefix);
            }

            DownloadChangefeedsData();
        } else {
            if (!Key) { // not encrypted
                ListChangefeeds();
            } else {
                // We don't rely on S3 listing in case of encryption
                Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "No changefeeds described in table metadata");
            }
        }
    }

    void DownloadChangefeedsData() {
        if (!ChangefeedsPrefixes.empty()) {
            auto& item = ImportInfo->Items.at(ItemIdx);
            Resize(item.Changefeeds.MutableChangefeeds(), ChangefeedsPrefixes.size());

            Y_ABORT_UNLESS(IndexDownloadedChangefeed < ChangefeedsPrefixes.size());
            HeadObject(ChangefeedDescriptionKeyFromSettings(*ImportInfo, ItemIdx, ChangefeedsPrefixes[IndexDownloadedChangefeed]));
        } else {
            Reply();
        }
    }

    void StartDownloadingScheme() {
        ResetRetries();
        DownloadScheme();
        Become(&TThis::StateDownloadScheme);
    }

    void StartDownloadingPermissions() {
        ResetRetries();
        DownloadPermissions();
        Become(&TThis::StateDownloadPermissions);
    }

    void StartCheckingMaterializedIndexes() {
        ResetRetries();
        if (NeedToCheckMaterializedIndexes(IndexPopulationMode)) {
            CheckMaterializedIndexes();
        } else {
            StartDownloadingChangefeeds();
        }
    }

    void StartDownloadingChangefeeds() {
        ResetRetries();
        DownloadChangefeeds();
    }

public:
    explicit TSchemeGetter(const TActorId& replyTo, TImportInfo::TPtr importInfo, ui32 itemIdx, TMaybe<NBackup::TEncryptionIV> iv)
        : TGetterFromS3<TSchemeGetter>(TGetterSettings::FromImportInfo(importInfo, std::move(iv)))
        , ImportInfo(std::move(importInfo))
        , ReplyTo(replyTo)
        , ItemIdx(itemIdx)
        , MetadataKey(MetadataKeyFromSettings(*ImportInfo, itemIdx))
        , SchemeKey(SchemeKeyFromSettings(*ImportInfo, itemIdx, "scheme.pb"))
        , PermissionsKey(PermissionsKeyFromSettings(*ImportInfo, itemIdx))
        , IndexPopulationMode(ImportInfo->Kind == TImportInfo::EKind::S3 ? ImportInfo->GetS3Settings().index_population_mode() : Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_UNSPECIFIED)
        , NeedDownloadPermissions(!ImportInfo->GetNoAcl())
        , NeedValidateChecksums(!ImportInfo->GetSkipChecksumValidation())
    {
    }

    void Bootstrap() {
        DownloadMetadata();
        Become(&TThis::StateDownloadMetadata);
    }

    STATEFN(StateDownloadMetadata) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvHeadObjectResponse, HandleMetadata);
            hFunc(TEvExternalStorage::TEvGetObjectResponse, HandleMetadata);

            sFunc(TEvents::TEvWakeup, DownloadMetadata);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    STATEFN(StateDownloadScheme) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvHeadObjectResponse, HandleScheme);
            hFunc(TEvExternalStorage::TEvGetObjectResponse, HandleScheme);

            sFunc(TEvents::TEvWakeup, DownloadScheme);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    STATEFN(StateDownloadPermissions) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvHeadObjectResponse, HandlePermissions);
            hFunc(TEvExternalStorage::TEvGetObjectResponse, HandlePermissions);

            sFunc(TEvents::TEvWakeup, DownloadPermissions);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    STATEFN(StateCheckIndexes) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvHeadObjectResponse, HandleIndex);
            hFunc(TEvExternalStorage::TEvGetObjectResponse, HandleIndex);

            sFunc(TEvents::TEvWakeup, CheckMaterializedIndexes);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    STATEFN(StateDownloadChangefeeds) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvListObjectsResponse, HandleChangefeeds);
            hFunc(TEvExternalStorage::TEvHeadObjectResponse, HandleChangefeed);
            hFunc(TEvExternalStorage::TEvGetObjectResponse, HandleChangefeed);

            sFunc(TEvents::TEvWakeup, DownloadChangefeeds);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    STATEFN(StateDownloadTopics) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvHeadObjectResponse, HandleTopic);
            hFunc(TEvExternalStorage::TEvGetObjectResponse, HandleTopic);

            sFunc(TEvents::TEvWakeup, DownloadChangefeeds);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

private:
    TImportInfo::TPtr ImportInfo;
    const TActorId ReplyTo;
    const ui32 ItemIdx;

    const TString MetadataKey;
    TString SchemeKey;
    NBackup::EBackupFileType SchemeFileType = NBackup::EBackupFileType::TableSchema;
    const TString PermissionsKey;
    ui32 SchemePropertiesIdx = 0;

    TVector<TString> ChangefeedsPrefixes;
    ui64 IndexDownloadedChangefeed = 0;

    TVector<NBackup::TIndexMetadata> IndexImplTablePrefixes;
    ui64 IndexCheckedMaterializedIndexImplTable = 0;
    Ydb::Import::ImportFromS3Settings::IndexPopulationMode IndexPopulationMode;

    bool NeedDownloadPermissions = true;
    bool NeedValidateChecksums = true;

}; // TSchemeGetter

class TSchemaMappingGetter : public TGetterFromS3<TSchemaMappingGetter> {
    static TString MetadataKeyFromSettings(const TImportInfo& importInfo) {
        return TStringBuilder() << importInfo.GetSource() << "/metadata.json";
    }

    static TString SchemaMappingKeyFromSettings(const TImportInfo& importInfo) {
        return TStringBuilder() << importInfo.GetSource() << "/SchemaMapping/mapping.json";
    }

    static TString SchemaMappingMetadataKeyFromSettings(const TImportInfo& importInfo) {
        return TStringBuilder() << importInfo.GetSource() << "/SchemaMapping/metadata.json";
    }

    void HandleMetadata(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandleMetadata TEvExternalStorage::TEvHeadObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "HeadObject")) {
            return;
        }

        GetObject(MetadataKey, result.GetResult().GetContentLength(), false);
    }

    void HandleSchemaMappingMetadata(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandleSchemaMappingMetadata TEvExternalStorage::TEvHeadObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "HeadObject")) {
            return;
        }

        GetObject(SchemaMappingMetadataKey, result.GetResult().GetContentLength());
    }

    void HandleSchemaMapping(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandleSchemaMapping TEvExternalStorage::TEvHeadObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "HeadObject")) {
            return;
        }

        GetObject(SchemaMappingKey, result.GetResult().GetContentLength());
    }

    void HandleMetadata(TEvExternalStorage::TEvGetObjectResponse::TPtr& ev) {
        const auto& msg = *ev->Get();
        const auto& result = msg.Result;

        LOG_D("HandleMetadata TEvExternalStorage::TEvGetObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "GetObject")) {
            return;
        }

        TString content = msg.Body;
        LOG_T("Trying to parse metadata"
            << ": self# " << SelfId()
            << ", body# " << SubstGlobalCopy(content, "\n", "\\n"));

        if (!ProcessMetadata(content)) {
            return;
        }

        auto nextStep = [this]() {
            StartDownloadingSchemaMappingMetadata();
        };

        if (NeedValidateChecksums) {
            StartValidatingChecksum(MetadataKey, content, nextStep);
        } else {
            nextStep();
        }
    }

    void HandleSchemaMappingMetadata(TEvExternalStorage::TEvGetObjectResponse::TPtr& ev) {
        const auto& msg = *ev->Get();
        const auto& result = msg.Result;

        LOG_D("HandleSchemaMappingMetadata TEvExternalStorage::TEvGetObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "GetObject")) {
            return;
        }

        TString content;
        if (!MaybeDecryptAndSaveIV(msg.Body, content)) {
            return;
        }
        ImportInfo->ExportIV = IV;

        LOG_T("Trying to parse schema mapping metadata"
            << ": self# " << SelfId()
            << ", body# " << SubstGlobalCopy(content, "\n", "\\n"));

        if (!ProcessSchemaMappingMetadata(content)) {
            return;
        }

        auto nextStep = [this]() {
            StartDownloadingSchemaMapping();
        };

        if (NeedValidateChecksums) {
            StartValidatingChecksum(SchemaMappingMetadataKey, content, nextStep);
        } else {
            nextStep();
        }
    }

    void HandleSchemaMapping(TEvExternalStorage::TEvGetObjectResponse::TPtr& ev) {
        const auto& msg = *ev->Get();
        const auto& result = msg.Result;

        LOG_D("HandleSchemaMapping TEvExternalStorage::TEvGetObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "GetObject")) {
            return;
        }

        TString content;
        if (!MaybeDecrypt(msg.Body, content, NBackup::EBackupFileType::SchemaMapping)) {
            return;
        }

        LOG_T("Trying to parse scheme"
            << ": self# " << SelfId()
            << ", schemaMappingKey# " << SchemaMappingKey
            << ", body# " << SubstGlobalCopy(content, "\n", "\\n"));

        ImportInfo->SchemaMapping.ConstructInPlace();
        TString error;
        if (!ImportInfo->SchemaMapping->Deserialize(content, error)) {
            Reply(Ydb::StatusIds::BAD_REQUEST, error);
            return;
        }

        auto nextStep = [this]() {
            Reply();
        };

        if (NeedValidateChecksums) {
            StartValidatingChecksum(SchemaMappingKey, content, nextStep);
        } else {
            nextStep();
        }
    }

    void Reply(Ydb::StatusIds::StatusCode statusCode = Ydb::StatusIds::SUCCESS, const TString& error = TString()) override {
        const bool success = (statusCode == Ydb::StatusIds::SUCCESS);
        LOG_I("Reply"
            << ": self# " << SelfId()
            << ", success# " << success
            << ", error# " << error);

        Send(ReplyTo, new TEvPrivate::TEvImportSchemaMappingReady(ImportInfo->Id, success, error));
        PassAway();
    }

    void DownloadMetadata() {
        Download(MetadataKey, false);
    }

    void DownloadSchemaMappingMetadata() {
        Download(SchemaMappingMetadataKey);
    }

    void DownloadSchemaMapping() {
        Download(SchemaMappingKey);
    }

    void StartDownloadingSchemaMappingMetadata() {
        ResetRetries();
        DownloadSchemaMappingMetadata();
        Become(&TThis::StateDownloadSchemaMappingMetadata);
    }

    void StartDownloadingSchemaMapping() {
        ResetRetries();
        DownloadSchemaMapping();
        Become(&TThis::StateDownloadSchemaMapping);
    }

    bool ProcessMetadata(const TString& content) {
        NJson::TJsonValue json;
        if (!NJson::ReadJsonTree(content, &json)) {
            Reply(Ydb::StatusIds::BAD_REQUEST, "Failed to parse metadata json");
            return false;
        }
        const NJson::TJsonValue& kind = json["kind"];
        if (kind.GetString() != "SimpleExportV0") {
            Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Unknown kind of metadata json: " << kind.GetString());
            return false;
        }
        const NJson::TJsonValue& checksum = json["checksum"];
        if (!checksum.IsDefined()) {
            NeedValidateChecksums = false; // No checksums in export
        } else if (checksum.GetString() != "sha256") {
            Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Unknown checksum type: " << checksum.GetString());
            return false;
        }
        return true;
    }

    bool ProcessSchemaMappingMetadata(const TString& content) {
        NJson::TJsonValue json;
        if (!NJson::ReadJsonTree(content, &json)) {
            Reply(Ydb::StatusIds::BAD_REQUEST, "Failed to parse schema mapping metadata json");
            return false;
        }
        const NJson::TJsonValue& kind = json["kind"];
        if (kind.GetString() != "SchemaMappingV0") {
            Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Unknown kind of schema mapping metadata json: " << kind.GetString());
            return false;
        }
        return true;
    }

public:
    TSchemaMappingGetter(const TActorId& replyTo, TImportInfo::TPtr importInfo)
        : TGetterFromS3<TSchemaMappingGetter>(TGetterSettings::FromImportInfo(importInfo, Nothing()))
        , ImportInfo(std::move(importInfo))
        , ReplyTo(replyTo)
        , MetadataKey(MetadataKeyFromSettings(*ImportInfo))
        , SchemaMappingMetadataKey(SchemaMappingMetadataKeyFromSettings(*ImportInfo))
        , SchemaMappingKey(SchemaMappingKeyFromSettings(*ImportInfo))
    {
    }

    void Bootstrap() {
        DownloadMetadata();
        Become(&TThis::StateDownloadMetadata);
    }

    STATEFN(StateDownloadMetadata) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvHeadObjectResponse, HandleMetadata);
            hFunc(TEvExternalStorage::TEvGetObjectResponse, HandleMetadata);

            sFunc(TEvents::TEvWakeup, DownloadMetadata);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    STATEFN(StateDownloadSchemaMappingMetadata) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvHeadObjectResponse, HandleSchemaMappingMetadata);
            hFunc(TEvExternalStorage::TEvGetObjectResponse, HandleSchemaMappingMetadata);

            sFunc(TEvents::TEvWakeup, DownloadSchemaMappingMetadata);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    STATEFN(StateDownloadSchemaMapping) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvHeadObjectResponse, HandleSchemaMapping);
            hFunc(TEvExternalStorage::TEvGetObjectResponse, HandleSchemaMapping);

            sFunc(TEvents::TEvWakeup, DownloadSchemaMapping);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

private:
    TImportInfo::TPtr ImportInfo;
    bool NeedValidateChecksums = true;
    const TActorId ReplyTo;
    const TString MetadataKey;
    const TString SchemaMappingMetadataKey;
    const TString SchemaMappingKey;
}; // TSchemaMappingGetter

class TListObjectsInS3ExportGetter : public TGetterFromS3<TListObjectsInS3ExportGetter> {
    class TPathFilter {
    public:
        void Build(const Ydb::Import::ListObjectsInS3ExportSettings& settings) {
            for (const auto& item : settings.items()) {
                TString path = NBackup::NormalizeItemPath(item.path());
                if (path) {
                    Paths.emplace(path);
                    PathPrefixes.emplace_back(path + "/");
                }
            }
        }

        bool Match(const TString& path) const {
            if (Paths.empty()) {
                return true;
            }

            if (Paths.contains(path)) {
                return true;
            }

            for (const TString& prefix : PathPrefixes) {
                if (path.StartsWith(prefix)) { // So this path is contained in a directory that is specified by user in request
                    return true;
                }
            }

            return false;
        }

    private:
        std::vector<TString> PathPrefixes;
        THashSet<TString> Paths;
    };
public:
    TListObjectsInS3ExportGetter(TEvImport::TEvListObjectsInS3ExportRequest::TPtr&& ev)
        : TGetterFromS3<TListObjectsInS3ExportGetter>(TGetterSettings::FromRequest(ev))
        , Request(std::move(ev))
    {
    }

    void Bootstrap() {
        if (ParseParameters()) {
            CreateClient();
            DownloadSchemaMapping();
        }
    }

    bool ParseParameters() {
        const auto& req = Request->Get()->Record;
        if (req.GetPageSize() < 0) {
            Reply(Ydb::StatusIds::BAD_REQUEST, "Page size should be greater than or equal to 0");
            return false;
        }
        PageSize = static_cast<size_t>(req.GetPageSize());
        if (req.GetPageToken()) {
            if (!TryFromString(req.GetPageToken(), StartPos)) {
                Reply(Ydb::StatusIds::BAD_REQUEST, "Failed to parse page token");
                return false;
            }
            if (req.GetPageSize() == 0) {
                Reply(Ydb::StatusIds::BAD_REQUEST, "Page size should be greater than 0");
                return false;
            }
        }
        const auto& settings = req.GetSettings();
        if (NBackup::NormalizeExportPrefix(settings.prefix()).empty()) {
            Reply(Ydb::StatusIds::BAD_REQUEST, "Empty S3 prefix specified");
            return false;
        }

        try {
            ExcludeRegexps = NBackup::CombineRegexps(settings.exclude_regexps());
        } catch (const std::exception& ex) {
            Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Invalid regexp: " << ex.what());
            return false;
        }
        return true;
    }

    void DownloadSchemaMapping() {
        ResetRetries();
        Download(GetSchemaMappingKey());
        Become(&TThis::StateDownloadSchemaMapping);
    }

    void HandleSchemaMapping(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandleSchemaMapping TEvExternalStorage::TEvHeadObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (IsNoSuchKeyError(result)) {
            return ListObjectsInS3Prefix();
        }

        if (!CheckResult(result, "HeadObject")) {
            return;
        }

        GetObject(GetSchemaMappingKey(), result.GetResult().GetContentLength());
    }

    void HandleSchemaMapping(TEvExternalStorage::TEvGetObjectResponse::TPtr& ev) {
        const auto& msg = *ev->Get();
        const auto& result = msg.Result;

        LOG_D("HandleSchemaMapping TEvExternalStorage::TEvGetObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (IsNoSuchKeyError(result)) {
            return ListObjectsInS3Prefix();
        }

        if (!CheckResult(result, "GetObject")) {
            return;
        }

        TString content;
        if (!MaybeDecryptAndSaveIV(msg.Body, content)) {
            return;
        }

        LOG_T("Trying to parse schema mapping"
            << ": self# " << SelfId()
            << ", schemaMappingKey# " << GetSchemaMappingKey()
            << ", body# " << SubstGlobalCopy(content, "\n", "\\n"));

        TString error;
        NBackup::TSchemaMapping schemaMapping;
        if (!schemaMapping.Deserialize(content, error)) {
            Reply(Ydb::StatusIds::BAD_REQUEST, error);
            return;
        }

        ProcessItemsAndReply(std::move(schemaMapping.Items));
    }

    void ListObjectsInS3Prefix() {
        ResetRetries();
        ListObjects(GetExportPrefix());
        Become(&TThis::StateListObjects);
    }

    void HandleListObjects(TEvExternalStorage::TEvListObjectsResponse::TPtr& ev) {
        const auto& result = ev.Get()->Get()->Result;
        LOG_D("HandleListObjects TEvExternalStorage::TEvListObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "ListObjects")) {
            return;
        }

        const auto& objects = result.GetResult().GetContents();
        const TString prefix = GetExportPrefix();
        const TString suffix = TString("/metadata.json") + (Key ? ".enc" : "");
        std::vector<NBackup::TSchemaMapping::TItem> items;
        items.reserve(objects.size());
        for (const auto& obj : objects) {
            TStringBuf key(obj.GetKey());
            // Skip prefix
            // Prefix also may be added with the bucket name here, so cut bucket name also
            size_t prefixPos = key.find(prefix);
            if (prefixPos == TStringBuf::npos) {
                LOG_D("Unexpected key found: " << key);
                continue;
            }
            key = key.SubString(prefixPos + prefix.size(), TStringBuf::npos);

            // Every backup object has metadata.json.
            // Process only keys with metadata.json suffix.
            if (key.ChopSuffix(suffix)) {
                TString keyStr(key);
                items.emplace_back(NBackup::TSchemaMapping::TItem{
                    .ExportPrefix = keyStr,
                    .ObjectPath = keyStr,
                });
            }
        }

        ProcessItemsAndReply(std::move(items));
    }

    STATEFN(StateDownloadSchemaMapping) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvHeadObjectResponse, HandleSchemaMapping);
            hFunc(TEvExternalStorage::TEvGetObjectResponse, HandleSchemaMapping);

            sFunc(TEvents::TEvWakeup, DownloadSchemaMapping);
        }
    }

    STATEFN(StateListObjects) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvListObjectsResponse, HandleListObjects);

            sFunc(TEvents::TEvWakeup, ListObjectsInS3Prefix);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    void Reply(Ydb::StatusIds::StatusCode statusCode = Ydb::StatusIds::SUCCESS, const TString& error = TString()) override {
        LOG_I("Reply"
            << ": self# " << SelfId()
            << ", status# " << static_cast<int>(statusCode)
            << ", error# " << error);

        auto result = MakeHolder<TEvImport::TEvListObjectsInS3ExportResponse>();
        result->Record.set_status(statusCode);
        if (error) {
            result->Record.add_issues()->set_message(error);
        }
        if (statusCode == Ydb::StatusIds::SUCCESS) {
            result->Record.mutable_result()->Swap(&Result);
        }
        Send(Request->Sender, std::move(result));
        PassAway();
    }

    TString GetSchemaMappingKey() const {
        return TStringBuilder() << NBackup::NormalizeExportPrefix(Request->Get()->Record.GetSettings().prefix()) << "/SchemaMapping/mapping.json";
    }

    TString GetExportPrefix() const {
        return NBackup::NormalizeExportPrefix(Request->Get()->Record.GetSettings().prefix()) + "/";
    }

    void ProcessItemsAndReply(std::vector<NBackup::TSchemaMapping::TItem>&& items) {
        std::sort(items.begin(), items.end(), [](const NBackup::TSchemaMapping::TItem& i1, const NBackup::TSchemaMapping::TItem& i2) {
            return i1.ObjectPath < i2.ObjectPath;
        });

        PathFilter.Build(Request->Get()->Record.GetSettings());

        size_t pos = 0;
        for (const auto& item : items) {
            if (!PathFilter.Match(item.ObjectPath)) {
                continue;
            }

            if (IsExcludedFromListing(item.ObjectPath)) {
                continue;
            }

            if (PageSize && pos >= StartPos + PageSize) { // Calc only items that suit filters
                NextPos = pos;
                break;
            }

            if (pos >= StartPos) {
                auto* result = Result.add_items();
                result->set_path(item.ObjectPath);
                result->set_prefix(item.ExportPrefix);
            }

            ++pos;
        }
        if (NextPos) {
            Result.set_next_page_token(ToString(NextPos));
        }
        Reply();
    }

    bool IsExcludedFromListing(const TString& path) const {
        for (const auto& regexp : ExcludeRegexps) {
            if (regexp.Match(path.c_str())) {
                return true;
            }
        }
        return false;
    }

private:
    TEvImport::TEvListObjectsInS3ExportRequest::TPtr Request;
    Ydb::Import::ListObjectsInS3ExportResult Result;
    size_t StartPos = 0;
    size_t PageSize = 0;
    size_t NextPos = 0;
    TPathFilter PathFilter;
    std::vector<TRegExMatch> ExcludeRegexps;
};

class TFSHelper {
public:
    static TString GetFullPath(const TString& basePath, const TString& relativePath) {
        if (basePath.empty()) {
            return TStringBuilder() << "/" << relativePath;
        }
        return TFsPath(basePath) / relativePath;
    }

    static bool ReadFile(const TString& path, TString& content, TString& error) {
        try {
            if (!NFs::Exists(path)) {
                error = TStringBuilder() << "File does not exist: " << path;
                return false;
            }

            TFileInput file(path);
            content = file.ReadAll();
            return true;
        } catch (const std::exception& e) {
            error = TStringBuilder() << "Failed to read file " << path << ": " << e.what();
            return false;
        }
    }
};

IActor* CreateSchemeGetter(const TActorId& replyTo, TImportInfo::TPtr importInfo, ui32 itemIdx, TMaybe<NBackup::TEncryptionIV> iv) {
    return new TSchemeGetter(replyTo, std::move(importInfo), itemIdx, std::move(iv));
}

IActor* CreateSchemaMappingGetter(const TActorId& replyTo, TImportInfo::TPtr importInfo) {
    return new TSchemaMappingGetter(replyTo, std::move(importInfo));
}

IActor* CreateListObjectsInS3ExportGetter(TEvImport::TEvListObjectsInS3ExportRequest::TPtr&& ev) {
    return new TListObjectsInS3ExportGetter(std::move(ev));
}

} // NSchemeShard
} // NKikimr
