#include "schemeshard_import_getters.h"
#include "schemeshard_import_helpers.h"
#include "schemeshard_private.h"

#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/wrappers/s3_storage_config.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/public/api/protos/ydb_import.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/lib/ydb_cli/dump/files/files.h>

#include <library/cpp/json/json_reader.h>

#include <google/protobuf/text_format.h>

#include <util/string/subst.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NWrappers;

using namespace Aws::Auth;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws;

static constexpr TDuration MaxDelay = TDuration::Minutes(10);

template <class TDerived>
class TGetterFromS3 : public TActorBootstrapped<TDerived> {
protected:
    explicit TGetterFromS3(TImportInfo::TPtr importInfo, TMaybe<NBackup::TEncryptionIV> iv)
        : ImportInfo(std::move(importInfo))
        , ExternalStorageConfig(new NWrappers::NExternalStorage::TS3ExternalStorageConfig(ImportInfo->Settings))
        , IV(std::move(iv))
        , Retries(ImportInfo->Settings.number_of_retries())
    {
        if (ImportInfo->Settings.has_encryption_settings()) {
            Key = NBackup::TEncryptionKey(ImportInfo->Settings.encryption_settings().symmetric_key().key());
        }
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
        Client = this->RegisterWithSameMailbox(CreateS3Wrapper(ExternalStorageConfig->ConstructStorageOperator()));
    }

    void PassAway() override {
        this->Send(Client, new TEvents::TEvPoisonPill());
        TActorBootstrapped<TDerived>::PassAway();
    }

    TString GetKey(TString key, bool autoAddEncSuffix = true) {
        if (autoAddEncSuffix && ImportInfo->Settings.has_encryption_settings()) {
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
        if (Attempt < Retries && error.ShouldRetry()) {
            Delay = Min(Delay * ++Attempt, MaxDelay);
            this->Schedule(Delay, new TEvents::TEvWakeup());
        } else {
            Reply(false, TStringBuilder() << "S3 error: " << error.GetMessage().c_str());
        }
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
                Reply(false, ex.what());
                return false;
            }
        }
        result = content;
        return true;
    }

    bool MaybeDecrypt(const TString& content, TString& result, NBackup::EBackupFileType fileType) {
        if (Key && IV) {
            try {
                NBackup::TEncryptionIV expectedIV = NBackup::TEncryptionIV::Combine(*IV, fileType, 0 /* already combined */, 0);
                auto buffer = NBackup::TEncryptedFileDeserializer::DecryptFullFile(*Key, expectedIV, TBuffer(content.data(), content.size()));
                result.assign(buffer.Data(), buffer.Size());
                return true;
            } catch (const std::exception& ex) {
                Reply(false, ex.what());
                return false;
            }
        }
        result = content;
        return true;
    }

    virtual void Reply(bool success = true, const TString& error = TString()) = 0;

protected:
    TImportInfo::TPtr ImportInfo;
    NWrappers::IExternalStorageConfig::TPtr ExternalStorageConfig;
    TActorId Client;
    TMaybe<NBackup::TEncryptionKey> Key;
    TMaybe<NBackup::TEncryptionIV> IV;

    const ui32 Retries;
    ui32 Attempt = 0;

    TDuration Delay = TDuration::Minutes(1);
};

// Downloads scheme-related objects from S3
class TSchemeGetter: public TGetterFromS3<TSchemeGetter> {
    static TString MetadataKeyFromSettings(const TImportInfo::TPtr& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        return TStringBuilder() << importInfo->GetItemSrcPrefix(itemIdx) << "/metadata.json";
    }

    static TString SchemeKeyFromSettings(const TImportInfo::TPtr& importInfo, ui32 itemIdx, TStringBuf filename) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        return TStringBuilder() << importInfo->GetItemSrcPrefix(itemIdx) << '/' << filename;
    }

    static TString PermissionsKeyFromSettings(const TImportInfo::TPtr& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        return TStringBuilder() << importInfo->GetItemSrcPrefix(itemIdx) << "/permissions.pb";
    }

    static TString ChangefeedDescriptionKeyFromSettings(const TImportInfo::TPtr& importInfo, ui32 itemIdx, const TString& changefeedName) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        return TStringBuilder() << importInfo->GetItemSrcPrefix(itemIdx) << "/" << changefeedName << "/changefeed_description.pb";
    }

    static TString TopicDescriptionKeyFromSettings(const TImportInfo::TPtr& importInfo, ui32 itemIdx, const TString& changefeedName) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        return TStringBuilder() << importInfo->GetItemSrcPrefix(itemIdx) << "/" << changefeedName << "/topic_description.pb";
    }

    static bool IsView(TStringBuf schemeKey) {
        return schemeKey.EndsWith(NYdb::NDump::NFiles::CreateView().FileName);
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

    void HandleScheme(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandleScheme TEvExternalStorage::TEvHeadObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!IsView(SchemeKey) && NoObjectFound(result.GetError().GetErrorType())) {
            // try search for a view
            SchemeKey = SchemeKeyFromSettings(ImportInfo, ItemIdx, NYdb::NDump::NFiles::CreateView().FileName);
            HeadObject(SchemeKey);
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
            StartDownloadingChangefeeds(); // permissions are optional
            return;
        } else if (!CheckResult(result, "HeadObject")) {
            return;
        }

        GetObject(PermissionsKey, result.GetResult().GetContentLength());
    }

    void HandleChecksum(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandleChecksum TEvExternalStorage::TEvHeadObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "HeadObject")) {
            return;
        }

        GetObject(NBackup::ChecksumKey(CurrentObjectKey), result.GetResult().GetContentLength(), false);
    }

    void HandleChangefeed(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandleChangefeed TEvExternalStorage::TEvHeadObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "HeadObject")) {
            return;
        }

        Y_ABORT_UNLESS(IndexDownloadedChangefeed < ChangefeedsNames.size());
        GetObject(ChangefeedDescriptionKeyFromSettings(ImportInfo, ItemIdx, ChangefeedsNames[IndexDownloadedChangefeed]), result.GetResult().GetContentLength());
    }

    void HandleTopic(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandleTopic TEvExternalStorage::TEvHeadObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "HeadObject")) {
            return;
        }

        Y_ABORT_UNLESS(IndexDownloadedChangefeed < ChangefeedsNames.size());
        GetObject(TopicDescriptionKeyFromSettings(ImportInfo, ItemIdx, ChangefeedsNames[IndexDownloadedChangefeed]), result.GetResult().GetContentLength());
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

        item.Metadata = NBackup::TMetadata::Deserialize(content);

        if (item.Metadata.HasVersion() && item.Metadata.GetVersion() == 0) {
            NeedValidateChecksums = false;
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
        if (!MaybeDecrypt(msg.Body, content, NBackup::EBackupFileType::TableSchema)) {
            return;
        }

        Y_ABORT_UNLESS(ItemIdx < ImportInfo->Items.size());
        auto& item = ImportInfo->Items.at(ItemIdx);

        LOG_T("Trying to parse scheme"
            << ": self# " << SelfId()
            << ", itemIdx# " << ItemIdx
            << ", schemeKey# " << SchemeKey
            << ", body# " << SubstGlobalCopy(content, "\n", "\\n"));

        if (IsView(SchemeKey)) {
            item.CreationQuery = content;
        } else if (!google::protobuf::TextFormat::ParseFromString(content, &item.Scheme)) {
            return Reply(false, "Cannot parse scheme");
        }

        auto nextStep = [this]() {
            if (NeedDownloadPermissions) {
                StartDownloadingPermissions();
            } else {
                StartDownloadingChangefeeds();
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
            return Reply(false, "Cannot parse permissions");
        }
        item.Permissions = std::move(permissions);

        auto nextStep = [this]() {
            StartDownloadingChangefeeds();
        };

        if (NeedValidateChecksums) {
            StartValidatingChecksum(PermissionsKey, content, nextStep);
        } else {
            nextStep();
        }
    }

    void HandleChecksum(TEvExternalStorage::TEvGetObjectResponse::TPtr& ev) {
        const auto& msg = *ev->Get();
        const auto& result = msg.Result;

        LOG_D("HandleChecksum TEvExternalStorage::TEvGetObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "GetObject")) {
            return;
        }

        TString expectedChecksum = msg.Body.substr(0, msg.Body.find(' '));
        if (expectedChecksum != CurrentObjectChecksum) {
            return Reply(false, TStringBuilder() << "Checksum mismatch for " << CurrentObjectKey
                << " expected# " << expectedChecksum
                << ", got# " << CurrentObjectChecksum);
        }

        ChecksumValidatedCallback();
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
        if (!MaybeDecrypt(msg.Body, content, NBackup::EBackupFileType::TableChangefeed)) {
            return;
        }

        Y_ABORT_UNLESS(ItemIdx < ImportInfo->Items.size());
        auto& item = ImportInfo->Items.at(ItemIdx);

        LOG_T("Trying to parse changefeed"
            << ": self# " << SelfId()
            << ", body# " << SubstGlobalCopy(content, "\n", "\\n"));

        Ydb::Table::ChangefeedDescription changefeed;
        if (!google::protobuf::TextFormat::ParseFromString(content, &changefeed)) {
            return Reply(false, "Cannot parse сhangefeed");
        }

        *item.Changefeeds.MutableChangefeeds(IndexDownloadedChangefeed)->MutableChangefeed() = std::move(changefeed);

        auto nextStep = [this]() {
            Become(&TThis::StateDownloadTopics);
            HeadObject(TopicDescriptionKeyFromSettings(ImportInfo, ItemIdx, ChangefeedsNames[IndexDownloadedChangefeed]));
        };

        if (NeedValidateChecksums) {
            StartValidatingChecksum(ChangefeedDescriptionKeyFromSettings(ImportInfo, ItemIdx, ChangefeedsNames[IndexDownloadedChangefeed]), content, nextStep);
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
        if (!MaybeDecrypt(msg.Body, content, NBackup::EBackupFileType::TableTopic)) {
            return;
        }

        Y_ABORT_UNLESS(ItemIdx < ImportInfo->Items.size());
        auto& item = ImportInfo->Items.at(ItemIdx);

        LOG_T("Trying to parse topic"
            << ": self# " << SelfId()
            << ", body# " << SubstGlobalCopy(content, "\n", "\\n"));

        Ydb::Topic::DescribeTopicResult topic;
        if (!google::protobuf::TextFormat::ParseFromString(content, &topic)) {
            return Reply(false, "Cannot parse topic");
        }
        *item.Changefeeds.MutableChangefeeds(IndexDownloadedChangefeed)->MutableTopic() = std::move(topic);

        auto nextStep = [this]() {
            if (++IndexDownloadedChangefeed >= ChangefeedsNames.size()) {
                Reply();
            } else {
                Become(&TThis::StateDownloadChangefeeds);
                HeadObject(ChangefeedDescriptionKeyFromSettings(ImportInfo, ItemIdx, ChangefeedsNames[IndexDownloadedChangefeed]));
            }
        };

        if (NeedValidateChecksums) {
            StartValidatingChecksum(TopicDescriptionKeyFromSettings(ImportInfo, ItemIdx, ChangefeedsNames[IndexDownloadedChangefeed]), content, nextStep);
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
        ChangefeedsNames.clear();
        ChangefeedsNames.reserve(objects.size());

        for (const auto& obj : objects) {
            const TFsPath& path = obj.GetKey();
            if (path.GetName() == "changefeed_description.pb") {
                ChangefeedsNames.push_back(path.Parent().GetName());
            }
        }

        if (!ChangefeedsNames.empty()) {
            auto& item = ImportInfo->Items.at(ItemIdx);
            Resize(item.Changefeeds.MutableChangefeeds(), ChangefeedsNames.size());

            Y_ABORT_UNLESS(IndexDownloadedChangefeed < ChangefeedsNames.size());
            HeadObject(ChangefeedDescriptionKeyFromSettings(ImportInfo, ItemIdx, ChangefeedsNames[IndexDownloadedChangefeed]));
        } else {
            Reply();
        }

    }

    void Reply(bool success = true, const TString& error = TString()) override {
        LOG_I("Reply"
            << ": self# " << SelfId()
            << ", success# " << success
            << ", error# " << error);

        Send(ReplyTo, new TEvPrivate::TEvImportSchemeReady(ImportInfo->Id, ItemIdx, success, error));
        PassAway();
    }

    void ListChangefeeds() {
        CreateClient();
        ListObjects(ImportInfo->GetItemSrcPrefix(ItemIdx));
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

    void DownloadChecksum() {
        Download(NBackup::ChecksumKey(CurrentObjectKey), false);
    }

    void DownloadChangefeeds() {
        Become(&TThis::StateDownloadChangefeeds);
        ListChangefeeds();
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

    void StartDownloadingChangefeeds() {
        ResetRetries();
        DownloadChangefeeds();
    }

    void StartValidatingChecksum(const TString& key, const TString& object, std::function<void()> checksumValidatedCallback) {
        CurrentObjectKey = key;
        CurrentObjectChecksum = NBackup::ComputeChecksum(object);
        ChecksumValidatedCallback = checksumValidatedCallback;

        ResetRetries();
        DownloadChecksum();
        Become(&TThis::StateDownloadChecksum);
    }

public:
    explicit TSchemeGetter(const TActorId& replyTo, TImportInfo::TPtr importInfo, ui32 itemIdx, TMaybe<NBackup::TEncryptionIV> iv)
        : TGetterFromS3<TSchemeGetter>(std::move(importInfo), std::move(iv))
        , ReplyTo(replyTo)
        , ItemIdx(itemIdx)
        , MetadataKey(MetadataKeyFromSettings(ImportInfo, itemIdx))
        , SchemeKey(SchemeKeyFromSettings(ImportInfo, itemIdx, "scheme.pb"))
        , PermissionsKey(PermissionsKeyFromSettings(ImportInfo, itemIdx))
        , NeedDownloadPermissions(!ImportInfo->Settings.no_acl())
        , NeedValidateChecksums(!ImportInfo->Settings.skip_checksum_validation())
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

    STATEFN(StateDownloadChecksum) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvHeadObjectResponse, HandleChecksum);
            hFunc(TEvExternalStorage::TEvGetObjectResponse, HandleChecksum);

            sFunc(TEvents::TEvWakeup, DownloadChecksum);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

private:
    const TActorId ReplyTo;
    const ui32 ItemIdx;

    const TString MetadataKey;
    TString SchemeKey;
    const TString PermissionsKey;
    TVector<TString> ChangefeedsNames;
    ui64 IndexDownloadedChangefeed = 0;

    const bool NeedDownloadPermissions = true;

    bool NeedValidateChecksums = true;

    TString CurrentObjectChecksum;
    TString CurrentObjectKey;
    std::function<void()> ChecksumValidatedCallback;
}; // TSchemeGetter

class TSchemaMappingGetter : public TGetterFromS3<TSchemaMappingGetter> {
    static TString SchemaMappingKeyFromSettings(const TImportInfo::TPtr& importInfo) {
        return TStringBuilder() << importInfo->Settings.source_prefix() << "/SchemaMapping/mapping.json";
    }

    static TString SchemaMappingMetadataKeyFromSettings(const TImportInfo::TPtr& importInfo) {
        return TStringBuilder() << importInfo->Settings.source_prefix() << "/SchemaMapping/metadata.json";
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

        TString content;
        if (!MaybeDecryptAndSaveIV(msg.Body, content)) {
            return;
        }
        ImportInfo->ExportIV = IV;

        LOG_T("Trying to parse metadata"
            << ": self# " << SelfId()
            << ", body# " << SubstGlobalCopy(content, "\n", "\\n"));

        if (!ProcessMetadata(content)) {
            return;
        }

        auto nextStep = [this]() {
            StartDownloadingSchemaMapping();
        };

        nextStep();
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
            Reply(false, error);
            return;
        }

        Reply();
    }

    void Reply(bool success = true, const TString& error = TString()) override {
        LOG_I("Reply"
            << ": self# " << SelfId()
            << ", success# " << success
            << ", error# " << error);

        Send(ReplyTo, new TEvPrivate::TEvImportSchemaMappingReady(ImportInfo->Id, success, error));
        PassAway();
    }

    void DownloadMetadata() {
        Download(MetadataKey);
    }

    void DownloadSchemaMapping() {
        Download(SchemaMappingKey);
    }

    void StartDownloadingSchemaMapping() {
        ResetRetries();
        DownloadSchemaMapping();
        Become(&TThis::StateDownloadSchemaMapping);
    }

    bool ProcessMetadata(const TString& content) {
        NJson::TJsonValue json;
        if (!NJson::ReadJsonTree(content, &json)) {
            Reply(false, "Failed to parse metadata json");
            return false;
        }
        const NJson::TJsonValue& kind = json["kind"];
        if (kind.GetString() != "SchemaMappingV0") {
            Reply(false, TStringBuilder() << "Unknown kind of metadata json: " << kind.GetString());
            return false;
        }
        return true;
    }

public:
    TSchemaMappingGetter(const TActorId& replyTo, TImportInfo::TPtr importInfo)
        : TGetterFromS3<TSchemaMappingGetter>(std::move(importInfo), Nothing())
        , ReplyTo(replyTo)
        , MetadataKey(SchemaMappingMetadataKeyFromSettings(ImportInfo))
        , SchemaMappingKey(SchemaMappingKeyFromSettings(ImportInfo))
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

    STATEFN(StateDownloadSchemaMapping) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvHeadObjectResponse, HandleSchemaMapping);
            hFunc(TEvExternalStorage::TEvGetObjectResponse, HandleSchemaMapping);

            sFunc(TEvents::TEvWakeup, DownloadSchemaMapping);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

private:
    const TActorId ReplyTo;
    const TString MetadataKey;
    const TString SchemaMappingKey;
}; // TSchemaMappingGetter

IActor* CreateSchemeGetter(const TActorId& replyTo, TImportInfo::TPtr importInfo, ui32 itemIdx, TMaybe<NBackup::TEncryptionIV> iv) {
    return new TSchemeGetter(replyTo, std::move(importInfo), itemIdx, std::move(iv));
}

IActor* CreateSchemaMappingGetter(const TActorId& replyTo, TImportInfo::TPtr importInfo) {
    return new TSchemaMappingGetter(replyTo, std::move(importInfo));
}

} // NSchemeShard
} // NKikimr
