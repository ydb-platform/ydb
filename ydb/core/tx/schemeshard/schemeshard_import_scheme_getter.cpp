#include "schemeshard_import_scheme_getter.h"
#include "schemeshard_import_helpers.h"
#include "schemeshard_private.h"

#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/wrappers/s3_storage_config.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/public/api/protos/ydb_import.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/lib/ydb_cli/dump/files/files.h>

#include <google/protobuf/text_format.h>

#include <util/string/subst.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NWrappers;

using namespace Aws::Auth;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws;

// Downloads scheme-related objects from S3
class TSchemeGetter: public TActorBootstrapped<TSchemeGetter> {
    static TString MetadataKeyFromSettings(const Ydb::Import::ImportFromS3Settings& settings, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < (ui32)settings.items_size());
        return TStringBuilder() << settings.items(itemIdx).source_prefix() << "/metadata.json";
    }

    static TString SchemeKeyFromSettings(const Ydb::Import::ImportFromS3Settings& settings, ui32 itemIdx, TStringBuf filename) {
        Y_ABORT_UNLESS(itemIdx < (ui32)settings.items_size());
        return TStringBuilder() << settings.items(itemIdx).source_prefix() << '/' << filename;
    }

    static TString PermissionsKeyFromSettings(const Ydb::Import::ImportFromS3Settings& settings, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < (ui32)settings.items_size());
        return TStringBuilder() << settings.items(itemIdx).source_prefix() << "/permissions.pb";
    }

    static bool IsView(TStringBuf schemeKey) {
        return schemeKey.EndsWith(NYdb::NDump::NFiles::CreateView().FileName);
    }

    static bool NoObjectFound(Aws::S3::S3Errors errorType) {
        return errorType == S3Errors::RESOURCE_NOT_FOUND || errorType == S3Errors::NO_SUCH_KEY;
    }

    void HeadObject(const TString& key) {
        auto request = Model::HeadObjectRequest()
            .WithKey(key);

        Send(Client, new TEvExternalStorage::TEvHeadObjectRequest(request));
    }

    void HandleMetadata(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandleMetadata TEvExternalStorage::TEvHeadObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "HeadObject")) {
            return;
        }

        const auto contentLength = result.GetResult().GetContentLength();
        GetObject(MetadataKey, std::make_pair(0, contentLength - 1));
    }

    void HandleScheme(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandleScheme TEvExternalStorage::TEvHeadObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!IsView(SchemeKey) && NoObjectFound(result.GetError().GetErrorType())) {
            // try search for a view
            SchemeKey = SchemeKeyFromSettings(ImportInfo->Settings, ItemIdx, NYdb::NDump::NFiles::CreateView().FileName);
            HeadObject(SchemeKey);
            return;
        }

        if (!CheckResult(result, "HeadObject")) {
            return;
        }

        const auto contentLength = result.GetResult().GetContentLength();
        GetObject(SchemeKey, std::make_pair(0, contentLength - 1));
    }

    void HandlePermissions(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandlePermissions TEvExternalStorage::TEvHeadObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (NoObjectFound(result.GetError().GetErrorType())) {
            Reply(); // permissions are optional
            return;
        } else if (!CheckResult(result, "HeadObject")) {
            return;
        }

        const auto contentLength = result.GetResult().GetContentLength();
        GetObject(PermissionsKey, std::make_pair(0, contentLength - 1));
    }

    void HandleChecksum(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandleChecksum TEvExternalStorage::TEvHeadObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "HeadObject")) {
            return;
        }

        const auto contentLength = result.GetResult().GetContentLength();
        GetObject(ChecksumKey, std::make_pair(0, contentLength - 1));
    }

    void GetObject(const TString& key, const std::pair<ui64, ui64>& range) {
        auto request = Model::GetObjectRequest()
            .WithKey(key)
            .WithRange(TStringBuilder() << "bytes=" << range.first << "-" << range.second);

        Send(Client, new TEvExternalStorage::TEvGetObjectRequest(request));
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

        Y_ABORT_UNLESS(ItemIdx < ImportInfo->Items.size());
        auto& item = ImportInfo->Items.at(ItemIdx);

        LOG_T("Trying to parse metadata"
            << ": self# " << SelfId()
            << ", body# " << SubstGlobalCopy(msg.Body, "\n", "\\n"));

        item.Metadata = NBackup::TMetadata::Deserialize(msg.Body);

        if (!item.Metadata.HasVersion()) {
            return Reply(false, "Metadata is corrupted: no version");
        }

        NeedValidateChecksums = item.Metadata.GetVersion() > 0 && !SkipChecksumValidation;

        auto nextStep = [this]() {
            StartDownloadingScheme();
        };

        if (NeedValidateChecksums) {
            StartValidatingChecksum(MetadataKey, msg.Body, nextStep);
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

        Y_ABORT_UNLESS(ItemIdx < ImportInfo->Items.size());
        auto& item = ImportInfo->Items.at(ItemIdx);

        LOG_T("Trying to parse scheme"
            << ": self# " << SelfId()
            << ", itemIdx# " << ItemIdx
            << ", schemeKey# " << SchemeKey
            << ", body# " << SubstGlobalCopy(msg.Body, "\n", "\\n"));

        if (IsView(SchemeKey)) {
            item.CreationQuery = msg.Body;
        } else if (!google::protobuf::TextFormat::ParseFromString(msg.Body, &item.Scheme)) {
            return Reply(false, "Cannot parse scheme");
        }

        auto nextStep = [this]() {
            if (NeedDownloadPermissions) {
                StartDownloadingPermissions();
            } else {
                Reply();
            }
        };

        if (NeedValidateChecksums) {
            StartValidatingChecksum(SchemeKey, msg.Body, nextStep);
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

        Y_ABORT_UNLESS(ItemIdx < ImportInfo->Items.size());
        auto& item = ImportInfo->Items.at(ItemIdx);

        LOG_T("Trying to parse permissions"
            << ": self# " << SelfId()
            << ", body# " << SubstGlobalCopy(msg.Body, "\n", "\\n"));

        Ydb::Scheme::ModifyPermissionsRequest permissions;
        if (!google::protobuf::TextFormat::ParseFromString(msg.Body, &permissions)) {
            return Reply(false, "Cannot parse permissions");
        }
        item.Permissions = std::move(permissions);

        auto nextStep = [this]() {
            Reply();
        };

        if (NeedValidateChecksums) {
            StartValidatingChecksum(PermissionsKey, msg.Body, nextStep);
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
        if (expectedChecksum != Checksum) {
            return Reply(false, TStringBuilder() << "Checksum mismatch for " << ChecksumKey
                << " expected# " << expectedChecksum
                << ", got# " << Checksum);
        }

        ChecksumValidatedCallback();
    }

    template <typename TResult>
    bool CheckResult(const TResult& result, const TStringBuf marker) {
        if (result.IsSuccess()) {
            return true;
        }

        LOG_E("Error at '" << marker << "'"
            << ": self# " << SelfId()
            << ", error# " << result);
        MaybeRetry(result.GetError());

        return false;
    }

    void MaybeRetry(const Aws::S3::S3Error& error) {
        if (Attempt < Retries && error.ShouldRetry()) {
            Delay = Min(Delay * ++Attempt, MaxDelay);
            Schedule(Delay, new TEvents::TEvWakeup());
        } else {
            Reply(false, TStringBuilder() << "S3 error: " << error.GetMessage().c_str());
        }
    }

    void Reply(bool success = true, const TString& error = TString()) {
        LOG_I("Reply"
            << ": self# " << SelfId()
            << ", success# " << success
            << ", error# " << error);

        Send(ReplyTo, new TEvPrivate::TEvImportSchemeReady(ImportInfo->Id, ItemIdx, success, error));
        PassAway();
    }

    void PassAway() override {
        Send(Client, new TEvents::TEvPoisonPill());
        TActor::PassAway();
    }

    void Download(const TString& key) {
        if (Client) {
            Send(Client, new TEvents::TEvPoisonPill());
        }
        Client = RegisterWithSameMailbox(CreateS3Wrapper(ExternalStorageConfig->ConstructStorageOperator()));

        HeadObject(key);
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
        Download(ChecksumKey);
    }

    void ResetRetries() {
        Attempt = 0;
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

    void StartValidatingChecksum(const TString& key, const TString& object, std::function<void()> checksumValidatedCallback) {
        ChecksumKey = NBackup::ChecksumKey(key);
        Checksum = NBackup::ComputeChecksum(object);
        ChecksumValidatedCallback = checksumValidatedCallback;

        ResetRetries();
        DownloadChecksum();
        Become(&TThis::StateDownloadChecksum);
    }

public:
    explicit TSchemeGetter(const TActorId& replyTo, TImportInfo::TPtr importInfo, ui32 itemIdx)
        : ExternalStorageConfig(new NWrappers::NExternalStorage::TS3ExternalStorageConfig(importInfo->Settings))
        , ReplyTo(replyTo)
        , ImportInfo(importInfo)
        , ItemIdx(itemIdx)
        , MetadataKey(MetadataKeyFromSettings(importInfo->Settings, itemIdx))
        , SchemeKey(SchemeKeyFromSettings(importInfo->Settings, itemIdx, "scheme.pb"))
        , PermissionsKey(PermissionsKeyFromSettings(importInfo->Settings, itemIdx))
        , Retries(importInfo->Settings.number_of_retries())
        , NeedDownloadPermissions(!importInfo->Settings.no_acl())
        , SkipChecksumValidation(importInfo->Settings.skip_checksum_validation())
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

    STATEFN(StateDownloadChecksum) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvHeadObjectResponse, HandleChecksum);
            hFunc(TEvExternalStorage::TEvGetObjectResponse, HandleChecksum);

            sFunc(TEvents::TEvWakeup, DownloadChecksum);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

private:
    NWrappers::IExternalStorageConfig::TPtr ExternalStorageConfig;
    const TActorId ReplyTo;
    TImportInfo::TPtr ImportInfo;
    const ui32 ItemIdx;

    const TString MetadataKey;
    TString SchemeKey;
    const TString PermissionsKey;

    const ui32 Retries;
    ui32 Attempt = 0;

    TDuration Delay = TDuration::Minutes(1);
    static constexpr TDuration MaxDelay = TDuration::Minutes(10);

    const bool NeedDownloadPermissions = true;

    TActorId Client;

    const bool SkipChecksumValidation = false;
    bool NeedValidateChecksums = true;

    TString Checksum;
    TString ChecksumKey;
    std::function<void()> ChecksumValidatedCallback;
}; // TSchemeGetter

IActor* CreateSchemeGetter(const TActorId& replyTo, TImportInfo::TPtr importInfo, ui32 itemIdx) {
    return new TSchemeGetter(replyTo, importInfo, itemIdx);
}

} // NSchemeShard
} // NKikimr
