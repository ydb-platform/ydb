#include "schemeshard_import_scheme_getter.h"
#include "schemeshard_import_helpers.h"
#include "schemeshard_private.h"

#include <ydb/core/wrappers/s3_storage_config.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/public/api/protos/ydb_import.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <google/protobuf/text_format.h>

#include <util/string/subst.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NWrappers;

using namespace Aws::Auth;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws;

class TSchemeGetter: public TActorBootstrapped<TSchemeGetter> {
    static TString SchemeKeyFromSettings(const Ydb::Import::ImportFromS3Settings& settings, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < (ui32)settings.items_size());
        return TStringBuilder() << settings.items(itemIdx).source_prefix() << "/scheme.pb";
    }

    void HeadObject(const TString& key) {
        auto request = Model::HeadObjectRequest()
            .WithKey(key);

        Send(Client, new TEvExternalStorage::TEvHeadObjectRequest(request));
    }

    void Handle(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("Handle TEvExternalStorage::TEvHeadObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "HeadObject")) {
            return;
        }

        const auto contentLength = result.GetResult().GetContentLength();
        GetObject(SchemeKey, std::make_pair(0, contentLength - 1));
    }

    void GetObject(const TString& key, const std::pair<ui64, ui64>& range) {
        auto request = Model::GetObjectRequest()
            .WithKey(key)
            .WithRange(TStringBuilder() << "bytes=" << range.first << "-" << range.second);

        Send(Client, new TEvExternalStorage::TEvGetObjectRequest(request));
    }

    void Handle(TEvExternalStorage::TEvGetObjectResponse::TPtr& ev) {
        const auto& msg = *ev->Get();
        const auto& result = msg.Result;

        LOG_D("Handle TEvExternalStorage::TEvGetObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "GetObject")) {
            return;
        }

        Y_ABORT_UNLESS(ItemIdx < ImportInfo->Items.size());
        auto& item = ImportInfo->Items.at(ItemIdx);

        LOG_T("Trying to parse"
            << ": self# " << SelfId()
            << ", body# " << SubstGlobalCopy(msg.Body, "\n", "\\n"));

        if (!google::protobuf::TextFormat::ParseFromString(msg.Body, &item.Scheme)) {
            return Reply(false, "Cannot parse scheme");
        }

        Reply();
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

public:
    explicit TSchemeGetter(const TActorId& replyTo, TImportInfo::TPtr importInfo, ui32 itemIdx)
        : ExternalStorageConfig(new NWrappers::NExternalStorage::TS3ExternalStorageConfig(importInfo->Settings))
        , ReplyTo(replyTo)
        , ImportInfo(importInfo)
        , ItemIdx(itemIdx)
        , SchemeKey(SchemeKeyFromSettings(importInfo->Settings, itemIdx))
        , Retries(importInfo->Settings.number_of_retries())
    {
    }

    void Bootstrap() {
        if (Client) {
            Send(Client, new TEvents::TEvPoisonPill());
        }
        Client = RegisterWithSameMailbox(CreateS3Wrapper(ExternalStorageConfig->ConstructStorageOperator()));

        HeadObject(SchemeKey);
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvHeadObjectResponse, Handle);
            hFunc(TEvExternalStorage::TEvGetObjectResponse, Handle);

            sFunc(TEvents::TEvWakeup, Bootstrap);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

private:
    NWrappers::IExternalStorageConfig::TPtr ExternalStorageConfig;
    const TActorId ReplyTo;
    TImportInfo::TPtr ImportInfo;
    const ui32 ItemIdx;

    const TString SchemeKey;

    const ui32 Retries;
    ui32 Attempt = 0;

    TDuration Delay = TDuration::Minutes(1);
    static constexpr TDuration MaxDelay = TDuration::Minutes(10);

    TActorId Client;

}; // TSchemeGetter

IActor* CreateSchemeGetter(const TActorId& replyTo, TImportInfo::TPtr importInfo, ui32 itemIdx) {
    return new TSchemeGetter(replyTo, importInfo, itemIdx);
}

} // NSchemeShard
} // NKikimr
