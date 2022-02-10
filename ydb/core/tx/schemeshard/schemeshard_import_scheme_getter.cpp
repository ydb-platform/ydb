#include "schemeshard_import_scheme_getter.h"
#include "schemeshard_import_helpers.h"
#include "schemeshard_private.h"

#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/public/api/protos/ydb_import.pb.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>

#include <util/string/subst.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NWrappers;

using namespace Aws::Auth;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws;

class TSchemeGetter: public TActorBootstrapped<TSchemeGetter>, private TS3User {
    static ClientConfiguration ConfigFromSettings(const Ydb::Import::ImportFromS3Settings& settings) {
        ClientConfiguration config;

        config.endpointOverride = settings.endpoint();
        config.verifySSL = false;
        config.connectTimeoutMs = 10000;
        config.maxConnections = 5;

        switch (settings.scheme()) {
        case Ydb::Import::ImportFromS3Settings::HTTP:
            config.scheme = Http::Scheme::HTTP;
            break;
        case Ydb::Import::ImportFromS3Settings::HTTPS:
            config.scheme = Http::Scheme::HTTPS;
            break;
        default:
            Y_FAIL("Unknown scheme");
        }

        return config;
    }

    static AWSCredentials CredentialsFromSettings(const Ydb::Import::ImportFromS3Settings& settings) {
        return AWSCredentials(settings.access_key(), settings.secret_key());
    }

    static TString SchemeKeyFromSettings(const Ydb::Import::ImportFromS3Settings& settings, ui32 itemIdx) {
        Y_VERIFY(itemIdx < (ui32)settings.items_size());
        return TStringBuilder() << settings.items(itemIdx).source_prefix() << "/scheme.pb";
    }

    void HeadObject(const TString& key) {
        auto request = Model::HeadObjectRequest()
            .WithBucket(ImportInfo->Settings.bucket())
            .WithKey(key);

        Send(Client, new TEvS3Wrapper::TEvHeadObjectRequest(request));
    }

    void Handle(TEvS3Wrapper::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("Handle TEvS3Wrapper::TEvHeadObjectResponse"
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
            .WithBucket(ImportInfo->Settings.bucket())
            .WithKey(key)
            .WithRange(TStringBuilder() << "bytes=" << range.first << "-" << range.second);

        Send(Client, new TEvS3Wrapper::TEvGetObjectRequest(request));
    }

    void Handle(TEvS3Wrapper::TEvGetObjectResponse::TPtr& ev) {
        const auto& msg = *ev->Get();
        const auto& result = msg.Result;

        LOG_D("Handle TEvS3Wrapper::TEvGetObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, "GetObject")) {
            return;
        }

        Y_VERIFY(ItemIdx < ImportInfo->Items.size());
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
        MaybeRetry(result.GetError().GetMessage().c_str());

        return false;
    }

    void MaybeRetry(const TString& error) {
        if (Attempt++ < Retries) {
            Schedule(TDuration::Minutes(1), new TEvents::TEvWakeup());
        } else {
            Reply(false, error);
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
        : ReplyTo(replyTo)
        , ImportInfo(importInfo)
        , ItemIdx(itemIdx)
        , Config(ConfigFromSettings(importInfo->Settings))
        , Credentials(CredentialsFromSettings(importInfo->Settings))
        , SchemeKey(SchemeKeyFromSettings(importInfo->Settings, itemIdx))
        , Retries(importInfo->Settings.number_of_retries())
    {
    }

    void Bootstrap() {
        if (Client) {
            Send(Client, new TEvents::TEvPoisonPill());
        }

        Client = RegisterWithSameMailbox(CreateS3Wrapper(Credentials, Config)); 

        HeadObject(SchemeKey);
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvS3Wrapper::TEvHeadObjectResponse, Handle);
            hFunc(TEvS3Wrapper::TEvGetObjectResponse, Handle);

            cFunc(TEvents::TEvWakeup::EventType, Bootstrap);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

private:
    const TActorId ReplyTo;
    TImportInfo::TPtr ImportInfo;
    const ui32 ItemIdx;

    const ClientConfiguration Config;
    const AWSCredentials Credentials;
    const TString SchemeKey;

    const ui32 Retries;
    ui32 Attempt = 0;

    TActorId Client;

}; // TSchemeGetter

IActor* CreateSchemeGetter(const TActorId& replyTo, TImportInfo::TPtr importInfo, ui32 itemIdx) {
    return new TSchemeGetter(replyTo, importInfo, itemIdx);
}

} // NSchemeShard
} // NKikimr
