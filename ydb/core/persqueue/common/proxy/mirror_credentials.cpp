#include "mirror_credentials.h"

#include "actor_persqueue_client_iface.h"

#include <ydb/core/base/path.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/services/scheme_secret/resolver.h>

namespace NKikimr::NPQ {

namespace {

class TMirrorCredentialsInitializer
    : public NActors::TActorBootstrapped<TMirrorCredentialsInitializer>
    , public NActors::IActorExceptionHandler
{
    void Reply(NActors::IEventBase* event) {
        Send(Parent, event, 0, Cookie);
        PassAway();
    }

    void ReplyError(const TString& error) {
        Reply(new TEvPQ::TEvCredentialsCreated(error));
    }

    void StartProviderFactory() {
        auto future = Factory->GetCredentialsProvider(ResolvedCredentials);
        future.Subscribe([
            actorSystem = ActorContext().ActorSystem(),
            selfId = SelfId()
        ](const NThreading::TFuture<NYdb::TCredentialsProviderFactoryPtr>& result) {
            THolder<TEvPQ::TEvCredentialsCreated> event;
            if (result.HasException()) {
                TString error;
                try {
                    result.TryRethrow();
                } catch (...) {
                    error = CurrentExceptionMessage();
                }
                event = MakeHolder<TEvPQ::TEvCredentialsCreated>(error);
            } else {
                event = MakeHolder<TEvPQ::TEvCredentialsCreated>(result.GetValue());
            }
            actorSystem->Send(new NActors::IEventHandle(selfId, selfId, event.Release()));
        });
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* response = ev->Get()->Request.Get();
        if (!response || response->ResultSet.size() != 1) {
            return ReplyError("Cannot resolve mirror topic owner: invalid scheme cache response");
        }

        const auto& entry = response->ResultSet.front();
        if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            return ReplyError(TStringBuilder() << "Cannot resolve mirror topic owner: " << static_cast<ui32>(entry.Status));
        }
        if (!entry.SecurityObject) {
            return ReplyError("Cannot resolve mirror topic owner: empty security object");
        }

        const auto& ownerSid = entry.SecurityObject->GetOwnerSID();
        if (ownerSid.empty()) {
            return ReplyError("Cannot resolve mirror topic owner: empty owner SID");
        }
        auto userToken = MakeIntrusiveConst<NACLib::TUserToken>(ownerSid, TVector<TString>());
        auto future = NSecret::DescribeSecret(
            {GetMirrorCredentialsSecretName(Credentials)},
            userToken,
            Database,
            ActorContext().ActorSystem(),
            {.RetryPolicy = NSecret::MakeLongRetryPolicy()}
        );
        future.Subscribe([
            actorSystem = ActorContext().ActorSystem(),
            selfId = SelfId()
        ](const NThreading::TFuture<NKqp::TEvDescribeSecretsResponse::TDescription>& result) {
            try {
                actorSystem->Send(selfId, new NKqp::TEvDescribeSecretsResponse(result.GetValue()));
            } catch (...) {
                actorSystem->Send(selfId, new NKqp::TEvDescribeSecretsResponse({
                    Ydb::StatusIds::INTERNAL_ERROR,
                    {NYql::TIssue(CurrentExceptionMessage())}
                }));
            }
        });
    }

    void Handle(NKqp::TEvDescribeSecretsResponse::TPtr& ev) {
        const auto& description = ev->Get()->Description;
        if (description.Status != Ydb::StatusIds::SUCCESS) {
            return ReplyError(TStringBuilder() << "Cannot resolve mirror credentials secret: "
                << description.Issues.ToOneLineString());
        }
        if (description.SecretValues.size() != 1 || description.SecretValues.front().empty()) {
            return ReplyError("Cannot resolve mirror credentials secret: empty or invalid response");
        }

        ResolvedCredentials = Credentials;
        if (!SetMirrorCredentialsSecretValue(ResolvedCredentials, description.SecretValues.front())) {
            return ReplyError("Cannot resolve mirror credentials secret: unsupported credentials type");
        }
        StartProviderFactory();
    }

    void Handle(TEvPQ::TEvCredentialsCreated::TPtr& ev) {
        if (!ev->Get()->Error && !ev->Get()->Credentials) {
            return ReplyError("Cannot initialize mirror credentials: empty credentials provider");
        }
        Reply(ev->Release().Release());
    }

    bool OnUnhandledException(const std::exception& exception) override {
        ReplyError(TStringBuilder() << "Unhandled mirror credentials initialization error: " << exception.what());
        return true;
    }

public:
    TMirrorCredentialsInitializer(
        const NActors::TActorId& parent,
        ui64 cookie,
        const NKikimrPQ::TMirrorPartitionConfig::TCredentials& credentials,
        TString database,
        TString topicPath,
        const IPersQueueMirrorReaderFactory* factory
    )
        : Parent(parent)
        , Cookie(cookie)
        , Credentials(credentials)
        , Database(std::move(database))
        , TopicPath(std::move(topicPath))
        , Factory(factory)
    {
    }

    void Bootstrap() {
        Become(&TThis::StateWork);

        const auto secretName = GetMirrorCredentialsSecretName(Credentials);
        if (secretName.empty()) {
            return ReplyError("Cannot resolve mirror credentials secret: empty secret name");
        }
        if (Credentials.HasIam() && Credentials.GetIam().HasServiceAccountKey()) {
            return ReplyError("Cannot resolve mirror credentials secret: inline IAM key and secret reference are both set");
        }
        if (Database.empty() || TopicPath.empty()) {
            return ReplyError("Cannot resolve mirror credentials secret: database and topic path are required");
        }

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        auto& entry = request->ResultSet.emplace_back();
        entry.Path = SplitPath(TopicPath);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.RedirectRequired = false;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(NKqp::TEvDescribeSecretsResponse, Handle);
            hFunc(TEvPQ::TEvCredentialsCreated, Handle);
            sFunc(NActors::TEvents::TEvPoison, PassAway);
        }
    }

private:
    const NActors::TActorId Parent;
    const ui64 Cookie;
    const NKikimrPQ::TMirrorPartitionConfig::TCredentials Credentials;
    const TString Database;
    const TString TopicPath;
    const IPersQueueMirrorReaderFactory* Factory;
    NKikimrPQ::TMirrorPartitionConfig::TCredentials ResolvedCredentials;
};

} // namespace

NActors::IActor* CreateMirrorCredentialsInitializer(
    const NActors::TActorId& parent,
    ui64 cookie,
    const NKikimrPQ::TMirrorPartitionConfig::TCredentials& credentials,
    TString database,
    TString topicPath,
    const IPersQueueMirrorReaderFactory* factory
) {
    return new TMirrorCredentialsInitializer(parent, cookie, credentials, std::move(database), std::move(topicPath), factory);
}

} // namespace NKikimr::NPQ
