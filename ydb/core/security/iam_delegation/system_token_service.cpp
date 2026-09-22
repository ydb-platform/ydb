#include "system_token_service.h"
#include "events.h"

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>

#include <library/cpp/threading/future/future.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::IAM_DELEGATION

namespace NKikimr::NIamDelegation {

namespace {

using namespace NActors;

class TIamSystemTokenService : public TActor<TIamSystemTokenService>, public IActorExceptionHandler {
private:
    using TProvider = std::shared_ptr<NYdb::ICredentialsProvider>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::IAM_SYSTEM_TOKEN_SERVICE_ACTOR;
    }

    TIamSystemTokenService(const TString& host, ui32 port)
        : TActor(&TThis::StateWork)
    {
        NYdb::TIamHost params;
        if (host) {
            params.Host = host;
            params.Port = port;
        }
        Factory = NYdb::CreateIamCredentialsProviderFactory(params);
    }

    using IActorExceptionHandler::OnUnhandledException;
    bool OnUnhandledException(const std::exception& e) override {
        YDB_LOG_ERROR("Unhandled exception in the IAM system token service", {"exception", e.what()});
        return true;
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvIamDelegation::TEvGetSystemToken, Handle);
        cFunc(TEvents::TEvPoison::EventType, PassAway);
    )

private:
    void Handle(TEvIamDelegation::TEvGetSystemToken::TPtr& ev) {
        const TActorId requester = ev->Sender;
        const ui64 cookie = ev->Cookie;

        NThreading::TFuture<std::string> future;
        try {
            if (!Provider) {
                Provider = Factory->CreateProvider();
            }
            future = Provider->GetAuthInfoAsync();
            if (future.HasException()) {
                // The SDK provider stops for good after one non-retryable answer of the metadata service (a 404
                // while no service account is attached to the VM, say) and keeps returning that failure. Drop it
                // and create a fresh one, so that this and the next requests ask the metadata service again.
                // Destroying it joins its facility thread, which is the thread the Subscribe callback below runs
                // on: that is why it happens here, in the handler, and never in the callback. The SDK caches
                // providers process-wide by endpoint, so the fresh one appears once every holder of the failed
                // one (AUTH_METHOD = "IAM" data sources, async replication) has released it.
                Provider.reset();
                Provider = Factory->CreateProvider();
                future = Provider->GetAuthInfoAsync();
            }
        } catch (const std::exception& e) {
            Send(requester, new TEvIamDelegation::TEvSystemTokenReady({},
                TStringBuilder() << "cannot create system service account credentials provider: " << e.what()), 0, cookie);
            return;
        }

        // the SDK completes the future on its own thread; TActorSystem::Send is the thread-safe way back into the actor system
        TActorSystem* const actorSystem = TActivationContext::ActorSystem();
        const TActorId self = SelfId();
        future.Subscribe([actorSystem, self, requester, cookie](const NThreading::TFuture<std::string>& future) {
            TString token;
            TString error;
            try {
                token = TString(future.GetValue());
            } catch (const std::exception& e) {
                error = TStringBuilder() << "cannot obtain system service account token: " << e.what();
            }
            actorSystem->Send(new IEventHandle(requester, self, new TEvIamDelegation::TEvSystemTokenReady(std::move(token), std::move(error)), 0, cookie));
        });
    }

private:
    std::shared_ptr<NYdb::ICredentialsProviderFactory> Factory;
    TProvider Provider;
};

} // namespace

IActor* CreateIamSystemTokenService(const TString& host, ui32 port) {
    return new TIamSystemTokenService(host, port);
}

} // namespace NKikimr::NIamDelegation
