#include "system_token_source.h"
#include "events.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>

#include <util/system/mutex.h>

namespace NKikimr::NIamDelegation {

namespace {

using namespace NActors;

void Deliver(TActorSystem* actorSystem, const TActorId& recipient, ui64 cookie, TString token, TString error) {
    actorSystem->Send(new IEventHandle(recipient, TActorId(), new TEvIamDelegation::TEvSystemTokenReady(std::move(token), std::move(error)), 0, cookie));
}

class TVmMetadataSystemTokenSource : public ISystemTokenSource {
public:
    TVmMetadataSystemTokenSource(const TString& host, ui32 port) {
        NYdb::TIamHost params;
        if (host) {
            params.Host = host;
            params.Port = port;
        }
        Factory = NYdb::CreateIamCredentialsProviderFactory(params);
    }

    void RequestToken(TActorSystem* actorSystem, const TActorId& recipient, ui64 cookie) override {
        std::shared_ptr<NYdb::ICredentialsProvider> provider;
        try {
            provider = GetProvider();
        } catch (const std::exception& e) {
            Deliver(actorSystem, recipient, cookie, {}, TStringBuilder() << "cannot create system service account credentials provider: " << e.what());
            return;
        }
        provider->GetAuthInfoAsync().Subscribe([actorSystem, recipient, cookie](const NThreading::TFuture<std::string>& future) {
            try {
                Deliver(actorSystem, recipient, cookie, TString(future.GetValue()), {});
            } catch (const std::exception& e) {
                Deliver(actorSystem, recipient, cookie, {}, TStringBuilder() << "cannot obtain system service account token: " << e.what());
            }
        });
    }

private:
    std::shared_ptr<NYdb::ICredentialsProvider> GetProvider() {
        with_lock (Mutex) {
            if (!Provider) {
                Provider = Factory->CreateProvider();
            }
            return Provider;
        }
    }

    std::shared_ptr<NYdb::ICredentialsProviderFactory> Factory;
    std::shared_ptr<NYdb::ICredentialsProvider> Provider;
    TMutex Mutex;
};

class TStaticSystemTokenSource : public ISystemTokenSource {
public:
    explicit TStaticSystemTokenSource(TString token)
        : Token(std::move(token))
    {}

    void RequestToken(TActorSystem* actorSystem, const TActorId& recipient, ui64 cookie) override {
        Deliver(actorSystem, recipient, cookie, Token, {});
    }

private:
    const TString Token;
};

} // namespace

ISystemTokenSource::TPtr CreateVmMetadataSystemTokenSource(const TString& host, ui32 port) {
    return MakeIntrusive<TVmMetadataSystemTokenSource>(host, port);
}

ISystemTokenSource::TPtr CreateStaticSystemTokenSource(const TString& token) {
    return MakeIntrusive<TStaticSystemTokenSource>(token);
}

} // namespace NKikimr::NIamDelegation
