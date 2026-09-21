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
        NThreading::TFuture<std::string> future;
        try {
            provider = GetProvider();
            future = provider->GetAuthInfoAsync();
            if (future.HasException()) {
                // The SDK provider stops for good after one non-retryable answer of the metadata service (a 404
                // while no service account is attached to the VM, say) and keeps returning that failure. Drop it
                // and create a fresh one, so that the next request asks the metadata service again. This runs on
                // the caller's thread on purpose: destroying the provider joins its facility thread, which is
                // the thread the Subscribe callback below runs on.
                DropProvider(provider);
                provider.reset();
                provider = GetProvider();
                future = provider->GetAuthInfoAsync();
            }
        } catch (const std::exception& e) {
            Deliver(actorSystem, recipient, cookie, {}, TStringBuilder() << "cannot create system service account credentials provider: " << e.what());
            return;
        }
        future.Subscribe([actorSystem, recipient, cookie](const NThreading::TFuture<std::string>& future) {
            try {
                Deliver(actorSystem, recipient, cookie, TString(future.GetValue()), {});
            } catch (const std::exception& e) {
                Deliver(actorSystem, recipient, cookie, {}, TStringBuilder() << "cannot obtain system service account token: " << e.what());
            }
        });
    }

private:
    // The SDK caches providers process-wide by endpoint, so a fresh provider is created only once every
    // holder of the failed one has released it; a client of the same metadata endpoint elsewhere in the
    // process (AUTH_METHOD = "IAM" data sources, async replication) that still holds it keeps it alive.
    std::shared_ptr<NYdb::ICredentialsProvider> GetProvider() {
        with_lock (Mutex) {
            if (!Provider) {
                Provider = Factory->CreateProvider();
            }
            return Provider;
        }
    }

    void DropProvider(const std::shared_ptr<NYdb::ICredentialsProvider>& failed) {
        with_lock (Mutex) {
            if (Provider == failed) {
                Provider.reset();
            }
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
