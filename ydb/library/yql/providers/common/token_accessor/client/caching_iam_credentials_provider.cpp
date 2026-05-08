#include "caching_iam_credentials_provider.h"
#include "caching_iam_credentials_provider_service.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam_private/iam.h>
#include <util/string/cast.h>

namespace NYql {

namespace {
struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),

        EvGetAuthInfoRequest = EvBegin,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events

    struct TEvGetAuthInfoRequest : public NActors::TEventLocal<TEvGetAuthInfoRequest, EvGetAuthInfoRequest> {
        TString ServiceAccountId;
        TString ResourceId;
        NThreading::TPromise<std::string> Promise;

        explicit TEvGetAuthInfoRequest(const TString& serviceAccountId, const TString& resourceId, NThreading::TPromise<std::string>& promise)
           : ServiceAccountId(serviceAccountId)
           , ResourceId(resourceId)
           , Promise(promise)
        {}
    };
};

class TIamResolverActor : public NActors::TActor<TIamResolverActor> {
    using TBase = NActors::TActor<TIamResolverActor>;
    public:
        TIamResolverActor(const TString& serviceAccountId, const TString& resourceId)
            : TBase(&TIamResolverActor::StateFunc)
        {
            const auto& serviceControl = NKikimr::AppData()->ReplicationConfig.GetIamServiceControl();

            NYdb::TIamServiceParams iamParams;
            iamParams.SystemServiceAccountCredentials = NYdb::CreateIamCredentialsProviderFactory();
            iamParams.Endpoint = serviceControl.GetEndpoint();
            iamParams.ServiceId = serviceControl.GetServiceId();
            iamParams.MicroserviceId = serviceControl.GetMicroserviceId();
            iamParams.ResourceType = serviceControl.GetResourceType();
            iamParams.ResourceId = resourceId;
            iamParams.TargetServiceAccountId = serviceAccountId;

            Provider = CreateIamServiceCredentialsProviderFactory(iamParams)->CreateProvider();
        }
    private:
        STRICT_STFUNC(StateFunc,
            hFunc(TEvPrivate::TEvGetAuthInfoRequest, Handle);
        )
        void Handle(TEvPrivate::TEvGetAuthInfoRequest::TPtr& event) {
            auto& promise = event->Get()->Promise;
            try {
                promise.SetValue(Provider->GetAuthInfo());
            } catch(...) {
                promise.SetException(std::current_exception());
            }
        }
        NYdb::TCredentialsProviderPtr Provider;
};

class TIamResolverServiceActor : public NActors::TActor<TIamResolverServiceActor> {
        using TBase = NActors::TActor<TIamResolverServiceActor>;
    public:
        TIamResolverServiceActor()
            : TBase(&TIamResolverServiceActor::StateFunc)
        {
        }
    private:
    STRICT_STFUNC(
            StateFunc,
            hFunc(TEvPrivate::TEvGetAuthInfoRequest, Handle);
            );
    void Handle(TEvPrivate::TEvGetAuthInfoRequest::TPtr& ev) {
        auto [it, inserted] = Actors.emplace(std::pair { std::move(ev->Get()->ServiceAccountId), std::move(ev->Get()->ResourceId) }, NActors::TActorId {});
        if (inserted) {
            it->second = Register(new TIamResolverActor(it->first.first, it->first.second));
        }
        Send(ev->Forward(it->second));
    }
    THashMap<std::pair<TString, TString>, NActors::TActorId> Actors; // TODO replace with LRU
};

class TCachingIamServiceCredentialsProvider : public NYdb::ICredentialsProvider {
public:
    explicit TCachingIamServiceCredentialsProvider(const TString& serviceAccountId, const TString& resourceId, NActors::TActorSystem* actorSystem)
        : ServiceAccountId(serviceAccountId)
        , ResourceId(resourceId)
        , ActorSystem(actorSystem)
    {
    }

    std::string GetAuthInfo() const override {
        auto promise = NThreading::NewPromise<std::string>();
        ActorSystem->Send(MakeCachingIamServiceCredentialsProviderServiceId(), new TEvPrivate::TEvGetAuthInfoRequest(ServiceAccountId, ResourceId, promise));

        return promise.GetFuture().GetValueSync();
    }

    bool IsValid() const override {
        return true;
    }

private:
    const TString ServiceAccountId;
    const TString ResourceId;
    NActors::TActorSystem* const ActorSystem;
};

class TCachingIamServiceCredentialsProviderFactory : public NYdb::ICredentialsProviderFactory {
public:
    explicit TCachingIamServiceCredentialsProviderFactory(const TString& serviceAccountId, const TString& resourceId, NActors::TActorSystem* actorSystem)
        : ServiceAccountId(serviceAccountId)
        , ResourceId(resourceId)
        , ActorSystem(actorSystem)
    {
    }

    std::string GetClientIdentity() const override {
        return "CACHING_IAM_CRED_PROV_FACTORY|" + ServiceAccountId + "|" + ResourceId + "|" + ToString(ui64(ActorSystem));
    }

    std::shared_ptr<NYdb::ICredentialsProvider> CreateProvider() const override {
        return std::make_shared<TCachingIamServiceCredentialsProvider>(ServiceAccountId, ResourceId, ActorSystem);
    }

private:
    const TString ServiceAccountId;
    const TString ResourceId;
    NActors::TActorSystem* const ActorSystem;
};

} // namespace {

std::shared_ptr<NYdb::ICredentialsProviderFactory> CreateCachingIamServiceCredentialsProviderFactory(const TString& serviceAccountId, const TString& resourceId) {
    auto actorSystem = NActors::TlsActivationContext ? NActors::TlsActivationContext->ActorSystem() : nullptr;
    Y_ENSURE(actorSystem);
    return std::make_shared<TCachingIamServiceCredentialsProviderFactory>(serviceAccountId, resourceId, actorSystem);
}

std::unique_ptr<NActors::IActor> NewCachingIamServiceCredentialsProviderService() {
    return std::make_unique<TIamResolverServiceActor>();
}

NActors::TActorId MakeCachingIamServiceCredentialsProviderServiceId() {
    return NActors::TActorId(0, "SvcCachedIAM"sv);
}
} // namespace NYql
