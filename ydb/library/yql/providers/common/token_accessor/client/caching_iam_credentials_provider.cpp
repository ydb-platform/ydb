#include "caching_iam_credentials_provider.h"
#include "caching_iam_credentials_provider_service.h"
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <util/string/cast.h>

namespace NYql {

namespace {

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
        ActorSystem->Send(MakeCachingIamServiceCredentialsProviderServiceId(), new TEvIamAuthCredentialsProviderService::TEvGetAuthInfoRequest(ServiceAccountId, ResourceId, promise));

        // TODO make asynchronous interface for GetAuthInfo
        return promise.GetFuture().ExtractValue(TDuration::Seconds(30));
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

} // namespace anonymous

std::shared_ptr<NYdb::ICredentialsProviderFactory> CreateCachingIamServiceCredentialsProviderFactory(const TString& serviceAccountId, const TString& resourceId) {
    auto actorSystem = NActors::TlsActivationContext ? NActors::TlsActivationContext->ActorSystem() : nullptr;
    Y_ENSURE(actorSystem);
    return std::make_shared<TCachingIamServiceCredentialsProviderFactory>(serviceAccountId, resourceId, actorSystem);
}

NActors::TActorId MakeCachingIamServiceCredentialsProviderServiceId() {
    return NActors::TActorId(0, "SvcCachedIAM"sv);
}
} // namespace NYql
