#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/library/yql/providers/common/token_accessor/client/caching_iam_credentials_provider_service.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam_private/iam.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

#include <util/string/cast.h>

namespace NKikimr::NKqp {
namespace {
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
            hFunc(NYql::TEvIamAuthCredentialsProviderService::TEvGetAuthInfoRequest, Handle);
            sFunc(NActors::TEvents::TEvPoison, PassAway);
        )
        void Handle(NYql::TEvIamAuthCredentialsProviderService::TEvGetAuthInfoRequest::TPtr& event) {
            auto& promise = event->Get()->Promise;
            try {
                if (!NKikimr::AppData()->FeatureFlags.GetEnableExternalDataSourceAuthMethodIam()) {
                    throw yexception() << "AUTH_METHOD=IAM is disabled. Please contact your system administrator to enable it";
                }
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
            hFunc(NYql::TEvIamAuthCredentialsProviderService::TEvGetAuthInfoRequest, Handle);
            sFunc(NActors::TEvents::TEvPoison, PassAway);
            );

    void Handle(NYql::TEvIamAuthCredentialsProviderService::TEvGetAuthInfoRequest::TPtr& ev) {
        auto [it, inserted] = Actors.emplace(std::pair { std::move(ev->Get()->ServiceAccountId), std::move(ev->Get()->ResourceId) }, NActors::TActorId {});
        if (inserted) {
            try {
                it->second = Register(new TIamResolverActor(it->first.first, it->first.second));
            } catch(...) {
                ev->Get()->Promise.SetException(std::current_exception());
                Actors.erase(it);
                return;
            }
        }
        Send(ev->Forward(it->second));
    }

    void PassAway() override {
        for (auto& [_, actorId]: Actors) {
            Send(actorId, new NActors::TEvents::TEvPoison());
        }
        TBase::PassAway();
    }

    private:
    THashMap<std::pair<TString, TString>, NActors::TActorId> Actors; // TODO replace with LRU
};
} // namespace {

std::unique_ptr<NActors::IActor> NewCachingIamServiceCredentialsProviderService() {
    return std::make_unique<TIamResolverServiceActor>();
}
} // namespace NKikimr::NKqp {
