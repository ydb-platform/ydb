#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam_private/common/types.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/generic_provider.h>

namespace NYdb::inline Dev {

template<typename TRequest, typename TResponse, typename TService>
class TIamServiceCredentialsProviderFactory : public ICredentialsProviderFactory {
private:
    static auto MakeRequestFiller(TIamServiceParams params) {
        return [params = std::move(params)](TRequest& req) {
            req.set_service_id(params.ServiceId);
            req.set_microservice_id(params.MicroserviceId);
            req.set_resource_id(params.ResourceId);
            req.set_resource_type(params.ResourceType);
            req.set_target_service_account_id(params.TargetServiceAccountId);
        };
    }

    static auto MakeRpc() {
        return [](typename TService::Stub* stub, grpc::ClientContext* context, const TRequest* request, TResponse* response, std::function<void(grpc::Status)> cb) {
            stub->async()->CreateForService(context, request, response, std::move(cb));
        };
    }

    class TCredentialsProvider : public TGrpcIamCredentialsProvider<TRequest, TResponse, TService> {
    public:
        TCredentialsProvider(const TIamServiceParams& params,
                             std::weak_ptr<ICoreFacility> responseFacility,
                             TCredentialsProviderPtr authProvider = {})
            : TGrpcIamCredentialsProvider<TRequest, TResponse, TService>(params,
                MakeRequestFiller(params),
                MakeRpc(),
                responseFacility,
                authProvider ? std::move(authProvider) :
                    params.SystemServiceAccountCredentials->CreateProvider(responseFacility))
        {}
    };

public:
    TIamServiceCredentialsProviderFactory(const TIamServiceParams& params)
        : Params_(params)
    {}

    TCredentialsProviderPtr CreateProvider() const override final {
        return NCredentials::NDetail::GetOrCreateCachedProvider(
            GetClientIdentity(),
            [this] {
                auto authProvider = NCredentials::NDetail::GetOrCreateCachedProvider(
                    "async:" + Params_.SystemServiceAccountCredentials->GetClientIdentity(), [this] {
                        auto facility = CreateSimpleCoreFacility();
                        return std::make_shared<TOwningFacilityCredentialsProvider>(facility,
                            Params_.SystemServiceAccountCredentials->CreateProvider(facility), true);
                    });
                auto facility = CreateSimpleCoreFacility();
                auto serviceProvider = std::make_shared<TCredentialsProvider>(
                    Params_, facility, std::move(authProvider));
                return std::make_shared<TOwningFacilityCredentialsProvider>(
                    std::move(facility), std::move(serviceProvider));
            });
    }

    TCredentialsProviderPtr CreateProvider(std::weak_ptr<ICoreFacility> facility) const override {
        return std::make_shared<TCredentialsProvider>(Params_, std::move(facility));
    }

    std::string GetClientIdentity() const override final {
        return NIam::NDetail::MakeClientIdentity(
            "TIamServiceCredentialsProviderFactory",
            Params_,
            TService::service_full_name(),
            Params_.ServiceId,
            Params_.MicroserviceId,
            Params_.ResourceId,
            Params_.ResourceType,
            Params_.TargetServiceAccountId,
            Params_.SystemServiceAccountCredentials->GetClientIdentity());
    }

private:
    TIamServiceParams Params_;
};

}
