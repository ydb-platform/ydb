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
        // TDriver path: a shared facility (TGRpcConnectionsImpl) supports multiple periodic tasks,
        // so we can hand the same weak_ptr to the nested auth provider here.
        TCredentialsProvider(const TIamServiceParams& params, std::weak_ptr<ICoreFacility> responseFacility)
            : TGrpcIamCredentialsProvider<TRequest, TResponse, TService>(params,
                MakeRequestFiller(params),
                MakeRpc(),
                responseFacility,
                params.SystemServiceAccountCredentials->CreateProvider(responseFacility))
        {}

        // Standalone (no-arg) path: the caller has already built a self-owning auth provider
        // backed by its OWN facility. We must not share `outerFacility` with the auth provider
        // because TSimpleCoreFacility allows only one periodic task.
        TCredentialsProvider(const TIamServiceParams& params,
                             std::weak_ptr<ICoreFacility> outerFacility,
                             TCredentialsProviderPtr authProvider)
            : TGrpcIamCredentialsProvider<TRequest, TResponse, TService>(params,
                MakeRequestFiller(params),
                MakeRpc(),
                std::move(outerFacility),
                std::move(authProvider))
        {}
    };

public:
    TIamServiceCredentialsProviderFactory(const TIamServiceParams& params)
        : Params_(params)
    {}

    // Deprecated. Kept for backward compatibility — see comment on TIamJwtCredentialsProviderFactory.
    // The nested auth provider gets its own facility (via a recursive no-arg CreateProvider() that
    // returns a TOwningFacilityCredentialsProvider). Sharing a TSimpleCoreFacility between two gRPC
    // IAM providers would abort: each one registers a periodic task and the facility allows only one.
    TCredentialsProviderPtr CreateProvider() const override final {
        auto authProvider = Params_.SystemServiceAccountCredentials->CreateProvider();
        auto outerFacility = CreateSimpleCoreFacility();
        auto serviceProvider = std::make_shared<TCredentialsProvider>(
            Params_, std::weak_ptr<ICoreFacility>(outerFacility), std::move(authProvider));
        return std::make_shared<TOwningFacilityCredentialsProvider>(
            std::move(outerFacility), std::move(serviceProvider));
    }

    TCredentialsProviderPtr CreateProvider(std::weak_ptr<ICoreFacility> facility) const override {
        return std::make_shared<TCredentialsProvider>(Params_, std::move(facility));
    }

private:
    TIamServiceParams Params_;
};

}
