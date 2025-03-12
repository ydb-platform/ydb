#include <ydb-cpp-sdk/client/iam_private/common/types.h>

#include <src/client/iam/common/iam.h>

namespace NYdb::inline Dev {

template<typename TRequest, typename TResponse, typename TService>

class TIamServiceCredentialsProviderFactory : public ICredentialsProviderFactory {
public:
    TIamServiceCredentialsProviderFactory(const TIamServiceParams& params) : Params_(params) {}

    TCredentialsProviderPtr CreateProvider() const final {
        return std::make_shared<TGrpcIamCredentialsProvider<TRequest, TResponse, TService>>(Params_,
                    [params = Params_](TRequest& req) {
                        req.set_service_id(params.ServiceId);
                        req.set_microservice_id(params.MicroserviceId);
                        req.set_resource_id(params.ResourceId);
                        req.set_resource_type(params.ResourceType);
                        req.set_target_service_account_id(params.TargetServiceAccountId);
                    }, &TService::Stub::AsyncCreateForService);
    }

private:
    TIamServiceParams Params_;
};

}
