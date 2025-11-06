#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam_private/common/types.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/generic_provider.h>

namespace NYdb::inline Dev {

template<typename TRequest, typename TResponse, typename TService>
class TIamServiceCredentialsProviderFactory : public ICredentialsProviderFactory {
private:
    class TCredentialsProvider : public TGrpcIamCredentialsProvider<TRequest, TResponse, TService> {
    public:
        TCredentialsProvider(const TIamServiceParams& params) 
            : TGrpcIamCredentialsProvider<TRequest, TResponse, TService>(params,
                [&params](TRequest& req) {
                    req.set_service_id(params.ServiceId);
                    req.set_microservice_id(params.MicroserviceId);
                    req.set_resource_id(params.ResourceId);
                    req.set_resource_type(params.ResourceType);
                    req.set_target_service_account_id(params.TargetServiceAccountId);
                },
                [](typename TService::Stub* stub, grpc::ClientContext* context, const TRequest* request, TResponse* response, std::function<void(grpc::Status)> cb) {
                    stub->async()->CreateForService(context, request, response, std::move(cb));
                },
                params.SystemServiceAccountCredentials->CreateProvider()) {}
    };

public:
    TIamServiceCredentialsProviderFactory(const TIamServiceParams& params)
        : Params_(params) 
    {}

    TCredentialsProviderPtr CreateProvider() const override final {
        return std::make_shared<TCredentialsProvider>(Params_);
    }

private:
    TIamServiceParams Params_;
};

}
