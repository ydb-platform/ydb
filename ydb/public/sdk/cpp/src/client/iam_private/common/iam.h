#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam_private/common/types.h>

#include <ydb/public/sdk/cpp/src/client/iam/common/iam.h>

namespace NYdb::inline Dev {

template<typename TRequest, typename TResponse, typename TService>

class TIamServiceCredentialsProviderFactory : public ICredentialsProviderFactory {
<<<<<<< HEAD
=======
private:
    class TCredentialsProvider : public TGrpcIamCredentialsProvider<TRequest, TResponse, TService> {
    public:
        TCredentialsProvider(const TIamServiceParams& params, std::weak_ptr<ICoreFacility> responseFacility)
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
                responseFacility,
                params.SystemServiceAccountCredentials->CreateProvider(responseFacility))
        {}
    };

>>>>>>> 0140ad83476 (fix sdk: fixed self thread join in iam cred provider (#39506))
public:
    TIamServiceCredentialsProviderFactory(const TIamServiceParams& params) : Params_(params) {}

<<<<<<< HEAD
    TCredentialsProviderPtr CreateProvider() const final {
        return std::make_shared<TGrpcIamCredentialsProvider<TRequest, TResponse, TService>>(Params_,
                    [params = Params_](TRequest& req) {
                        req.set_service_id(params.ServiceId);
                        req.set_microservice_id(params.MicroserviceId);
                        req.set_resource_id(params.ResourceId);
                        req.set_resource_type(params.ResourceType);
                        req.set_target_service_account_id(params.TargetServiceAccountId);
                    }, &TService::Stub::AsyncCreateForService);
=======
    TCredentialsProviderPtr CreateProvider() const override final {
        ythrow yexception() << "Not supported";
    }

    TCredentialsProviderPtr CreateProvider(std::weak_ptr<ICoreFacility> facility) const override {
        return std::make_shared<TCredentialsProvider>(Params_, std::move(facility));
>>>>>>> 0140ad83476 (fix sdk: fixed self thread join in iam cred provider (#39506))
    }

private:
    TIamServiceParams Params_;
};

}
