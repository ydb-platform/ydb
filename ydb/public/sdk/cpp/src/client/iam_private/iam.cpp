#include "common/iam.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam_private/iam.h>

#include <ydb/public/api/client/yc_private/iam/iam_token_service.pb.h>
#include <ydb/public/api/client/yc_private/iam/iam_token_service.grpc.pb.h>
#include <mutex>
#include <string>
#include <util/generic/hash.h>

using namespace yandex::cloud::priv::iam::v1;

namespace NYdb::inline Dev {

TCredentialsProviderFactoryPtr CreateIamJwtCredentialsProviderFactoryImplPrivate(TIamJwtParams&& jwtParams) {
    return std::make_shared<TIamJwtCredentialsProviderFactory<
                    CreateIamTokenRequest,
                    CreateIamTokenResponse,
                    IamTokenService
                >>(std::move(jwtParams));
}

TCredentialsProviderFactoryPtr CreateIamJwtFileCredentialsProviderFactoryPrivate(const TIamJwtFilename& params) {
    TIamJwtParams jwtParams = { params, ReadJwtKeyFile(params.JwtFilename) };
    return CreateIamJwtCredentialsProviderFactoryImplPrivate(std::move(jwtParams));
}

TCredentialsProviderFactoryPtr CreateIamJwtParamsCredentialsProviderFactoryPrivate(const TIamJwtContent& params) {
    TIamJwtParams jwtParams = { params, ParseJwtParams(params.JwtContent) };
    return CreateIamJwtCredentialsProviderFactoryImplPrivate(std::move(jwtParams));
}

class TIamserviceProviderFactories{
    std::mutex Mutex;
    THashMap<std::string, TCredentialsProviderFactoryPtr> Factories;
    std::string GetKey(const TIamServiceParams& params) {
        return TStringBuilder()
                << '\t' << params.ServiceId
                << '\t' << params.MicroserviceId
                << '\t' << params.ResourceId
                << '\t' << params.ResourceType
                << '\t' << params.TargetServiceAccountId
                ;
        
    }
public:
    TCredentialsProviderFactoryPtr GetFactory(const TIamServiceParams& params) {
        std::lock_guard<std::mutex> lock(Mutex);
        auto key = GetKey(params);
        const auto p = Factories.FindPtr(key);
        if (p) {
            return *p;
        }
        Factories[key] = std::make_shared<TIamServiceCredentialsProviderFactory<
                CreateIamTokenForServiceRequest,
                CreateIamTokenResponse,
                IamTokenService
            >>(params);
        return Factories[key];
    }
};



TCredentialsProviderFactoryPtr CreateIamServiceCredentialsProviderFactory(const TIamServiceParams& params) {
    static TIamserviceProviderFactories iamServiceProviderFactories;
    return iamServiceProviderFactories.GetFactory(params);
}

}
