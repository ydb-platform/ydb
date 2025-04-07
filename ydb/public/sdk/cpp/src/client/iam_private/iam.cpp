#include "common/iam.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam_private/iam.h>

#include <ydb/public/api/client/yc_private/iam/iam_token_service.pb.h>
#include <ydb/public/api/client/yc_private/iam/iam_token_service.grpc.pb.h>

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

TCredentialsProviderFactoryPtr CreateIamServiceCredentialsProviderFactory(const TIamServiceParams& params) {
    return std::make_shared<TIamServiceCredentialsProviderFactory<
                    CreateIamTokenForServiceRequest,
                    CreateIamTokenResponse,
                    IamTokenService
                >>(std::move(params));
}

}
