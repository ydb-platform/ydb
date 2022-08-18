#include "impl/iam_impl.h"

#include <ydb/public/sdk/cpp/client/iam/proto/v1/iam_token_service.pb.h>
#include <ydb/public/sdk/cpp/client/iam/proto/v1/iam_token_service.grpc.pb.h>

using namespace yandex::cloud::iam::v1;

namespace NYdb {

TCredentialsProviderFactoryPtr CreateIamJwtFileCredentialsProviderFactory(const TIamJwtFilename& params) {
    TIamJwtParams jwtParams = { params, ReadJwtKeyFile(params.JwtFilename) };
    return std::make_shared<TIamJwtCredentialsProviderFactory<CreateIamTokenRequest,
                                                              CreateIamTokenResponse,
                                                              IamTokenService>>(std::move(jwtParams));
}

TCredentialsProviderFactoryPtr CreateIamJwtParamsCredentialsProviderFactory(const TIamJwtContent& params) {
    TIamJwtParams jwtParams = { params, ParseJwtParams(params.JwtContent) };
    return std::make_shared<TIamJwtCredentialsProviderFactory<CreateIamTokenRequest,
                                                              CreateIamTokenResponse,
                                                              IamTokenService>>(std::move(jwtParams));
}

TCredentialsProviderFactoryPtr CreateIamCredentialsProviderFactory(const TIamHost& params) {
    return std::make_shared<TIamCredentialsProviderFactory>(params);
}

TCredentialsProviderFactoryPtr CreateIamOAuthCredentialsProviderFactory(const TIamOAuth& params) {
    return std::make_shared<TIamOAuthCredentialsProviderFactory<CreateIamTokenRequest,
                                                                CreateIamTokenResponse,
                                                                IamTokenService>>(params);
}

}
