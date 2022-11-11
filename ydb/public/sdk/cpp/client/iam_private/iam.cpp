#include "iam.h"

#include <ydb/public/sdk/cpp/client/iam/common/iam.h>

#include <ydb/public/api/client/yc_private/iam/iam_token_service.pb.h>
#include <ydb/public/api/client/yc_private/iam/iam_token_service.grpc.pb.h>

namespace NYdb {

TCredentialsProviderFactoryPtr CreateIamJwtCredentialsProviderFactoryImplPrivate(TIamJwtParams&& jwtParams) {
    return std::make_shared<TIamJwtCredentialsProviderFactory<
                    yandex::cloud::priv::iam::v1::CreateIamTokenRequest,
                    yandex::cloud::priv::iam::v1::CreateIamTokenResponse,
                    yandex::cloud::priv::iam::v1::IamTokenService
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

}
