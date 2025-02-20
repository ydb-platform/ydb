#pragma once

#include <ydb-cpp-sdk/client/iam/common/types.h>

namespace NYdb::inline V3 {

struct TIamServiceParams : TIamEndpoint {
    std::string ServiceId;
    std::string MicroserviceId;
    std::string ResourceId;
    std::string ResourceType;
    std::string TargetServiceAccountId;
};

/// Acquire an IAM token using a JSON Web Token (JWT) file name.
TCredentialsProviderFactoryPtr CreateIamJwtFileCredentialsProviderFactoryPrivate(const TIamJwtFilename& params);

/// Acquire an IAM token using JSON Web Token (JWT) contents.
TCredentialsProviderFactoryPtr CreateIamJwtParamsCredentialsProviderFactoryPrivate(const TIamJwtContent& param);

/// Acquire an IAM token for system service account (SSA).
TCredentialsProviderFactoryPtr CreateIamServiceCredentialsProviderFactory(const TIamServiceParams& params);

} // namespace NYdb
