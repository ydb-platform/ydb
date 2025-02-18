#pragma once

#include <ydb-cpp-sdk/client/iam/common/types.h>

namespace NYdb::inline V3 {

/// Acquire an IAM token using a JSON Web Token (JWT) file name.
TCredentialsProviderFactoryPtr CreateIamJwtFileCredentialsProviderFactoryPrivate(const TIamJwtFilename& params);

/// Acquire an IAM token using JSON Web Token (JWT) contents.
TCredentialsProviderFactoryPtr CreateIamJwtParamsCredentialsProviderFactoryPrivate(const TIamJwtContent& param);

} // namespace NYdb
