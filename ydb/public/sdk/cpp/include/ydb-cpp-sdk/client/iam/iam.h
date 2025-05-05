#pragma once

#include "common/types.h"

namespace NYdb::inline Dev {

/// Acquire an IAM token using a local metadata service on a virtual machine.
TCredentialsProviderFactoryPtr CreateIamCredentialsProviderFactory(const TIamHost& params = {});

/// Acquire an IAM token using a JSON Web Token (JWT) file name.
TCredentialsProviderFactoryPtr CreateIamJwtFileCredentialsProviderFactory(const TIamJwtFilename& params);

/// Acquire an IAM token using JSON Web Token (JWT) contents.
TCredentialsProviderFactoryPtr CreateIamJwtParamsCredentialsProviderFactory(const TIamJwtContent& param);

// Acquire an IAM token using a user OAuth token.
TCredentialsProviderFactoryPtr CreateIamOAuthCredentialsProviderFactory(const TIamOAuth& params);

}
