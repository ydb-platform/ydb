#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <util/generic/fwd.h>

namespace NYql {

std::shared_ptr<NYdb::ICredentialsProviderFactory> CreateCachingIamServiceCredentialsProviderFactory(const TString& serviceAccountId, const TString& resourceId);

}
