#pragma once

#include "state_storage.h"

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/core/yq/libs/config/protos/storage.pb.h>

namespace NYq {

////////////////////////////////////////////////////////////////////////////////

TStateStoragePtr NewYdbStateStorage( 
    const NConfig::TYdbStorageConfig& config, 
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory); 

} // namespace NYq
