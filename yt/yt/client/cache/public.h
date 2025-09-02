#pragma once

#include <yt/yt/client/api/public.h>

namespace NYT::NClient::NCache {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IClientsCache)
DECLARE_REFCOUNTED_STRUCT(TClientsCacheConfig)
DECLARE_REFCOUNTED_STRUCT(TClientsCacheAuthentificationOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NCache
