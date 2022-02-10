#pragma once

#include "defs.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h>

namespace NKikimr {

IActor* CreateSchemeBoardSchemeCache(NSchemeCache::TSchemeCacheConfig* config);

} // NKikimr
