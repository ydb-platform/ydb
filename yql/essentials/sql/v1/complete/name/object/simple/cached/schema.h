#pragma once

#include "key.h"

#include <yql/essentials/sql/v1/complete/name/object/simple/schema.h>

#include <yql/essentials/sql/v1/complete/name/cache/cache.h>

namespace NSQLComplete {

    using ISchemaListCache = ICache<TSchemaListCacheKey, TVector<TFolderEntry>>;

    ISimpleSchema::TPtr MakeCachedSimpleSchema(
        ISchemaListCache::TPtr cache, TString zone, ISimpleSchema::TPtr origin);

} // namespace NSQLComplete
