#pragma once

#include "key.h"

#include <yql/essentials/sql/v1/complete/name/object/simple/schema.h>

#include <yql/essentials/sql/v1/complete/name/cache/cache.h>

namespace NSQLComplete {

    using ISchemaListCache = ICache<TSchemaDescribeCacheKey, TVector<TFolderEntry>>;
    using ISchemaDescribeTableCache = ICache<TSchemaDescribeCacheKey, TMaybe<TTableDetails>>;

    struct TSchemaCaches {
        ISchemaListCache::TPtr List;
        ISchemaDescribeTableCache::TPtr DescribeTable;
    };

    // TODO(YQL-19747): deprecated, migrate YDB CLI
    ISimpleSchema::TPtr MakeCachedSimpleSchema(
        ISchemaListCache::TPtr cache, TString zone, ISimpleSchema::TPtr origin);

    ISimpleSchema::TPtr MakeCachedSimpleSchema(
        TSchemaCaches caches, TString zone, ISimpleSchema::TPtr origin);

} // namespace NSQLComplete
