#include "db_key_resolver.h"

#include <util/stream/str.h>


namespace NYql {

IDbSchemeResolver::TEvents::TEvResolveTablesResult::TEvResolveTablesResult(TTableResults&& result)
    : Result(std::move(result))
{
}

} // namespace NYql
