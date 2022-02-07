#pragma once
#include <ydb/core/base/defs.h>

namespace NYql {
class IDbSchemeResolver;
} // namespace NYql

namespace NActors {
    class TActorSystem;
    struct TActorId;
}

namespace NKikimr {
namespace NSchCache {

NYql::IDbSchemeResolver* CreateDbSchemeResolver(TActorSystem *actorSystem, const TActorId &schemeCacheActor);

} // namespace NSchCache
} // namespace NKikimr
