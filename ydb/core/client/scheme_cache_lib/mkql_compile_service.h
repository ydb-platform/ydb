#pragma once

#include <util/generic/ptr.h>
#include <util/system/types.h>

namespace NActors {
class IActor;
} // namespace NActors

namespace NYql {
class IDbSchemeResolver;
} // namespace NYql

namespace NKikimr {

NActors::IActor* CreateMiniKQLCompileService(size_t compileInflightLimit);
NActors::IActor* CreateMiniKQLCompileService(size_t compileInflightLimit, THolder<NYql::IDbSchemeResolver>&& dbSchemeResolver);

} // namespace NKikimr
