#include <ydb/library/actors/core/actor.h>

#include <util/system/types.h>


namespace NFq {

NActors::TActorId YqAuditServiceActorId() {
    constexpr TStringBuf name = "YQAUDSVC";
    return NActors::TActorId(0, name);
}

} // namespace NFq
