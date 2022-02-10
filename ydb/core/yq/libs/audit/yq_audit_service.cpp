#include <library/cpp/actors/core/actor.h>

#include <util/system/types.h>


namespace NYq {

NActors::TActorId YqAuditServiceActorId() {
    constexpr TStringBuf name = "YQAUDSVC";
    return NActors::TActorId(0, name);
}

} // namespace NYq
