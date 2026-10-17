#include "events.h"

#include <ydb/core/base/events.h>

namespace NFq {

static_assert((int)NKikimr::TKikimrEvents::EEventSpaceKikimr::ES_YQL_ANALYTICS_PROXY == (int)TEventIds::ES_YQL_ANALYTICS_PROXY);

NActors::TActorId MakeYqPrivateProxyId() {
    constexpr TStringBuf name = "YQPRIVPROXY";
    return NActors::TActorId(0, name);
}

} // namespace NFq
