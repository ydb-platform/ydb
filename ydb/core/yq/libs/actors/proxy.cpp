#include "proxy.h"

namespace NFq {

using namespace NActors;

NActors::TActorId MakeYqlAnalyticsHttpProxyId() {
    constexpr TStringBuf name = "YQLHTTPROXY";
    return NActors::TActorId(0, name);
}

} // namespace NFq
