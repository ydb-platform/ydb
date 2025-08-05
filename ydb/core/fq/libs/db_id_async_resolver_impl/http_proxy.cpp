#include "http_proxy.h"

namespace NFq {

NActors::TActorId MakeYqlAnalyticsHttpProxyId() {
    constexpr TStringBuf name = "YQLHTTPROXY";
    return NActors::TActorId(0, name);
}

} // namespace NFq
