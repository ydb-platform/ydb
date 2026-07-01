#include "events.h"

namespace NRetroTracing {

TEvCollectRetroTrace::TEvCollectRetroTrace(const NWilson::TTraceId& traceId)
    : TraceId(traceId)
{}

}  // namespace NRetroTracing
