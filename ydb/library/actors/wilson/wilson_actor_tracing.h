#pragma once

#include <ydb/library/actors/core/event_trace.h>

namespace NWilson {
// Install in TActorSystemSetup before starting executor threads.
std::shared_ptr<NActors::IActorEventTracing> CreateActorEventTracing();
} // namespace NWilson
