#pragma once

#include <memory>
#include <util/system/types.h>

namespace NActors {
class IEventHandle;
class TActorSystem;

// Optional instrumentation owned by the embedding application. The actor core
// does not depend on a tracing exporter, and the hook stays fixed after setup.
class IActorTraceSpan {
public:
    virtual ~IActorTraceSpan() = default;
    virtual void Finish(bool acceptedOrReturned) = 0;
};

class IActorEventTracing {
public:
    virtual ~IActorEventTracing() = default;
    virtual std::unique_ptr<IActorTraceSpan> BeginMailboxSubmit(IEventHandle& event,
        TActorSystem* system, ui32 pool) const = 0;
    virtual std::unique_ptr<IActorTraceSpan> BeginHandler(IEventHandle& event,
        TActorSystem* system, ui32 pool, ui32 activity) const = 0;
};
} // namespace NActors
