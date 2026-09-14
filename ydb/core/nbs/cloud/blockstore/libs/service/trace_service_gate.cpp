
#include "trace_service_gate.h"

#include "trace_service.h"

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TTraceServiceGate::TTraceServiceGate(ITraceServicePtr traceService)
{
    Attach(std::move(traceService));
}

TTraceServiceGate::~TTraceServiceGate() = default;

void TTraceServiceGate::Attach(ITraceServicePtr traceService)
{
    auto newHolder = MakeIntrusive<THolder>();
    newHolder->TraceService = std::move(traceService);
    Holder.AtomicStore(newHolder);
}

void TTraceServiceGate::Detach()
{
    Holder.AtomicStore(nullptr);
}

NWilson::TSpan TTraceServiceGate::CreateRootSpan(TStringBuf name)
{
    auto storageHolder = Holder.AtomicLoad();
    if (!storageHolder) {
        return {};
    }
    return storageHolder->TraceService->CreateRootSpan(name);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
