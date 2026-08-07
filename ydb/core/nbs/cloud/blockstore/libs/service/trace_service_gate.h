#pragma once

#include "public.h"

#include <ydb/library/actors/wilson/wilson_span.h>

#include <library/cpp/threading/hot_swap/hot_swap.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TTraceServiceGate
{
public:
    explicit TTraceServiceGate(ITraceServicePtr traceService);
    virtual ~TTraceServiceGate();

    void Attach(ITraceServicePtr traceService);
    void Detach();

    [[nodiscard]] virtual NWilson::TSpan CreateRootSpan(TStringBuf name);

private:
    struct THolder: public TAtomicRefCount<THolder>
    {
        ITraceServicePtr TraceService;
    };

    THotSwap<THolder> Holder;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
