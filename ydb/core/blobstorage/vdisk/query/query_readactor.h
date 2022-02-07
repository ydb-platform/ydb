#pragma once
#include "defs.h"

namespace NKikimr {

    class TReadBatcherResult;

    IActor *CreateReadBatcherActor(
        TReadBatcherCtxPtr ctx,
        const TActorId notifyID,
        std::shared_ptr<TReadBatcherResult> result,
        ui8 priority,
        NWilson::TTraceId traceId,
        bool isRepl);

} // NKikimr

