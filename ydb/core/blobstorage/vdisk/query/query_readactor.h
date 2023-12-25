#pragma once
#include "defs.h"
#include <ydb/core/blobstorage/vdisk/query/query_readbatch.h>

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

