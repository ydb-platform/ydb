#include "read_request_executor.h"

#include "read_request_multiple_location.h"
#include "read_request_single_location.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

IReadRequestExecutorPtr CreateReadRequestExecutor(
    NActors::TActorSystem const* actorSystem,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    TReadHint readHint,
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    if (readHint.RangeHints.size() == 1) {
        return std::make_shared<TReadSingleLocationRequestExecutor>(
            actorSystem,
            vChunkConfig,
            std::move(directBlockGroup),
            std::move(readHint),
            std::move(callContext),
            std::move(request),
            std::move(traceId));
    }

    return std::make_shared<TReadMultipleLocationRequestExecutor>(
        actorSystem,
        vChunkConfig,
        std::move(directBlockGroup),
        std::move(readHint),
        std::move(callContext),
        std::move(request),
        std::move(traceId));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
