#pragma once

#include "public.h"

#include "direct_block_group.h"
#include "request_executor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_checksums.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/log_title.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <ydb/library/actors/core/actorsystem.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class IReadRequestExecutor: public IRequestExecutor
{
public:
    struct TResponse
    {
        NProto::TError Error;

        // Same contract as TDBGReadBlocksResponse::Checksums. A single-location
        // read forwards the DBG vector. A multi-location read joins pieces by
        // byte offset. With checksums enabled every piece is complete, so the
        // joined vector is complete. With checksums disabled every piece is
        // empty, so this stays empty.
        TBlockChecksums Checksums;
    };

    [[nodiscard]] virtual NThreading::TFuture<TResponse> GetFuture() const = 0;
};

using IReadRequestExecutorPtr = std::shared_ptr<IReadRequestExecutor>;

// Fabric method for creation appropriate executor based on readHints size
IReadRequestExecutorPtr CreateReadRequestExecutor(
    NActors::TActorSystem const* actorSystem,
    const TLogTitle& logTitle,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    TReadHint readHint,
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
