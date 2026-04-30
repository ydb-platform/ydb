#pragma once

#include "read_request_executor.h"
#include "vchunk_config.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/direct_block_group.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>

#include <ydb/library/actors/core/actorsystem.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Class works with a single readHint. It performs a request into a 1 source
// location
// ATTENTION: usually you should use fabric method CreateReadRequestExecutor
class TReadSingleLocationRequestExecutor
    : public IReadRequestExecutor
    , public std::enable_shared_from_this<TReadSingleLocationRequestExecutor>
{
public:
    TReadSingleLocationRequestExecutor(
        NActors::TActorSystem const* actorSystem,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        TReadHint readHint,
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId);

    ~TReadSingleLocationRequestExecutor() override;

    void Run() override;

    [[nodiscard]] NThreading::TFuture<TResponse> GetFuture() const override;

private:
    void OnReadResponse(const TDBGReadBlocksResponse& response);
    void Reply(NProto::TError error);

    NActors::TActorSystem const* ActorSystem;
    const TVChunkConfig VChunkConfig;
    const IDirectBlockGroupPtr DirectBlockGroup;
    const TReadHint ReadHint;
    const TCallContextPtr CallContext;
    const std::shared_ptr<TReadBlocksLocalRequest> Request;
    const NWilson::TTraceId TraceId;

    size_t TryNumber = 0;

    NThreading::TPromise<TResponse> Promise =
        NThreading::NewPromise<TResponse>();
};

using TReadSingleLocationRequestExecutorPtr =
    std::shared_ptr<TReadSingleLocationRequestExecutor>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
