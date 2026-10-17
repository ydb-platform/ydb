#pragma once

#include "read_request_executor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/log_title.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/direct_block_group.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/read_request_single_location.h>

#include <ydb/library/actors/core/actorsystem.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Class works with a multiple readHints.
// It encapsulates logic of splitting original request into N subrequests,
// sending them to different sources and collecting responses.
// ATTENTION: you should use factory method CreateReadRequestExecutor().
class TReadMultipleLocationRequestExecutor
    : public IReadRequestExecutor
    , public std::enable_shared_from_this<TReadMultipleLocationRequestExecutor>
{
public:
    TReadMultipleLocationRequestExecutor(
        NActors::TActorSystem const* actorSystem,
        const TLogTitle& logTitle,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        TReadHint readHint,
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId);

    ~TReadMultipleLocationRequestExecutor() override;

    // Implementation of IRequestExecutor
    void Run() override;
    TString Print() override;

    // Implementation of IReadRequestExecutor
    [[nodiscard]] NThreading::TFuture<TResponse> GetFuture() const override;

private:
    // Checksum-unit span of one sub-request inside the parent read.
    struct TSubRequestChecksumPlace
    {
        size_t Offset = 0;
        size_t Count = 0;
    };

    void OnSubRequestComplete(const TResponse& response, size_t index);
    // Copies a complete piece into AssembledChecksums. An empty piece means
    // checksums are disabled and is skipped. Any other length aborts: the
    // DBG never returns a partial vector on success.
    void AcceptSubRequestChecksums(
        const TBlockChecksums& checksums,
        size_t index);
    void Reply(NProto::TError error, TBlockChecksums checksums, size_t index);

    NActors::TActorSystem const* ActorSystem;
    const TChildLogTitle LogTitle;
    const TVChunkConfig VChunkConfig;
    const IDirectBlockGroupPtr DirectBlockGroup;
    const TCallContextPtr CallContext;
    const std::shared_ptr<TReadBlocksLocalRequest> Request;
    const NWilson::TTraceId TraceId;

    TGuardedSgList SgList;
    TVector<TReadSingleLocationRequestExecutorPtr> SubRequestExecutors;
    // Parallel to SubRequestExecutors. Offsets are hint.RequestRelativeRange
    // converted to checksum units.
    TVector<TSubRequestChecksumPlace> SubRequestChecksumPlaces;
    // Parent read length in checksum units. Allocated into
    // AssembledChecksums on the first complete piece.
    size_t ParentChecksumCount = 0;
    TBlockChecksums AssembledChecksums;
    // How many checksum units have been copied. Zero means every piece was
    // empty. Equal to ParentChecksumCount means the join is complete.
    size_t AssembledChecksumCount = 0;
    size_t CompletedCount{0};
    NThreading::TPromise<TResponse> Promise =
        NThreading::NewPromise<TResponse>();
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
