#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// The wrapper over iStorage protects the underlying layers from executing
// overlapping zero or write requests.
// If a request is received that intersects with an already executing one, it is
// postponed until the completion of the executing one.
// If the new request is completely covered by the executing one, then such a
// request can not be executed, but wait for completion and respond to both
// requests to the client. This is possible because inflight requests can be
// reordered, and we pretend that the #2 request was executed first, and was
// then completely rewritten by #1.
// Read requests are not affected in any way.
class TOverlappedRequestsGuardStorageWrapper final
    : public IStorage
    , public std::enable_shared_from_this<
          TOverlappedRequestsGuardStorageWrapper>
{
private:
    struct TInflight;

    const IStoragePtr Storage;

    TAdaptiveLock Lock;
    ui64 RequestIdGenerator = 0;
    TBlockRangeMap<ui64, std::unique_ptr<TInflight>> InflightRequests;

public:
    explicit TOverlappedRequestsGuardStorageWrapper(IStoragePtr storage);
    ~TOverlappedRequestsGuardStorageWrapper() override;

    // implements IStorage
    NThreading::TFuture<TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request) override;

    NThreading::TFuture<TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request) override;

    NThreading::TFuture<TZeroBlocksLocalResponse> ZeroBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TZeroBlocksLocalRequest> request) override;

    void ReportIOError() override;

private:
    void OnRequestFinished(ui64 requestId, const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
