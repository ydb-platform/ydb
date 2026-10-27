#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range/block_range.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host.h>

#include <ydb/core/nbs/cloud/storage/core/protos/error.pb.h>

#include <library/cpp/threading/future/future.h>

#include <memory>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TAggregatedListPBufferResponse;
struct TListPBufferResponse;
class IDirectBlockGroup;
using IDirectBlockGroupPtr = std::shared_ptr<IDirectBlockGroup>;

////////////////////////////////////////////////////////////////////////////////

// Aggregates ListPBuffers replies from every host of one direct block group.
// The group is held weakly: an in-flight list must not keep the group, and
// therefore its coroutine executor, alive after the owner drops them.
class TRestoreRequestExecutor
    : public std::enable_shared_from_this<TRestoreRequestExecutor>
{
public:
    // `directBlockGroup` is observed weakly for the lifetime of the request.
    explicit TRestoreRequestExecutor(
        std::weak_ptr<IDirectBlockGroup> directBlockGroup);

    // Completes the promise with E_REJECTED when the request is dropped early.
    ~TRestoreRequestExecutor();

    // Starts a list on each host.
    void Run();

    // Completes when every host has replied, or the group is already gone.
    NThreading::TFuture<TAggregatedListPBufferResponse> GetFuture() const;

private:
    void DoRun(THostIndex hostIndex);
    void OnResponse(THostIndex hostIndex, TListPBufferResponse response);
    void Reply(NProto::TError error);

    const std::weak_ptr<IDirectBlockGroup> DirectBlockGroup;

    NThreading::TPromise<TAggregatedListPBufferResponse> Promise =
        NThreading::NewPromise<TAggregatedListPBufferResponse>();

    std::unique_ptr<TAggregatedListPBufferResponse> Response;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
