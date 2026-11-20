#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range/block_range.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host.h>

#include <ydb/core/nbs/cloud/storage/core/protos/error.pb.h>

#include <ydb/library/actors/core/actorsystem.h>

#include <memory>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TAggregatedListPBufferResponse;
struct TListPBufferResponse;
class IDirectBlockGroup;
using IDirectBlockGroupPtr = std::shared_ptr<IDirectBlockGroup>;

////////////////////////////////////////////////////////////////////////////////

// Lists persistent-buffer records on every host while a direct block group
// starts. The group is held weakly so in-flight list subscriptions cannot
// keep the group and its coroutine executor alive after shutdown.
class TRestoreRequestExecutor
    : public std::enable_shared_from_this<TRestoreRequestExecutor>
{
public:
    // `directBlockGroup` must outlive Run(); afterwards only a weak reference
    // is kept.
    TRestoreRequestExecutor(
        NActors::TActorSystem* actorSystem,
        IDirectBlockGroupPtr directBlockGroup);

    // Completes the promise with E_REJECTED when Run() did not reply.
    ~TRestoreRequestExecutor();

    // Lists PBuffers on every host. May block on the group executor.
    void Run();

    // Ready when listing finishes or this object is destroyed.
    NThreading::TFuture<TAggregatedListPBufferResponse> GetFuture() const;

private:
    void DoRun(IDirectBlockGroup& directBlockGroup, THostIndex hostIndex);
    void OnResponse(THostIndex hostIndex, TListPBufferResponse response);
    void Reply(NProto::TError error);

    NActors::TActorSystem const* ActorSystem;
    const std::weak_ptr<IDirectBlockGroup> DirectBlockGroup;

    NThreading::TPromise<TAggregatedListPBufferResponse> Promise =
        NThreading::NewPromise<TAggregatedListPBufferResponse>();

    std::unique_ptr<TAggregatedListPBufferResponse> Response;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
