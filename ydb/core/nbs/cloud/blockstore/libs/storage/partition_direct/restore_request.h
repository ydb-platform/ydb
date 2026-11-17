#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_mask.h>

#include <ydb/core/nbs/cloud/storage/core/protos/error.pb.h>

#include <ydb/library/actors/core/actorsystem.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TAggregatedListPBufferResponse;
struct TListPBufferResponse;
class IDirectBlockGroup;
using IDirectBlockGroupPtr = std::shared_ptr<IDirectBlockGroup>;

////////////////////////////////////////////////////////////////////////////////

// Collects PBuffer metadata from a fixed set of non-removed DBG slots.
class TRestoreRequestExecutor
    : public std::enable_shared_from_this<TRestoreRequestExecutor>
{
public:
    // The host mask includes temporarily unavailable slots.
    TRestoreRequestExecutor(
        NActors::TActorSystem* actorSystem,
        IDirectBlockGroupPtr directBlockGroup,
        THostMask hostsToList);

    ~TRestoreRequestExecutor();

    void Run();

    NThreading::TFuture<TAggregatedListPBufferResponse> GetFuture() const;

private:
    void DoListPBuffers(THostIndex hostIndex);
    void OnResponse(THostIndex hostIndex, TListPBufferResponse response);
    void Reply(NProto::TError error);

    NActors::TActorSystem const* ActorSystem;
    const IDirectBlockGroupPtr DirectBlockGroup;
    const THostMask HostsToList;

    NThreading::TPromise<TAggregatedListPBufferResponse> Promise =
        NThreading::NewPromise<TAggregatedListPBufferResponse>();

    std::unique_ptr<TAggregatedListPBufferResponse> Response;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
