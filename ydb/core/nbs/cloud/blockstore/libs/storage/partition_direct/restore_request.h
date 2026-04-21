#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>

#include <ydb/core/nbs/cloud/storage/core/protos/error.pb.h>

#include <ydb/library/actors/core/actorsystem.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TAggregatedListPBufferResponse;
struct TListPBufferResponse;
class IDirectBlockGroup;
using IDirectBlockGroupPtr = std::shared_ptr<IDirectBlockGroup>;

////////////////////////////////////////////////////////////////////////////////

class TRestoreRequestExecutor
    : public std::enable_shared_from_this<TRestoreRequestExecutor>
{
public:
    TRestoreRequestExecutor(
        NActors::TActorSystem* actorSystem,
        IDirectBlockGroupPtr directBlockGroup);

    ~TRestoreRequestExecutor();

    void Run();

    NThreading::TFuture<TAggregatedListPBufferResponse> GetFuture() const;

private:
    void DoRun(ui8 hostIndex);
    void OnResponse(ui8 hostIndex, TListPBufferResponse response);
    void Reply(NProto::TError error);

    NActors::TActorSystem const* ActorSystem;
    const IDirectBlockGroupPtr DirectBlockGroup;

    NThreading::TPromise<TAggregatedListPBufferResponse> Promise =
        NThreading::NewPromise<TAggregatedListPBufferResponse>();

    std::unique_ptr<TAggregatedListPBufferResponse> Response;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
