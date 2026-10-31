#include "restore_request.h"

#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TRestoreRequestExecutor::TRestoreRequestExecutor(
    NActors::TActorSystem* actorSystem,
    IDirectBlockGroupPtr directBlockGroup)
    : ActorSystem(actorSystem)
    , DirectBlockGroup(std::move(directBlockGroup))
    , Response(std::make_unique<TAggregatedListPBufferResponse>())
{}

TRestoreRequestExecutor::~TRestoreRequestExecutor()
{
    if (Promise.IsReady()) {
        return;
    }

    // Shutdown drops the executor while a list is still in flight. Reply so
    // subscribers release this object instead of aborting the process.
    LOG_ERROR(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "TRestoreRequestExecutor destroyed before reply");

    Reply(MakeError(E_REJECTED, "TRestoreRequestExecutor destroyed"));
}

void TRestoreRequestExecutor::Run()
{
    const auto directBlockGroup = DirectBlockGroup.lock();
    if (!directBlockGroup) {
        Reply(MakeError(E_REJECTED, "DirectBlockGroup destroyed"));
        return;
    }

    for (THostIndex i = 0; i < DirectBlockGroupHostCount; ++i) {
        DoRun(*directBlockGroup, i);
    }
}

void TRestoreRequestExecutor::DoRun(
    IDirectBlockGroup& directBlockGroup,
    THostIndex hostIndex)
{
    auto future = directBlockGroup.ListPBuffers(hostIndex);
    future.Subscribe([self = shared_from_this(), hostIndex]   //
                     (const NThreading::TFuture<TListPBufferResponse>& f)
                     { self->OnResponse(hostIndex, UnsafeExtractValue(f)); });
}

NThreading::TFuture<TAggregatedListPBufferResponse>
TRestoreRequestExecutor::GetFuture() const
{
    return Promise.GetFuture();
}

void TRestoreRequestExecutor::OnResponse(
    THostIndex hostIndex,
    TListPBufferResponse response)
{
    if (HasError(response.Error)) {
        Reply(response.Error);
        return;
    }

    Response->Meta[hostIndex] = std::move(response.Meta);
    if (Response->Meta.size() == DirectBlockGroupHostCount) {
        Reply(MakeError(S_OK));
    }
}

void TRestoreRequestExecutor::Reply(NProto::TError error)
{
    Response->Error = std::move(error);
    Promise.TrySetValue(std::move(*Response));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
