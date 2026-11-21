#include "restore_request.h"

#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TRestoreRequestExecutor::TRestoreRequestExecutor(
    NActors::TActorSystem* actorSystem,
    IDirectBlockGroupPtr directBlockGroup,
    THostMask hostsToList)
    : ActorSystem(actorSystem)
    , DirectBlockGroup(std::move(directBlockGroup))
    , HostsToList(hostsToList)
    , Response(std::make_unique<TAggregatedListPBufferResponse>())
{}

TRestoreRequestExecutor::~TRestoreRequestExecutor()
{
    if (!Promise.IsReady()) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TRestoreRequestExecutor. Reply not sent");

        Y_ABORT_UNLESS(false);
    }
}

void TRestoreRequestExecutor::Run()
{
    Y_ABORT_UNLESS(!HostsToList.Empty());

    for (const auto host: HostsToList) {
        DoListPBuffers(host);
    }
}

NThreading::TFuture<TAggregatedListPBufferResponse>
TRestoreRequestExecutor::GetFuture() const
{
    return Promise.GetFuture();
}

void TRestoreRequestExecutor::DoListPBuffers(THostIndex hostIndex)
{
    auto future = DirectBlockGroup->ListPBuffers(hostIndex);
    future.Subscribe(
        [self = shared_from_this(), hostIndex]   //
        (const NThreading::TFuture<TListPBufferResponse>& f)
        {
            //
            self->OnResponse(hostIndex, UnsafeExtractValue(f));
        });
}

void TRestoreRequestExecutor::OnResponse(
    THostIndex hostIndex,
    TListPBufferResponse response)
{
    if (HasError(response.Error)) {
        // TODO(#55521): Retry failed lists; publish after all hosts succeed.
        Reply(response.Error);
        return;
    }

    Response->Meta[hostIndex] = std::move(response.Meta);
    if (Response->Meta.size() == HostsToList.Count()) {
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
