#include "restore_request.h"

#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <util/datetime/base.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration ListRetryDelay = TDuration::MilliSeconds(100);

}   // namespace

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
    for (THostIndex i = 0; i < DirectBlockGroupHostCount; ++i) {
        DoRun(i);
    }
}

void TRestoreRequestExecutor::DoRun(THostIndex hostIndex)
{
    if (Promise.IsReady()) {
        return;
    }

    auto future = DirectBlockGroup->ListPBuffers(hostIndex);
    future.Subscribe(
        [self = shared_from_this(), hostIndex]   //
        (const NThreading::TFuture<TListPBufferResponse>& f)
        {
            //
            self->OnResponse(hostIndex, UnsafeExtractValue(f));
        });
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
    if (Promise.IsReady()) {
        return;
    }

    if (HasError(response.Error)) {
        // This host has not answered. Ask again. The list is published
        // only when every host has answered.
        DirectBlockGroup->Schedule(
            ListRetryDelay,
            [self = shared_from_this(), hostIndex]()
            {
                self->DoRun(hostIndex);
            });
        return;
    }

    if (Response->Meta.find(hostIndex) != Response->Meta.end()) {
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
