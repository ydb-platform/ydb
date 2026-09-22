#include "restore_request.h"

#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TRestoreRequestExecutor::TRestoreRequestExecutor(
    std::weak_ptr<IDirectBlockGroup> directBlockGroup)
    : DirectBlockGroup(std::move(directBlockGroup))
    , Response(std::make_unique<TAggregatedListPBufferResponse>())
{}

TRestoreRequestExecutor::~TRestoreRequestExecutor()
{
    if (!Promise.IsReady()) {
        // The group (or the transport future that kept this request) is gone
        // before every host replied. Complete the promise so waiters unblock.
        Reply(MakeError(E_REJECTED, "TDirectBlockGroup destroyed"));
    }
}

void TRestoreRequestExecutor::Run()
{
    if (DirectBlockGroup.expired()) {
        Reply(MakeError(E_REJECTED, "TDirectBlockGroup destroyed"));
        return;
    }

    for (THostIndex i = 0; i < DirectBlockGroupHostCount; ++i) {
        DoRun(i);
    }
}

void TRestoreRequestExecutor::DoRun(THostIndex hostIndex)
{
    auto directBlockGroup = DirectBlockGroup.lock();
    if (!directBlockGroup) {
        OnResponse(
            hostIndex,
            TListPBufferResponse{
                .Error = MakeError(E_REJECTED, "TDirectBlockGroup destroyed")});
        return;
    }

    auto future = directBlockGroup->ListPBuffers(hostIndex);
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
    if (Promise.IsReady()) {
        return;
    }

    Response->Error = std::move(error);
    Promise.TrySetValue(std::move(*Response));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
