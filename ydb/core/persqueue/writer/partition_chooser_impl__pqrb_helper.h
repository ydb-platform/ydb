#pragma once

#include "pipe_utils.h"

#include <ydb/core/persqueue/events/global.h>


namespace NKikimr::NPQ::NPartitionChooser {

template<typename TPipeCreator>
class TPQRBHelper {
public:
    TPQRBHelper(ui64 balancerTabletId)
        : BalancerTabletId(balancerTabletId) {
    }

    std::optional<ui32> PartitionId() const {
        return PartitionId_;
    }

    void SendRequest(const NActors::TActorContext& ctx) {
        Y_ABORT_UNLESS(BalancerTabletId);

        if (!Pipe) {
            NTabletPipe::TClientConfig clientConfig;
            clientConfig.RetryPolicy = {
                .RetryLimitCount = 6,
                .MinRetryTime = TDuration::MilliSeconds(10),
                .MaxRetryTime = TDuration::MilliSeconds(100),
                .BackoffMultiplier = 2,
                .DoFirstRetryInstantly = true
            };
            Pipe = ctx.RegisterWithSameMailbox(TPipeCreator::CreateClient(ctx.SelfID, BalancerTabletId, clientConfig));
        }

        NTabletPipe::SendData(ctx, Pipe, new TEvPersQueue::TEvGetPartitionIdForWrite());
    }

    ui32 Handle(TEvPersQueue::TEvGetPartitionIdForWriteResponse::TPtr& ev, const TActorContext& ctx) {
        Close(ctx);

        PartitionId_ = ev->Get()->Record.GetPartitionId();
        return PartitionId_.value();
    }

    void Close(const TActorContext& ctx) {
        if (Pipe) {
            NTabletPipe::CloseClient(ctx, Pipe);
            Pipe = TActorId();
        }
    }

private:
    const ui64 BalancerTabletId;

    TActorId Pipe;
    std::optional<ui32> PartitionId_;
};


} // namespace NKikimr::NPQ::NPartitionChooser
