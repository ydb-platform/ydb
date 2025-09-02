#pragma once

#include "common.h"
#include "pipe_utils.h"
#include "source_id_encoding.h"

#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

namespace NKikimr::NPQ::NPartitionChooser {

template<typename TPipeCreator>
class TPartitionHelper {
public:
    TPartitionHelper(NWilson::TTraceId traceId)
        : TraceId(std::move(traceId))
    {
    }

    void Open(ui64 tabletId, const TActorContext& ctx) {
        Close(ctx);

        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = {
            .RetryLimitCount = 6,
            .MinRetryTime = TDuration::MilliSeconds(10),
            .MaxRetryTime = TDuration::MilliSeconds(100),
            .BackoffMultiplier = 2,
            .DoFirstRetryInstantly = true
        };
        Pipe = ctx.RegisterWithSameMailbox(TPipeCreator::CreateClient(ctx.SelfID, tabletId, clientConfig));
    }

    void SendGetOwnershipRequest(ui32 partitionId, const TString& sourceId, bool registerIfNotExists, const TActorContext& ctx) {
        auto ev = MakeRequest(partitionId, Pipe);

        auto& cmd = *ev->Record.MutablePartitionRequest()->MutableCmdGetOwnership();
        cmd.SetOwner(sourceId ? sourceId : CreateGuidAsString());
        cmd.SetForce(true);
        cmd.SetRegisterIfNotExists(registerIfNotExists);

        NTabletPipe::SendData(ctx, Pipe, ev.Release(), 0, NWilson::TTraceId(TraceId));
    }

    void SendMaxSeqNoRequest(ui32 partitionId, const TString& sourceId, const TActorContext& ctx) {
        auto ev = MakeRequest(partitionId, Pipe);

        auto& cmd = *ev->Record.MutablePartitionRequest()->MutableCmdGetMaxSeqNo();
        cmd.AddSourceId(NSourceIdEncoding::EncodeSimple(sourceId));

        NTabletPipe::SendData(ctx, Pipe, ev.Release(), 0, NWilson::TTraceId(TraceId));
    }

    void SendCheckPartitionStatusRequest(ui32 partitionId, const TString& sourceId, const TActorContext& ctx) {
        auto ev = MakeHolder<NKikimr::TEvPQ::TEvCheckPartitionStatusRequest>(partitionId);
        if (sourceId) {
            ev->Record.SetSourceId(sourceId);
        }

        NTabletPipe::SendData(ctx, Pipe, ev.Release(), 0, NWilson::TTraceId(TraceId));
    }

    void Close(const TActorContext& ctx) {
        if (Pipe) {
            NTabletPipe::CloseClient(ctx, Pipe);
            Pipe = TActorId();
        }
    }

    const TString& OwnerCookie() const {
        return OwnerCookie_;
    }

    bool IsPipe(const TActorId& actorId) const {
        return actorId == Pipe;
    }

private:
    THolder<TEvPersQueue::TEvRequest> MakeRequest(ui32 partitionId, TActorId pipe) {
        auto ev = MakeHolder<TEvPersQueue::TEvRequest>();

        ev->Record.MutablePartitionRequest()->SetPartition(partitionId);
        ActorIdToProto(pipe, ev->Record.MutablePartitionRequest()->MutablePipeClient());

        return ev;
    }

private:
    TActorId Pipe;
    TString OwnerCookie_;
    NWilson::TTraceId TraceId;
};

} // namespace NKikimr::NPQ::NPartitionChooser
