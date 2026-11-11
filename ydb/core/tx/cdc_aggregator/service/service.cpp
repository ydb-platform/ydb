#include "heartbeat_queue.h"
#include "service.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/tx/cdc_aggregator/public/events.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::CDC_AGGREGATOR, "[CdcAggregatorService] " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::CDC_AGGREGATOR, "[CdcAggregatorService] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::CDC_AGGREGATOR, "[CdcAggregatorService] " << stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::CDC_AGGREGATOR, "[CdcAggregatorService] " << stream)

namespace NKikimr::NCdcAggregator {

class TCdcAggregatorService: public TActor<TCdcAggregatorService> {
    static constexpr ui32 MaxBatchSize = 1000;
    static constexpr ui32 MaxWaiters = 10000;
    static constexpr ui32 PipeRetryLimit = 3;

    struct TAggregator {
        THeartbeatQueue Queue{MaxBatchSize, MaxWaiters};
        TActorId PipeClient;
        bool FlushScheduled = false;
    };

    using TAggregators = THashMap<ui64, TAggregator>;
    using EStatus = TEvCdcAggregatorService::TEvWriteHeartbeatResult::EStatus;

    void Reply(const THeartbeatQueue::TReply& reply) {
        auto ev = MakeHolder<TEvCdcAggregatorService::TEvWriteHeartbeatResult>();
        ev->Status = reply.Status;
        ev->WrittenVersion = reply.WrittenVersion;
        ev->Error = reply.Error;

        LOG_T("Reply " << ev->ToString() << ": recipient# " << reply.Sender);
        Send(reply.Sender, ev.Release(), 0, reply.Cookie);
    }

    void Reply(const TVector<THeartbeatQueue::TReply>& replies) {
        for (const auto& reply : replies) {
            Reply(reply);
        }
    }

    void SendBatch(ui64 tabletId, TAggregator& aggregator) {
        if (!aggregator.Queue.CanSend()) {
            return;
        }

        if (!aggregator.PipeClient) {
            NTabletPipe::TClientConfig config;
            config.RetryPolicy = {.RetryLimitCount = PipeRetryLimit};
            aggregator.PipeClient = Register(NTabletPipe::CreateClient(SelfId(), tabletId, config));
        }

        auto ev = aggregator.Queue.NextBatch();
        LOG_D("Send batch: aggregator# " << tabletId << ", heartbeats# " << ev->Record.HeartbeatsSize());
        NTabletPipe::SendData(SelfId(), aggregator.PipeClient, ev.Release());
    }

    void Remove(TAggregators::iterator it) {
        if (it->second.PipeClient) {
            NTabletPipe::CloseClient(SelfId(), it->second.PipeClient);
        }
        Aggregators.erase(it);
    }

    void Fail(ui64 tabletId, const TActorId& pipeClient, const TString& error) {
        auto it = Aggregators.find(tabletId);
        if (it == Aggregators.end() || it->second.PipeClient != pipeClient) {
            return;
        }

        LOG_N(error << ": aggregator# " << tabletId << ", waiters# " << it->second.Queue.GetWaitersCount());
        Reply(it->second.Queue.Fail(error));
        Remove(it);
    }

    void Handle(TEvCdcAggregatorService::TEvWriteHeartbeat::TPtr& ev) {
        const auto* msg = ev->Get();
        LOG_T("Handle " << msg->ToString());

        const ui64 tabletId = msg->RecipientTabletId;
        auto& aggregator = Aggregators[tabletId];
        if (!aggregator.Queue.Enqueue(msg->SourceDataShardId, msg->Heartbeat, ev->Sender, ev->Cookie)) {
            LOG_W("Queue overflow: aggregator# " << tabletId << ", source# " << msg->SourceDataShardId);
            Reply(THeartbeatQueue::TReply{
                .Sender = ev->Sender,
                .Cookie = ev->Cookie,
                .Status = EStatus::Retry,
                .Error = "Queue overflow",
            });
            return;
        }

        if (!aggregator.FlushScheduled && aggregator.Queue.CanSend()) {
            aggregator.FlushScheduled = true;
            Send(SelfId(), new TEvents::TEvWakeup(tabletId));
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        auto it = Aggregators.find(ev->Get()->Tag);
        if (it == Aggregators.end()) {
            return;
        }

        it->second.FlushScheduled = false;
        SendBatch(it->first, it->second);
    }

    void Handle(TEvCdcAggregator::TEvWriteHeartbeatsResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        LOG_D("Handle " << ev->Get()->ToString());

        auto it = Aggregators.find(record.GetOrigin());
        if (it == Aggregators.end()) {
            return;
        }

        Reply(it->second.Queue.OnResult(record));
        if (it->second.Queue.IsIdle()) {
            Remove(it);
        } else {
            SendBatch(it->first, it->second);
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        const auto* msg = ev->Get();
        if (msg->Status == NKikimrProto::OK) {
            return;
        }

        Fail(msg->TabletId, msg->ClientId, TStringBuilder() << "Pipe connect failed: " << NKikimrProto::EReplyStatus_Name(msg->Status));
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        Fail(ev->Get()->TabletId, ev->Get()->ClientId, "Pipe destroyed");
    }

public:
    TCdcAggregatorService()
        : TActor(&TThis::StateWork)
    {
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvCdcAggregatorService::TEvWriteHeartbeat, Handle);
            hFunc(TEvCdcAggregator::TEvWriteHeartbeatsResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
        }
    }

private:
    TAggregators Aggregators;
};

IActor* CreateCdcAggregatorService() {
    return new TCdcAggregatorService();
}

} // namespace NKikimr::NCdcAggregator
