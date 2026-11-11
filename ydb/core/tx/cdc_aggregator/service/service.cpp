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

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

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

    using EStatus = TEvCdcAggregatorService::TEvWriteHeartbeatResult::EStatus;

    void Reply(const THeartbeatQueue::TReply& reply) {
        auto ev = MakeHolder<TEvCdcAggregatorService::TEvWriteHeartbeatResult>();
        ev->Status = reply.Status;
        ev->WrittenVersion = reply.WrittenVersion;
        ev->Error = reply.Error;

        YDB_LOG_TRACE("Reply",
            {"ev", ev->ToString()},
            {"recipient", reply.Sender});
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
        YDB_LOG_DEBUG("Send batch",
            {"aggregator", tabletId},
            {"heartbeats", ev->Record.HeartbeatsSize()});
        NTabletPipe::SendData(SelfId(), aggregator.PipeClient, ev.Release());
    }

    void ResetAggregator(ui64 tabletId, const TActorId& pipeClient, const TString& error) {
        auto it = Aggregators.find(tabletId);
        if (it == Aggregators.end() || it->second.PipeClient != pipeClient) {
            return;
        }

        YDB_LOG_NOTICE("Reset aggregator",
            {"aggregator", tabletId},
            {"waiters", it->second.Queue.GetWaitersCount()},
            {"error", error});
        Reply(it->second.Queue.Reset(error));
        Aggregators.erase(it);
    }

    void Handle(TEvCdcAggregatorService::TEvWriteHeartbeat::TPtr& ev) {
        const auto* msg = ev->Get();
        YDB_LOG_TRACE("Handle",
            {"ev", msg->ToString()});

        const ui64 tabletId = msg->RecipientTabletId;
        auto& aggregator = Aggregators[tabletId];
        if (!aggregator.Queue.Enqueue(msg->SourceDataShardId, msg->Heartbeat, ev->Sender, ev->Cookie)) {
            YDB_LOG_WARN("Queue overflow",
                {"aggregator", tabletId},
                {"sourceDataShardId", msg->SourceDataShardId});
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
        YDB_LOG_DEBUG("Handle",
            {"ev", ev->Get()->ToString()});

        auto it = Aggregators.find(record.GetOrigin());
        if (it == Aggregators.end()) {
            return;
        }

        Reply(it->second.Queue.OnResult(record));
        SendBatch(it->first, it->second);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        const auto* msg = ev->Get();
        if (msg->Status == NKikimrProto::OK) {
            return;
        }

        ResetAggregator(msg->TabletId, msg->ClientId, TStringBuilder() << "Pipe connect failed: " << NKikimrProto::EReplyStatus_Name(msg->Status));
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        ResetAggregator(ev->Get()->TabletId, ev->Get()->ClientId, "Pipe destroyed");
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CDC_AGGREGATOR_SERVICE;
    }

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
    THashMap<ui64, TAggregator> Aggregators;
};

IActor* CreateCdcAggregatorService() {
    return new TCdcAggregatorService();
}

} // namespace NKikimr::NCdcAggregator
