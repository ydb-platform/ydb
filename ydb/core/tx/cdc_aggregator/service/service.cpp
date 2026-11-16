#include "cdc_aggregator_heartbeat_delivery.h"
#include "cdc_aggregator_heartbeat_delivery_proto.h"
#include "service.h"

#include <ydb/core/base/events.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/tx/cdc_aggregator/public/events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

namespace {

struct TEvPrivate {
    enum EEv {
        EvTimerFired = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

    struct TEvTimerFired: public TEventLocal<TEvTimerFired, EvTimerFired> {};
};

} // namespace

class TCdcAggregatorService: public TActorBootstrapped<TCdcAggregatorService> {
    static constexpr ui32 MaxBatchSize = 1000;
    static constexpr TDuration TimerPeriod = TDuration::MilliSeconds(100);
    static constexpr ui32 PipeRetryLimit = 3;
    static constexpr TDuration BatchTimeout = TDuration::Seconds(30);
    static constexpr TDuration IdleTimeout = TDuration::Minutes(1);

    struct TOrigin {
        TActorId Sender;
        ui64 Cookie = 0;
    };

    struct TCdcAggregatorConnection {
        TCdcAggregatorHeartbeatDelivery CdcAggregatorHeartbeatDelivery{MaxBatchSize};
        THashMap<ui64, TOrigin> Origins;
        TActorId PipeClient;
        ui64 BatchCookie = 0;
        TMonotonic BatchDeadline;
        TMonotonic LastActivity;
    };

    using EStatus = TCdcAggregatorHeartbeatDeliveryReply::EStatus;

    void AcceptHeartbeat(const TOrigin& origin, const TEvCdcAggregatorService::TEvWriteHeartbeat& msg) {
        YDB_LOG_TRACE("Accept heartbeat",
            {"ev", msg.ToString()});

        if (!msg.RecipientTabletId) {
            Reply(origin, TCdcAggregatorHeartbeatDeliveryReply{
                .SourceDataShardId = msg.SourceDataShardId,
                .Status = EStatus::Rejected,
                .Error = "Recipient tablet id is not set",
            });
            return;
        }

        auto& connection = CdcAggregatorConnections[msg.RecipientTabletId];
        connection.LastActivity = TActivationContext::Monotonic();
        connection.Origins[msg.SourceDataShardId] = origin;
        connection.CdcAggregatorHeartbeatDelivery.AddHeartbeat(msg.SourceDataShardId, msg.Heartbeat);
    }

    void SendHeartbeatBatches() {
        for (auto& [tabletId, connection] : CdcAggregatorConnections) {
            auto& delivery = connection.CdcAggregatorHeartbeatDelivery;
            if (!delivery.HasBatchToSend()) {
                continue;
            }

            if (!connection.PipeClient) {
                NTabletPipe::TClientConfig config;
                config.RetryPolicy = {.RetryLimitCount = PipeRetryLimit};
                connection.PipeClient = Register(NTabletPipe::CreateClient(SelfId(), tabletId, config));
            }

            auto ev = CdcAggregatorHeartbeatDeliveryProtoConverter.SerializeBatch(delivery.TakeBatch());
            connection.BatchCookie = ++LastBatchCookie;
            connection.BatchDeadline = TActivationContext::Monotonic() + BatchTimeout;
            YDB_LOG_DEBUG("Send batch",
                {"aggregator", tabletId},
                {"batchCookie", connection.BatchCookie},
                {"heartbeats", ev->Record.HeartbeatsSize()});
            NTabletPipe::SendData(SelfId(), connection.PipeClient, ev.Release(), connection.BatchCookie);
        }
    }

    void RemoveExpiredCdcAggregatorConnections() {
        const auto now = TActivationContext::Monotonic();
        for (auto it = CdcAggregatorConnections.begin(); it != CdcAggregatorConnections.end();) {
            const auto current = it++;
            auto& connection = current->second;
            if (connection.BatchCookie && now >= connection.BatchDeadline) {
                NTabletPipe::CloseClient(SelfId(), connection.PipeClient);
                DropCdcAggregatorConnection(current->first, connection.PipeClient, "Heartbeat batch timed out");
            } else if (connection.CdcAggregatorHeartbeatDelivery.IsIdle() && now - connection.LastActivity >= IdleTimeout) {
                NTabletPipe::CloseClient(SelfId(), connection.PipeClient);
                CdcAggregatorConnections.erase(current);
            }
        }
    }

    void ApplyHeartbeatBatchResult(const NKikimrCdcAggregator::TEvWriteHeartbeatsResult& record, ui64 cookie) {
        YDB_LOG_DEBUG("Apply batch result",
            {"record", record.ShortDebugString()});

        auto it = CdcAggregatorConnections.find(record.GetOrigin());
        if (it == CdcAggregatorConnections.end() || !it->second.BatchCookie || it->second.BatchCookie != cookie) {
            return;
        }

        it->second.BatchCookie = 0;
        it->second.LastActivity = TActivationContext::Monotonic();
        auto& delivery = it->second.CdcAggregatorHeartbeatDelivery;
        auto [result, error] = CdcAggregatorHeartbeatDeliveryProtoConverter.ParseBatchResult(record);
        ReplyToOrigins(it->second, result ? delivery.CompleteBatch(*result) : delivery.FailBatch(error));
    }

    void DropCdcAggregatorConnection(ui64 tabletId, const TActorId& pipeClient, const TString& error) {
        auto it = CdcAggregatorConnections.find(tabletId);
        if (it == CdcAggregatorConnections.end() || it->second.PipeClient != pipeClient) {
            return;
        }

        YDB_LOG_NOTICE("Drop connection",
            {"aggregator", tabletId},
            {"origins", it->second.Origins.size()},
            {"error", error});
        for (const auto& [sourceDataShardId, origin] : it->second.Origins) {
            Reply(origin, TCdcAggregatorHeartbeatDeliveryReply{
                .SourceDataShardId = sourceDataShardId,
                .Status = EStatus::Retry,
                .Error = error,
            });
        }
        CdcAggregatorConnections.erase(it);
    }

    void ReplyToOrigins(TCdcAggregatorConnection& connection, const TVector<TCdcAggregatorHeartbeatDeliveryReply>& replies) {
        for (const auto& reply : replies) {
            auto it = connection.Origins.find(reply.SourceDataShardId);
            if (it == connection.Origins.end()) {
                continue;
            }

            Reply(it->second, reply);
            connection.Origins.erase(it);
        }
    }

    void Handle(TEvCdcAggregatorService::TEvWriteHeartbeat::TPtr& ev) {
        AcceptHeartbeat(TOrigin{.Sender = ev->Sender, .Cookie = ev->Cookie}, *ev->Get());
    }

    void Handle(TEvCdcAggregator::TEvWriteHeartbeatsResult::TPtr& ev) {
        ApplyHeartbeatBatchResult(ev->Get()->Record, ev->Cookie);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        const auto* msg = ev->Get();
        if (msg->Status != NKikimrProto::OK) {
            DropCdcAggregatorConnection(msg->TabletId, msg->ClientId, TStringBuilder() << "Pipe connect failed: " << NKikimrProto::EReplyStatus_Name(msg->Status));
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        DropCdcAggregatorConnection(ev->Get()->TabletId, ev->Get()->ClientId, "Pipe destroyed");
    }

    void HandleTimerFired() {
        RemoveExpiredCdcAggregatorConnections();
        SendHeartbeatBatches();
        Schedule(TimerPeriod, new TEvPrivate::TEvTimerFired());
    }

    void Reply(const TOrigin& origin, const TCdcAggregatorHeartbeatDeliveryReply& reply) {
        auto ev = CdcAggregatorHeartbeatDeliveryProtoConverter.SerializeReply(reply);
        YDB_LOG_TRACE("Reply",
            {"ev", ev->ToString()},
            {"recipient", origin.Sender});
        Send(origin.Sender, ev.Release(), 0, origin.Cookie);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CDC_AGGREGATOR_SERVICE;
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        Schedule(TimerPeriod, new TEvPrivate::TEvTimerFired());
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvCdcAggregatorService::TEvWriteHeartbeat, Handle);
            hFunc(TEvCdcAggregator::TEvWriteHeartbeatsResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            sFunc(TEvPrivate::TEvTimerFired, HandleTimerFired);
        }
    }

private:
    TCdcAggregatorHeartbeatDeliveryProtoConverter CdcAggregatorHeartbeatDeliveryProtoConverter;
    THashMap<ui64, TCdcAggregatorConnection> CdcAggregatorConnections;
    ui64 LastBatchCookie = 0;
};

IActor* CreateCdcAggregatorService() {
    return new TCdcAggregatorService();
}

} // namespace NKikimr::NCdcAggregator
