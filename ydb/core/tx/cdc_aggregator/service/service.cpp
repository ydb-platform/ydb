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

    struct TOrigin {
        TActorId Sender;
        ui64 Cookie = 0;
    };

    struct TCdcAggregatorConnection {
        TCdcAggregatorHeartbeatDelivery CdcAggregatorHeartbeatDelivery{MaxBatchSize};
        THashMap<ui64, TOrigin> Origins;
        TActorId PipeClient;
    };

    using EStatus = TCdcAggregatorHeartbeatDeliveryReply::EStatus;

    void Handle(TEvCdcAggregatorService::TEvWriteHeartbeat::TPtr& ev) {
        const auto* msg = ev->Get();
        YDB_LOG_TRACE("Handle",
            {"ev", msg->ToString()});

        const TOrigin origin{
            .Sender = ev->Sender,
            .Cookie = ev->Cookie,
        };

        if (!msg->RecipientTabletId) {
            Reply(origin, TCdcAggregatorHeartbeatDeliveryReply{
                .SourceDataShardId = msg->SourceDataShardId,
                .Status = EStatus::Rejected,
                .Error = "Recipient tablet id is not set",
            });
            return;
        }

        const ui64 tabletId = msg->RecipientTabletId;
        auto& connection = CdcAggregatorConnections[tabletId];
        connection.Origins[msg->SourceDataShardId] = origin;
        connection.CdcAggregatorHeartbeatDelivery.AddHeartbeat(msg->SourceDataShardId, msg->Heartbeat);
    }

    void SendHeartbeatBatchesPeriodically() {
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

            auto ev = TCdcAggregatorHeartbeatDeliveryProtoConverter().SerializeBatch(delivery.TakeBatch());
            YDB_LOG_DEBUG("Send batch",
                {"aggregator", tabletId},
                {"heartbeats", ev->Record.HeartbeatsSize()});
            NTabletPipe::SendData(SelfId(), connection.PipeClient, ev.Release());
        }

        Schedule(TimerPeriod, new TEvPrivate::TEvTimerFired());
    }

    void Handle(TEvCdcAggregator::TEvWriteHeartbeatsResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        YDB_LOG_DEBUG("Handle",
            {"ev", ev->Get()->ToString()});

        auto it = CdcAggregatorConnections.find(record.GetOrigin());
        if (it == CdcAggregatorConnections.end()) {
            return;
        }

        auto& delivery = it->second.CdcAggregatorHeartbeatDelivery;
        auto [result, error] = TCdcAggregatorHeartbeatDeliveryProtoConverter().ParseBatchResult(record);
        Reply(it->second, result ? delivery.CompleteBatch(*result) : delivery.FailBatch(error));
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        const auto* msg = ev->Get();
        if (msg->Status == NKikimrProto::OK) {
            return;
        }

        DropCdcAggregatorConnection(msg->TabletId, msg->ClientId, TStringBuilder() << "Pipe connect failed: " << NKikimrProto::EReplyStatus_Name(msg->Status));
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        DropCdcAggregatorConnection(ev->Get()->TabletId, ev->Get()->ClientId, "Pipe destroyed");
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

    void Reply(TCdcAggregatorConnection& connection, const TVector<TCdcAggregatorHeartbeatDeliveryReply>& replies) {
        for (const auto& reply : replies) {
            auto it = connection.Origins.find(reply.SourceDataShardId);
            if (it == connection.Origins.end()) {
                continue;
            }

            Reply(it->second, reply);
            connection.Origins.erase(it);
        }
    }

    void Reply(const TOrigin& origin, const TCdcAggregatorHeartbeatDeliveryReply& reply) {
        auto ev = TCdcAggregatorHeartbeatDeliveryProtoConverter().SerializeReply(reply);
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
            sFunc(TEvPrivate::TEvTimerFired, SendHeartbeatBatchesPeriodically);
        }
    }

private:
    THashMap<ui64, TCdcAggregatorConnection> CdcAggregatorConnections;
};

IActor* CreateCdcAggregatorService() {
    return new TCdcAggregatorService();
}

} // namespace NKikimr::NCdcAggregator
