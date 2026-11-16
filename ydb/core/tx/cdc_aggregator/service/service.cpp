#include "cdc_aggregator_heartbeat_delivery.h"
#include "cdc_aggregator_heartbeat_delivery_proto.h"
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
    static constexpr ui32 PipeRetryLimit = 3;

    struct TOrigin {
        TActorId Sender;
        ui64 Cookie = 0;
    };

    struct TCdcAggregatorConnection {
        TCdcAggregatorHeartbeatDelivery CdcAggregatorHeartbeatDelivery{MaxBatchSize};
        THashMap<ui64, TOrigin> Origins;
        TActorId PipeClient;
        bool FlushScheduled = false;
    };

    using EStatus = TCdcAggregatorHeartbeatDeliveryReply::EStatus;

    void Reply(const TOrigin& origin, const TCdcAggregatorHeartbeatDeliveryReply& reply) {
        auto ev = TCdcAggregatorHeartbeatDeliveryProtoConverter().SerializeReply(reply);
        YDB_LOG_TRACE("Reply",
            {"ev", ev->ToString()},
            {"recipient", origin.Sender});
        Send(origin.Sender, ev.Release(), 0, origin.Cookie);
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

    void SendHeartbeatBatch(ui64 tabletId, TCdcAggregatorConnection& connection) {
        auto& delivery = connection.CdcAggregatorHeartbeatDelivery;
        if (!delivery.HasBatchToSend()) {
            return;
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
        auto& delivery = connection.CdcAggregatorHeartbeatDelivery;
        connection.Origins[msg->SourceDataShardId] = origin;
        delivery.AddHeartbeat(msg->SourceDataShardId, msg->Heartbeat);

        if (!connection.FlushScheduled && delivery.HasBatchToSend()) {
            connection.FlushScheduled = true;
            Send(SelfId(), new TEvents::TEvWakeup(tabletId));
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        auto it = CdcAggregatorConnections.find(ev->Get()->Tag);
        if (it == CdcAggregatorConnections.end()) {
            return;
        }

        it->second.FlushScheduled = false;
        SendHeartbeatBatch(it->first, it->second);
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
        SendHeartbeatBatch(it->first, it->second);
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
    THashMap<ui64, TCdcAggregatorConnection> CdcAggregatorConnections;
};

IActor* CreateCdcAggregatorService() {
    return new TCdcAggregatorService();
}

} // namespace NKikimr::NCdcAggregator
