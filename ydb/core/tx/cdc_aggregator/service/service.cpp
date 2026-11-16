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

#include <functional>

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

    using TSendReply = std::function<void(const TOrigin&, const TCdcAggregatorHeartbeatDeliveryReply&)>;

    class TCdcAggregatorConnection {
    public:
        explicit TCdcAggregatorConnection(const TActorId& pipeClient)
            : PipeClient(pipeClient)
        {
        }

        void RegisterHeartbeat(const TOrigin& origin, ui64 sourceDataShardId, const NPQ::THeartbeat& heartbeat) {
            LastActivity = TActivationContext::Monotonic();
            Origins[sourceDataShardId] = origin;
            CdcAggregatorHeartbeatDelivery.AddHeartbeat(sourceDataShardId, heartbeat);
        }

        std::optional<TCdcAggregatorHeartbeatBatch> PrepareNextBatchIfPossible(ui64 batchCookie) {
            auto batch = CdcAggregatorHeartbeatDelivery.PrepareNextBatchIfPossible();
            if (batch) {
                BatchCookie = batchCookie;
                BatchDeadline = TActivationContext::Monotonic() + BatchTimeout;
            }
            return batch;
        }

        bool IsAwaitingBatch(ui64 batchCookie) const {
            return BatchCookie && BatchCookie == batchCookie;
        }

        void CompleteBatch(const TCdcAggregatorWriteHeartbeatsResult& writeHeartbeatsResult, const TSendReply& sendReply) {
            BatchCookie = 0;
            LastActivity = TActivationContext::Monotonic();
            ReplyToOrigins(CdcAggregatorHeartbeatDelivery.CompleteBatch(writeHeartbeatsResult), sendReply);
        }

        void FailBatch(const TString& error, const TSendReply& sendReply) {
            BatchCookie = 0;
            LastActivity = TActivationContext::Monotonic();
            ReplyToOrigins(CdcAggregatorHeartbeatDelivery.FailBatch(error), sendReply);
        }

        void ReplyToAllOrigins(const TCdcAggregatorHeartbeatDeliveryReply& reply, const TSendReply& sendReply) {
            for (const auto& [sourceDataShardId, origin] : Origins) {
                sendReply(origin, reply);
            }
            Origins.clear();
        }

        bool IsBatchExpired(TMonotonic now) const {
            return BatchCookie && now >= BatchDeadline;
        }

        bool IsIdleExpired(TMonotonic now) const {
            return CdcAggregatorHeartbeatDelivery.IsIdle() && now - LastActivity >= IdleTimeout;
        }

        const TActorId& GetPipeClient() const {
            return PipeClient;
        }

        size_t GetOriginCount() const {
            return Origins.size();
        }

    private:
        void ReplyToOrigins(const TCdcAggregatorHeartbeatBatchResult& batchResult, const TSendReply& sendReply) {
            for (const auto& [sourceDataShardId, reply] : batchResult) {
                auto it = Origins.find(sourceDataShardId);
                if (it == Origins.end()) {
                    continue;
                }

                sendReply(it->second, reply);
                Origins.erase(it);
            }
        }

        const TActorId PipeClient;
        TCdcAggregatorHeartbeatDelivery CdcAggregatorHeartbeatDelivery{MaxBatchSize};
        THashMap<ui64, TOrigin> Origins;
        ui64 BatchCookie = 0;
        TMonotonic BatchDeadline;
        TMonotonic LastActivity;
    };

    void AcceptHeartbeat(const TOrigin& origin, const TEvCdcAggregatorService::TEvWriteHeartbeat& msg) {
        YDB_LOG_TRACE("Accept heartbeat",
            {"ev", msg.ToString()});

        if (!msg.RecipientTabletId) {
            Reply(origin, TCdcAggregatorHeartbeatRejected{.Error = "Recipient tablet id is not set"});
            return;
        }

        auto it = CdcAggregatorConnections.find(msg.RecipientTabletId);
        if (it == CdcAggregatorConnections.end()) {
            it = CdcAggregatorConnections.emplace(msg.RecipientTabletId, TCdcAggregatorConnection(CreatePipeClient(msg.RecipientTabletId))).first;
        }
        it->second.RegisterHeartbeat(origin, msg.SourceDataShardId, msg.Heartbeat);
    }

    TActorId CreatePipeClient(ui64 tabletId) {
        NTabletPipe::TClientConfig config;
        config.RetryPolicy = {.RetryLimitCount = PipeRetryLimit};
        return Register(NTabletPipe::CreateClient(SelfId(), tabletId, config));
    }

    void SendHeartbeatBatches() {
        for (auto& [tabletId, connection] : CdcAggregatorConnections) {
            const ui64 batchCookie = ++LastBatchCookie;
            auto batch = connection.PrepareNextBatchIfPossible(batchCookie);
            if (!batch) {
                continue;
            }

            auto ev = ProtoConverter.SerializeBatch(*batch);
            YDB_LOG_DEBUG("Send batch",
                {"aggregator", tabletId},
                {"batchCookie", batchCookie},
                {"heartbeats", ev->Record.HeartbeatsSize()});
            NTabletPipe::SendData(SelfId(), connection.GetPipeClient(), ev.Release(), batchCookie);
        }
    }

    void RemoveExpiredCdcAggregatorConnections() {
        const auto now = TActivationContext::Monotonic();
        for (auto it = CdcAggregatorConnections.begin(); it != CdcAggregatorConnections.end();) {
            const auto current = it++;
            auto& connection = current->second;
            if (connection.IsBatchExpired(now)) {
                NTabletPipe::CloseClient(SelfId(), connection.GetPipeClient());
                DropCdcAggregatorConnection(current->first, connection.GetPipeClient(), "Heartbeat batch timed out");
            } else if (connection.IsIdleExpired(now)) {
                NTabletPipe::CloseClient(SelfId(), connection.GetPipeClient());
                CdcAggregatorConnections.erase(current);
            }
        }
    }

    void ApplyHeartbeatBatchResult(const NKikimrCdcAggregator::TEvWriteHeartbeatsResult& record, ui64 cookie) {
        YDB_LOG_DEBUG("Apply batch result",
            {"record", record.ShortDebugString()});

        auto it = CdcAggregatorConnections.find(record.GetOrigin());
        if (it == CdcAggregatorConnections.end() || !it->second.IsAwaitingBatch(cookie)) {
            return;
        }

        const auto sendReply = [this](const TOrigin& origin, const TCdcAggregatorHeartbeatDeliveryReply& reply) {
            Reply(origin, reply);
        };
        auto [writeHeartbeatsResult, error] = ProtoConverter.ParseWriteHeartbeatsResult(record);
        if (writeHeartbeatsResult) {
            it->second.CompleteBatch(*writeHeartbeatsResult, sendReply);
        } else {
            it->second.FailBatch(error, sendReply);
        }
    }

    void DropCdcAggregatorConnection(ui64 tabletId, const TActorId& pipeClient, const TString& error) {
        auto it = CdcAggregatorConnections.find(tabletId);
        if (it == CdcAggregatorConnections.end() || it->second.GetPipeClient() != pipeClient) {
            return;
        }

        YDB_LOG_NOTICE("Drop connection",
            {"aggregator", tabletId},
            {"origins", it->second.GetOriginCount()},
            {"error", error});
        it->second.ReplyToAllOrigins(TCdcAggregatorHeartbeatRetry{.Error = error}, [this](const TOrigin& origin, const TCdcAggregatorHeartbeatDeliveryReply& reply) {
            Reply(origin, reply);
        });
        CdcAggregatorConnections.erase(it);
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
        auto ev = ProtoConverter.SerializeReply(reply);
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
    TCdcAggregatorHeartbeatDeliveryProtoConverter ProtoConverter;
    THashMap<ui64, TCdcAggregatorConnection> CdcAggregatorConnections;
    ui64 LastBatchCookie = 0;
};

IActor* CreateCdcAggregatorService() {
    return new TCdcAggregatorService();
}

} // namespace NKikimr::NCdcAggregator
