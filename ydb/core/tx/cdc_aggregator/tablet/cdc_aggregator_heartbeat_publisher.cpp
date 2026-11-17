#include "cdc_aggregator_heartbeat_publication.h"
#include "cdc_aggregator_heartbeat_publisher.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/public/lib/base/msgbus_status.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

namespace {

TVector<ui32> GetPartitionIds(const TVector<TCdcAggregatorHeartbeatPublisherPartition>& partitions) {
    TVector<ui32> partitionIds;
    for (const auto& partition : partitions) {
        partitionIds.push_back(partition.PartitionId);
    }
    return partitionIds;
}

THolder<TEvPersQueue::TEvRequest> MakeEmitHeartbeatRequest(const TString& streamPath, ui32 partitionId, const NPQ::THeartbeat& heartbeat) {
    auto request = MakeHolder<TEvPersQueue::TEvRequest>();
    auto& partitionRequest = *request->Record.MutablePartitionRequest();
    partitionRequest.SetTopic(streamPath);
    partitionRequest.SetPartition(partitionId);
    partitionRequest.SetCookie(partitionId);
    heartbeat.Serialize(*partitionRequest.MutableCmdEmitHeartbeat()->MutableHeartbeat());
    return request;
}

std::optional<TRowVersion> ParseEmittedVersion(const NKikimrClient::TResponse& response) {
    if (response.GetStatus() != NMsgBusProxy::MSTATUS_OK
        || response.GetErrorCode() != NPersQueue::NErrorCode::OK
        || !response.GetPartitionResponse().HasCmdEmitHeartbeatResult())
    {
        return std::nullopt;
    }

    const auto& result = response.GetPartitionResponse().GetCmdEmitHeartbeatResult();
    return TRowVersion(result.GetStep(), result.GetTxId());
}

} // namespace

class TCdcAggregatorHeartbeatPublisher : public TActorBootstrapped<TCdcAggregatorHeartbeatPublisher> {
    static constexpr TDuration RetryDelay = TDuration::Seconds(1);

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CDC_AGGREGATOR_TABLET;
    }

    TCdcAggregatorHeartbeatPublisher(TString streamPath, const TVector<TCdcAggregatorHeartbeatPublisherPartition>& partitions)
        : StreamPath(std::move(streamPath))
        , Publication(GetPartitionIds(partitions))
    {
        for (const auto& partition : partitions) {
            TabletIdByPartitionId.emplace(partition.PartitionId, partition.TabletId);
        }
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvCdcAggregatorHeartbeatPublisher::TEvPublishHeartbeat, Handle);
            hFunc(TEvPersQueue::TEvResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            sFunc(TEvents::TEvWakeup, RetryFailedEmits);
            sFunc(TEvents::TEvPoison, PassAway);
        default:
            YDB_LOG_WARN("Unexpected event",
                {"eventType", ev->GetTypeRewrite()});
        }
    }

    void Handle(TEvCdcAggregatorHeartbeatPublisher::TEvPublishHeartbeat::TPtr& ev) {
        Publication.SetHeartbeat(ev->Get()->Heartbeat);
        EmitHeartbeat(Publication.TakePartitionsToEmit());
    }

    void Handle(TEvPersQueue::TEvResponse::TPtr& ev) {
        const auto& response = ev->Get()->Record;
        if (!response.GetPartitionResponse().HasCookie()) {
            YDB_LOG_WARN("Heartbeat emit response without partition",
                {"response", response.ShortDebugString()});
            return;
        }

        const ui32 partitionId = response.GetPartitionResponse().GetCookie();
        const auto emittedVersion = ParseEmittedVersion(response);
        if (!emittedVersion) {
            YDB_LOG_WARN("Heartbeat emit failed",
                {"partitionId", partitionId},
                {"response", response.ShortDebugString()});
            Publication.FailEmit(partitionId);
        } else if (Publication.CompleteEmit(partitionId, *emittedVersion)) {
            EmitHeartbeat({partitionId});
        }
        ScheduleRetryIfNeeded();
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            FailTabletEmits(ev->Get()->TabletId);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        FailTabletEmits(ev->Get()->TabletId);
    }

    void FailTabletEmits(ui64 tabletId) {
        if (const auto pipe = PipeByTabletId.find(tabletId); pipe != PipeByTabletId.end()) {
            NTabletPipe::CloseClient(SelfId(), pipe->second);
            PipeByTabletId.erase(pipe);
        }
        for (const auto& [partitionId, partitionTabletId] : TabletIdByPartitionId) {
            if (partitionTabletId == tabletId) {
                Publication.FailEmit(partitionId);
            }
        }
        ScheduleRetryIfNeeded();
    }

    void RetryFailedEmits() {
        RetryScheduled = false;
        Publication.RetryFailedEmits();
        EmitHeartbeat(Publication.TakePartitionsToEmit());
    }

    void ScheduleRetryIfNeeded() {
        if (!RetryScheduled && Publication.HasFailedEmits()) {
            RetryScheduled = true;
            Schedule(RetryDelay, new TEvents::TEvWakeup());
        }
    }

    void EmitHeartbeat(const TVector<ui32>& partitionIds) {
        for (const auto partitionId : partitionIds) {
            NTabletPipe::SendData(SelfId(), GetOrCreatePipe(TabletIdByPartitionId.at(partitionId)),
                MakeEmitHeartbeatRequest(StreamPath, partitionId, *Publication.GetHeartbeat()).Release());
        }
    }

    TActorId GetOrCreatePipe(ui64 tabletId) {
        auto& pipe = PipeByTabletId[tabletId];
        if (!pipe) {
            pipe = Register(NTabletPipe::CreateClient(SelfId(), tabletId));
        }
        return pipe;
    }

    void PassAway() override {
        for (const auto& [_, pipe] : PipeByTabletId) {
            NTabletPipe::CloseClient(SelfId(), pipe);
        }
        TActorBootstrapped::PassAway();
    }

private:
    const TString StreamPath;
    THashMap<ui32, ui64> TabletIdByPartitionId;
    TCdcAggregatorHeartbeatPublication Publication;
    THashMap<ui64, TActorId> PipeByTabletId;
    bool RetryScheduled = false;
};

IActor* CreateCdcAggregatorHeartbeatPublisher(TString streamPath, const TVector<TCdcAggregatorHeartbeatPublisherPartition>& partitions) {
    return new TCdcAggregatorHeartbeatPublisher(std::move(streamPath), partitions);
}

} // namespace NKikimr::NCdcAggregator
