#include "cdc_aggregator_heartbeat_publisher.h"
#include "cdc_aggregator_heartbeat_publication.h"

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

TVector<ui32> MakePartitionIds(const TVector<TCdcAggregatorHeartbeatPublisherPartition>& partitions) {
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

bool IsPermanentEmitError(NPersQueue::NErrorCode::EErrorCode errorCode) {
    switch (errorCode) {
    case NPersQueue::NErrorCode::BAD_REQUEST:
    case NPersQueue::NErrorCode::WRITE_ERROR_PARTITION_INACTIVE:
    case NPersQueue::NErrorCode::WRONG_PARTITION_NUMBER:
    case NPersQueue::NErrorCode::TABLET_IS_DROPPED:
        return true;
    default:
        return false;
    }
}

} // namespace

class TCdcAggregatorHeartbeatPublisher : public TActorBootstrapped<TCdcAggregatorHeartbeatPublisher> {
    static constexpr TDuration MinRetryDelay = TDuration::Seconds(1);
    static constexpr TDuration MaxRetryDelay = TDuration::Seconds(30);

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CDC_AGGREGATOR_TABLET;
    }

    TCdcAggregatorHeartbeatPublisher(TString streamPath, const TVector<TCdcAggregatorHeartbeatPublisherPartition>& partitions)
        : StreamPath(std::move(streamPath))
        , Publication(MakePartitionIds(partitions))
    {
        for (const auto& partition : partitions) {
            TabletIdByPartitionId.emplace(partition.PartitionId, partition.TabletId);
            PartitionIdsByTabletId[partition.TabletId].push_back(partition.PartitionId);
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
                {"streamPath", StreamPath},
                {"eventType", ev->GetTypeRewrite()});
        }
    }

    void Handle(TEvCdcAggregatorHeartbeatPublisher::TEvPublishHeartbeat::TPtr& ev) {
        if (!Publication.GetFailedEmitCount()) {
            RetryDelay = MinRetryDelay;
        }
        Publication.SetHeartbeat(ev->Get()->Heartbeat);
        EmitHeartbeat(Publication.TakePartitionsToEmit());
    }

    void Handle(TEvPersQueue::TEvResponse::TPtr& ev) {
        const auto& response = ev->Get()->Record;
        const ui32 partitionId = response.GetPartitionResponse().GetCookie();
        if (const auto emittedVersion = ParseEmittedVersion(response)) {
            if (Publication.CompleteEmit(partitionId, *emittedVersion)) {
                EmitHeartbeat({partitionId});
            }
        } else if (IsPermanentEmitError(response.GetErrorCode())) {
            YDB_LOG_WARN("Heartbeat emit stopped",
                {"streamPath", StreamPath},
                {"partitionId", partitionId},
                {"response", response.ShortDebugString()});
            Publication.StopEmit(partitionId);
        } else {
            YDB_LOG_DEBUG("Heartbeat emit failed",
                {"streamPath", StreamPath},
                {"partitionId", partitionId},
                {"response", response.ShortDebugString()});
            Publication.FailEmit(partitionId);
        }
        ScheduleRetryIfNeeded();
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            FailTabletEmits(ev->Get()->TabletId, ev->Get()->ClientId);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        FailTabletEmits(ev->Get()->TabletId, ev->Get()->ClientId);
    }

    void FailTabletEmits(ui64 tabletId, const TActorId& clientId) {
        const auto pipe = PipeByTabletId.find(tabletId);
        if (pipe == PipeByTabletId.end() || pipe->second != clientId) {
            return;
        }
        PipeByTabletId.erase(pipe);

        YDB_LOG_DEBUG("Heartbeat emit pipe lost",
            {"streamPath", StreamPath},
            {"tabletId", tabletId});
        for (const auto partitionId : PartitionIdsByTabletId.at(tabletId)) {
            Publication.FailEmit(partitionId);
        }
        ScheduleRetryIfNeeded();
    }

    void RetryFailedEmits() {
        RetryScheduled = false;
        Publication.RetryFailedEmits();
        EmitHeartbeat(Publication.TakePartitionsToEmit());
    }

    void ScheduleRetryIfNeeded() {
        if (RetryScheduled || !Publication.GetFailedEmitCount()) {
            return;
        }
        YDB_LOG_WARN("Heartbeat emit retry scheduled",
            {"streamPath", StreamPath},
            {"failedPartitions", Publication.GetFailedEmitCount()},
            {"delay", RetryDelay});
        RetryScheduled = true;
        Schedule(RetryDelay, new TEvents::TEvWakeup());
        RetryDelay = Min(RetryDelay * 2, MaxRetryDelay);
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
    THashMap<ui64, TVector<ui32>> PartitionIdsByTabletId;
    TCdcAggregatorHeartbeatPublication Publication;
    THashMap<ui64, TActorId> PipeByTabletId;
    TDuration RetryDelay = MinRetryDelay;
    bool RetryScheduled = false;
};

IActor* CreateCdcAggregatorHeartbeatPublisher(TString streamPath, const TVector<TCdcAggregatorHeartbeatPublisherPartition>& partitions) {
    return new TCdcAggregatorHeartbeatPublisher(std::move(streamPath), partitions);
}

} // namespace NKikimr::NCdcAggregator
