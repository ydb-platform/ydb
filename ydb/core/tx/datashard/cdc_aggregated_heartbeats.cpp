#include "cdc_aggregated_heartbeats.h"

#include <ydb/core/persqueue/common/heartbeat.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CHANGE_EXCHANGE

namespace NKikimr::NDataShard {

namespace {

using IChangeRecord = NChangeExchange::IChangeRecord;
using TEvWriteHeartbeat = NCdcAggregator::TEvCdcAggregatorService::TEvWriteHeartbeat;
using TEvWriteHeartbeatResult = NCdcAggregator::TEvCdcAggregatorService::TEvWriteHeartbeatResult;

std::optional<ui64> FindHeartbeatToSend(const TMap<ui64, IChangeRecord::TPtr>& heartbeats, const TSet<ui64>& pendingRecords) {
    const ui64 firstPending = pendingRecords ? *pendingRecords.begin() : Max<ui64>();
    const auto firstBlocked = heartbeats.lower_bound(firstPending);
    if (firstBlocked == heartbeats.begin()) {
        return std::nullopt;
    }
    return std::prev(firstBlocked)->first;
}

TVector<ui64> EraseUpTo(TMap<ui64, IChangeRecord::TPtr>& heartbeats, ui64 upTo) {
    TVector<ui64> orders;
    const auto end = heartbeats.upper_bound(upTo);
    for (auto it = heartbeats.begin(); it != end; it = heartbeats.erase(it)) {
        orders.push_back(it->first);
    }
    return orders;
}

bool IsWritten(const TEvWriteHeartbeatResult& result, const IChangeRecord& heartbeat) {
    return result.Status == TEvWriteHeartbeatResult::EStatus::Written
        && result.WrittenVersion >= TRowVersion(heartbeat.GetStep(), heartbeat.GetTxId());
}

NPQ::THeartbeat MakeHeartbeat(IChangeRecordSerializer& serializer, const IChangeRecord& record) {
    NKikimrClient::TPersQueuePartitionRequest::TCmdWrite cmd;
    serializer.Serialize(cmd, record);
    return NPQ::THeartbeat::Parse(cmd.GetHeartbeat());
}

} // anonymous

TCdcAggregatedHeartbeats::TCdcAggregatedHeartbeats(THolder<IChangeRecordSerializer> serializer, ui64 sourceDataShardId)
    : Serializer(std::move(serializer))
    , SourceDataShardId(sourceDataShardId)
{
}

void TCdcAggregatedHeartbeats::SetAggregator(ui64 tabletId) {
    AggregatorTabletId = tabletId;
    SendReadyHeartbeat();
}

void TCdcAggregatedHeartbeats::RegisterPendingRecordIds(const TVector<TRecordInfo>& records) {
    for (const auto& record : records) {
        PendingRecords.insert(record.Order);
    }
}

void TCdcAggregatedHeartbeats::RemovePendingRecordIds(const TVector<ui64>& orders) {
    for (const auto order : orders) {
        PendingRecords.erase(order);
    }
    SendReadyHeartbeat();
}

std::pair<TVector<IChangeRecord::TPtr>, TVector<ui64>> TCdcAggregatedHeartbeats::BufferHeartbeats(TVector<IChangeRecord::TPtr> records) {
    TVector<IChangeRecord::TPtr> passed;
    TVector<ui64> buffered;
    for (auto& record : records) {
        if (record->GetKind() != IChangeRecord::EKind::CdcHeartbeat) {
            passed.push_back(std::move(record));
            continue;
        }

        const ui64 order = record->GetOrder();
        PendingRecords.erase(order);
        PendingHeartbeats.emplace(order, std::move(record));
        buffered.push_back(order);
    }

    SendReadyHeartbeat();
    return {std::move(passed), std::move(buffered)};
}

TVector<ui64> TCdcAggregatedHeartbeats::HandleCdcAggregatorServiceReply(TAutoPtr<IEventHandle>& ev) {
    if (!InFlight) {
        return {};
    }

    switch (ev->GetTypeRewrite()) {
    case TEvWriteHeartbeatResult::EventType: {
        const auto& result = *ev->Get<TEvWriteHeartbeatResult>();
        const bool written = IsWritten(result, *PendingHeartbeats.at(*InFlight));
        if (!written) {
            YDB_LOG_WARN("CdcAggregatorService has not written the heartbeat",
                {"eventDetails", result.ToString()});
        }

        if (written || result.Status == TEvWriteHeartbeatResult::EStatus::Rejected) {
            return CompleteInFlight();
        }
        return RetryInFlight();
    }
    case TEvents::TEvUndelivered::EventType:
        if (ev->Get<TEvents::TEvUndelivered>()->SourceType == TEvWriteHeartbeat::EventType) {
            return RetryInFlight();
        }
        break;
    }
    return {};
}

TVector<ui64> TCdcAggregatedHeartbeats::Clear() {
    InFlight.reset();
    PendingRecords.clear();
    return EraseUpTo(PendingHeartbeats, Max<ui64>());
}

void TCdcAggregatedHeartbeats::SendReadyHeartbeat() {
    if (!AggregatorTabletId || InFlight) {
        return;
    }

    InFlight = FindHeartbeatToSend(PendingHeartbeats, PendingRecords);
    if (InFlight) {
        Send(TDuration::Zero());
    }
}

TVector<ui64> TCdcAggregatedHeartbeats::CompleteInFlight() {
    auto completed = EraseUpTo(PendingHeartbeats, *InFlight);
    InFlight.reset();
    Backoff.Reset();
    SendReadyHeartbeat();
    return completed;
}

TVector<ui64> TCdcAggregatedHeartbeats::RetryInFlight() {
    TVector<ui64> covered;
    const auto newest = FindHeartbeatToSend(PendingHeartbeats, PendingRecords);
    if (newest && *newest > *InFlight) {
        covered = EraseUpTo(PendingHeartbeats, *newest - 1);
        InFlight = newest;
    }

    Send(Backoff.Next());
    return covered;
}

void TCdcAggregatedHeartbeats::Send(TDuration delay) {
    const auto& ctx = TActivationContext::AsActorContext();
    auto ev = MakeHolder<TEvWriteHeartbeat>();
    ev->RecipientTabletId = AggregatorTabletId;
    ev->SourceDataShardId = SourceDataShardId;
    ev->Heartbeat = MakeHeartbeat(*Serializer, *PendingHeartbeats.at(*InFlight));

    auto* handle = new IEventHandle(NCdcAggregator::MakeCdcAggregatorServiceId(ctx.SelfID.NodeId()), ctx.SelfID,
        ev.Release(), IEventHandle::FlagTrackDelivery);
    if (delay) {
        TActivationContext::Schedule(delay, handle);
    } else {
        TActivationContext::Send(handle);
    }
}

} // namespace NKikimr::NDataShard
