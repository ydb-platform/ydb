#include "cdc_aggregated_heartbeats.h"
#include "change_record.h"

#include <ydb/core/change_exchange/visitor.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/library/aclib/user_context.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CHANGE_EXCHANGE

namespace NKikimr::NDataShard {

namespace {

struct THeartbeatSerializer: public NChangeExchange::TBaseVisitor {
    IChangeRecordSerializer& Serializer;
    NKikimrClient::TPersQueuePartitionRequest::TCmdWrite Cmd;

    explicit THeartbeatSerializer(IChangeRecordSerializer& serializer)
        : Serializer(serializer)
    {
    }

    void Visit(const TChangeRecord& record) override {
        Serializer.Serialize(Cmd, record);
    }
};

} // anonymous

TCdcAggregatedHeartbeats::TCdcAggregatedHeartbeats(THolder<IChangeRecordSerializer> serializer, ui64 sourceDataShardId)
    : Serializer(std::move(serializer))
    , SourceDataShardId(sourceDataShardId)
{
}

void TCdcAggregatedHeartbeats::SetAggregatorTabletId(ui64 aggregatorTabletId) {
    AggregatorTabletId = aggregatorTabletId;
}

void TCdcAggregatedHeartbeats::TrackRecords(const TVector<TRecordInfo>& records) {
    for (const auto& record : records) {
        Undelivered.insert(record.Order);
    }
}

TVector<ui64> TCdcAggregatedHeartbeats::ExtractHeartbeats(TVector<NChangeExchange::IChangeRecord::TPtr>& records) {
    TVector<ui64> heartbeats;
    TVector<NChangeExchange::IChangeRecord::TPtr> rest(Reserve(records.size()));
    for (auto& record : records) {
        if (record->GetKind() != NChangeExchange::IChangeRecord::EKind::CdcHeartbeat) {
            rest.push_back(std::move(record));
            continue;
        }

        THeartbeatSerializer visitor(*Serializer);
        record->Accept(visitor);
        Undelivered.erase(record->GetOrder());
        Held.emplace(record->GetOrder(), NPQ::THeartbeat::Parse(visitor.Cmd.GetHeartbeat()));
        heartbeats.push_back(record->GetOrder());
    }

    records = std::move(rest);
    return heartbeats;
}

void TCdcAggregatedHeartbeats::UntrackRecords(const TVector<ui64>& orders) {
    for (const auto order : orders) {
        Undelivered.erase(order);
    }
}

TVector<ui64> TCdcAggregatedHeartbeats::SendReadyHeartbeat() {
    const auto ready = Undelivered ? Held.lower_bound(*Undelivered.begin()) : Held.end();
    if (!AggregatorTabletId || ready == Held.begin() || Sent == std::prev(ready)->first) {
        return {};
    }

    auto covered = RemoveHeld(std::prev(ready));
    Sent = Held.begin()->first;
    Send(TDuration::Zero());
    return covered;
}

TVector<ui64> TCdcAggregatedHeartbeats::HandleWriteResult(const TEvWriteHeartbeatResult& result, ui64 cookie) {
    using EStatus = TEvWriteHeartbeatResult::EStatus;
    if (!Sent || cookie != Cookie) {
        return {};
    }

    if (result.Status != EStatus::Written || result.WrittenVersion < Held.at(*Sent).Version) {
        YDB_LOG_WARN("CdcAggregatorService has not written the heartbeat",
            {"eventDetails", result.ToString()});
        if (result.Status != EStatus::Rejected) {
            Send(Backoff.Next());
            return {};
        }
    }

    auto confirmed = RemoveHeld(Held.upper_bound(*Sent));
    Sent.reset();
    Backoff.Reset();
    return confirmed;
}

void TCdcAggregatedHeartbeats::RetryHeartbeat(ui64 cookie) {
    if (Sent && cookie == Cookie) {
        Send(Backoff.Next());
    }
}

TVector<ui64> TCdcAggregatedHeartbeats::ReleaseHeartbeats() {
    Sent.reset();
    return RemoveHeld(Held.end());
}

void TCdcAggregatedHeartbeats::Send(TDuration delay) {
    const auto& ctx = TActivationContext::AsActorContext();
    auto ev = MakeHolder<NCdcAggregator::TEvCdcAggregatorService::TEvWriteHeartbeat>();
    ev->RecipientTabletId = AggregatorTabletId;
    ev->SourceDataShardId = SourceDataShardId;
    ev->Heartbeat = Held.at(*Sent);

    auto* handle = new IEventHandle(NCdcAggregator::MakeCdcAggregatorServiceId(ctx.SelfID.NodeId()), ctx.SelfID,
        ev.Release(), IEventHandle::FlagTrackDelivery, ++Cookie);
    if (delay) {
        TActivationContext::Schedule(delay, handle);
    } else {
        TActivationContext::Send(handle);
    }
}

TVector<ui64> TCdcAggregatedHeartbeats::RemoveHeld(TMap<ui64, NPQ::THeartbeat>::iterator end) {
    TVector<ui64> orders;
    for (auto it = Held.begin(); it != end; it = Held.erase(it)) {
        orders.push_back(it->first);
    }
    return orders;
}

} // namespace NKikimr::NDataShard
