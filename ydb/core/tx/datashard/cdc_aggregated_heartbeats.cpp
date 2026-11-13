#include "cdc_aggregated_heartbeats.h"
#include "change_record.h"

#include <ydb/core/change_exchange/visitor.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CHANGE_EXCHANGE

namespace NKikimr::NDataShard {

namespace {

class THeartbeatSerializer: public NChangeExchange::TBaseVisitor {
    IChangeRecordSerializer& Serializer;
    NKikimrClient::TPersQueuePartitionRequest::TCmdWrite& Cmd;

public:
    explicit THeartbeatSerializer(IChangeRecordSerializer& serializer, NKikimrClient::TPersQueuePartitionRequest::TCmdWrite& cmd)
        : Serializer(serializer)
        , Cmd(cmd)
    {
    }

    void Visit(const TChangeRecord& record) override {
        Serializer.Serialize(Cmd, record);
    }
};

} // anonymous

TCdcAggregatedHeartbeats::TCdcAggregatedHeartbeats(THolder<IChangeRecordSerializer> serializer, const TDataShardId& dataShard)
    : Serializer(std::move(serializer))
    , DataShard(dataShard)
{
}

void TCdcAggregatedHeartbeats::OnRecordsEnqueued(const TVector<TRecordInfo>& records) {
    for (const auto& record : records) {
        Undelivered.insert(record.Order);
    }
}

TVector<ui64> TCdcAggregatedHeartbeats::OnRecordsReceived(TVector<NChangeExchange::IChangeRecord::TPtr>& records) {
    TVector<ui64> heartbeats;
    TVector<NChangeExchange::IChangeRecord::TPtr> rest(Reserve(records.size()));
    for (auto& record : records) {
        if (record->GetKind() != NChangeExchange::IChangeRecord::EKind::CdcHeartbeat) {
            rest.push_back(std::move(record));
            continue;
        }

        NKikimrClient::TPersQueuePartitionRequest::TCmdWrite cmd;
        THeartbeatSerializer visitor(*Serializer, cmd);
        record->Accept(visitor);

        Undelivered.erase(record->GetOrder());
        Held.emplace(record->GetOrder(), NPQ::THeartbeat::Parse(cmd.GetHeartbeat()));
        heartbeats.push_back(record->GetOrder());
    }

    records = std::move(rest);
    if (heartbeats) {
        SendIfReady();
    }

    return heartbeats;
}

void TCdcAggregatedHeartbeats::OnRecordsRemoved(const TVector<ui64>& orders) {
    for (const auto order : orders) {
        Undelivered.erase(order);
    }

    SendIfReady();
}

void TCdcAggregatedHeartbeats::OnStreamResolved(ui64 aggregatorTabletId) {
    AggregatorTabletId = aggregatorTabletId;
    SendIfReady();
}

void TCdcAggregatedHeartbeats::OnWriteResult(const TEvWriteHeartbeatResult& result, ui64 cookie) {
    using EStatus = TEvWriteHeartbeatResult::EStatus;

    if (!Sent || cookie != Cookie) {
        return;
    }

    if (result.Status == EStatus::Written && result.WrittenVersion >= Held.at(*Sent).Version) {
        return CompleteSent();
    }

    if (result.Status == EStatus::Rejected) {
        YDB_LOG_WARN("CdcAggregatorService has rejected the heartbeat",
            {"eventDetails", result.ToString()});
        return CompleteSent();
    }

    YDB_LOG_WARN("CdcAggregatorService has not written the heartbeat, retrying",
        {"eventDetails", result.ToString()});
    Send(Backoff.Next());
}

void TCdcAggregatedHeartbeats::OnUndelivered(ui64 cookie) {
    if (Sent && cookie == Cookie) {
        Send(Backoff.Next());
    }
}

void TCdcAggregatedHeartbeats::RemoveAll() {
    TVector<ui64> orders(Reserve(Held.size()));
    for (const auto& [order, _] : Held) {
        orders.push_back(order);
    }

    Held.clear();
    Sent.reset();
    Remove(std::move(orders));
}

void TCdcAggregatedHeartbeats::SendIfReady() {
    if (!AggregatorTabletId) {
        return;
    }

    const auto ready = Undelivered ? Held.lower_bound(*Undelivered.begin()) : Held.end();
    if (ready == Held.begin()) {
        return;
    }

    const auto newest = std::prev(ready);
    if (Sent == newest->first) {
        return;
    }

    TVector<ui64> covered;
    for (auto it = Held.begin(); it != newest; it = Held.erase(it)) {
        covered.push_back(it->first);
    }

    Sent = newest->first;
    Remove(std::move(covered));
    Send(TDuration::Zero());
}

void TCdcAggregatedHeartbeats::Send(TDuration delay) {
    const auto& ctx = TActivationContext::AsActorContext();

    auto ev = MakeHolder<NCdcAggregator::TEvCdcAggregatorService::TEvWriteHeartbeat>();
    ev->RecipientTabletId = AggregatorTabletId;
    ev->SourceDataShardId = DataShard.TabletId;
    ev->Heartbeat = Held.at(*Sent);

    auto* handle = new IEventHandle(NCdcAggregator::MakeCdcAggregatorServiceId(ctx.SelfID.NodeId()), ctx.SelfID,
        ev.Release(), IEventHandle::FlagTrackDelivery, ++Cookie);
    if (delay) {
        TActivationContext::Schedule(delay, handle);
    } else {
        TActivationContext::Send(handle);
    }
}

void TCdcAggregatedHeartbeats::CompleteSent() {
    TVector<ui64> orders;
    const auto end = Held.upper_bound(*Sent);
    for (auto it = Held.begin(); it != end; it = Held.erase(it)) {
        orders.push_back(it->first);
    }

    Sent.reset();
    Backoff.Reset();
    Remove(std::move(orders));
}

void TCdcAggregatedHeartbeats::Remove(TVector<ui64>&& orders) {
    if (orders) {
        TActivationContext::AsActorContext().Send(DataShard.ActorId,
            new NChangeExchange::TEvChangeExchange::TEvRemoveRecords(std::move(orders)));
    }
}

} // namespace NKikimr::NDataShard
