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

TCdcAggregatedHeartbeats::TCdcAggregatedHeartbeats(THolder<IChangeRecordSerializer> serializer, ui64 sourceDataShardId)
    : Serializer(std::move(serializer))
    , SourceDataShardId(sourceDataShardId)
{
}

void TCdcAggregatedHeartbeats::OnRecordsEnqueued(const TVector<TRecordInfo>& records) {
    if (!Serializer) {
        return;
    }

    for (const auto& record : records) {
        Undelivered.insert(record.Order);
    }
}

void TCdcAggregatedHeartbeats::OnRecordsRemoved(const TVector<ui64>& orders) {
    if (!Serializer) {
        return;
    }

    for (const auto order : orders) {
        Undelivered.erase(order);
    }
}

TVector<ui64> TCdcAggregatedHeartbeats::ExtractHeartbeats(TVector<NChangeExchange::IChangeRecord::TPtr>& records) {
    TVector<ui64> heartbeats;
    if (!Serializer) {
        return heartbeats;
    }

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
    return heartbeats;
}

TVector<ui64> TCdcAggregatedHeartbeats::SendIfNeeded(const TActorId& owner, ui64 aggregatorTabletId) {
    TVector<ui64> covered;
    if (!aggregatorTabletId) {
        return covered;
    }

    const auto ready = Undelivered ? Held.lower_bound(*Undelivered.begin()) : Held.end();
    if (ready == Held.begin()) {
        return covered;
    }

    const auto newest = std::prev(ready);
    if (Sent == newest->first) {
        return covered;
    }

    for (auto it = Held.begin(); it != newest; it = Held.erase(it)) {
        covered.push_back(it->first);
    }

    Owner = owner;
    AggregatorTabletId = aggregatorTabletId;
    Sent = newest->first;
    Send(TDuration::Zero());

    return covered;
}

TVector<ui64> TCdcAggregatedHeartbeats::OnWriteResult(const TEvWriteHeartbeatResult& result, ui64 cookie) {
    using EStatus = TEvWriteHeartbeatResult::EStatus;

    if (!Sent || cookie != Cookie) {
        return {};
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

    return {};
}

void TCdcAggregatedHeartbeats::OnUndelivered(ui64 cookie) {
    if (Sent && cookie == Cookie) {
        Send(Backoff.Next());
    }
}

TVector<ui64> TCdcAggregatedHeartbeats::Reset() {
    TVector<ui64> remove(Reserve(Held.size()));
    for (const auto& [order, _] : Held) {
        remove.push_back(order);
    }

    Held.clear();
    Sent.reset();
    return remove;
}

void TCdcAggregatedHeartbeats::Send(TDuration delay) {
    auto ev = MakeHolder<NCdcAggregator::TEvCdcAggregatorService::TEvWriteHeartbeat>();
    ev->RecipientTabletId = AggregatorTabletId;
    ev->SourceDataShardId = SourceDataShardId;
    ev->Heartbeat = Held.at(*Sent);

    auto* handle = new IEventHandle(NCdcAggregator::MakeCdcAggregatorServiceId(Owner.NodeId()), Owner,
        ev.Release(), IEventHandle::FlagTrackDelivery, ++Cookie);
    if (delay) {
        TActivationContext::Schedule(delay, handle);
    } else {
        TActivationContext::Send(handle);
    }
}

TVector<ui64> TCdcAggregatedHeartbeats::CompleteSent() {
    TVector<ui64> remove;
    const auto end = Held.upper_bound(*Sent);
    for (auto it = Held.begin(); it != end; it = Held.erase(it)) {
        remove.push_back(it->first);
    }

    Sent.reset();
    Backoff.Reset();
    return remove;
}

} // namespace NKikimr::NDataShard
