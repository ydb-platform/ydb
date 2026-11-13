#include "cdc_aggregated_heartbeats.h"
#include "change_record.h"

#include <ydb/core/change_exchange/visitor.h>
#include <ydb/core/protos/msgbus_pq.pb.h>

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

TCdcAggregatedHeartbeats::TCdcAggregatedHeartbeats(THolder<IChangeRecordSerializer> serializer)
    : Serializer(std::move(serializer))
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

std::optional<TCdcAggregatedHeartbeats::TToSend> TCdcAggregatedHeartbeats::TakeHeartbeatToSend() {
    const auto ready = Undelivered ? Held.lower_bound(*Undelivered.begin()) : Held.end();
    if (ready == Held.begin()) {
        return std::nullopt;
    }

    const auto newest = std::prev(ready);
    if (Sent == newest->first) {
        return std::nullopt;
    }

    TToSend result{.Heartbeat = newest->second};
    for (auto it = Held.begin(); it != newest; it = Held.erase(it)) {
        result.Covered.push_back(it->first);
    }

    Sent = newest->first;
    return result;
}

const NPQ::THeartbeat* TCdcAggregatedHeartbeats::GetSentHeartbeat() const {
    return Sent ? &Held.at(*Sent) : nullptr;
}

TVector<ui64> TCdcAggregatedHeartbeats::CompleteSentHeartbeat() {
    Y_ENSURE(Sent);

    TVector<ui64> remove;
    const auto end = Held.upper_bound(*Sent);
    for (auto it = Held.begin(); it != end; it = Held.erase(it)) {
        remove.push_back(it->first);
    }

    Sent.reset();
    return remove;
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

} // namespace NKikimr::NDataShard
