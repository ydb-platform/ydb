#include "heartbeat_queue.h"

#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <algorithm>

namespace NKikimr::NCdcAggregator {

THeartbeatQueue::THeartbeatQueue(ui32 maxBatchSize, ui32 maxWaiters)
    : MaxBatchSize(maxBatchSize)
    , MaxWaiters(maxWaiters)
{
    Y_ENSURE(MaxBatchSize > 0);
}

bool THeartbeatQueue::Enqueue(ui64 sourceId, const NPQ::THeartbeat& heartbeat, const TActorId& sender, ui64 cookie) {
    if (WaitersCount >= MaxWaiters) {
        return false;
    }

    auto& source = Sources[sourceId];
    auto inFlight = InFlight.find(sourceId);
    if (inFlight == InFlight.end() || inFlight->second < heartbeat.Version) {
        if (!source.Pending) {
            Queue.push_back(sourceId);
        }
        if (!source.Pending || source.Pending->Version < heartbeat.Version) {
            source.Pending = heartbeat;
        }
    }

    source.Waiters.push_back(TWaiter{
        .Sender = sender,
        .Cookie = cookie,
        .Version = heartbeat.Version,
    });
    ++WaitersCount;

    return true;
}

bool THeartbeatQueue::CanSend() const {
    return InFlight.empty() && !Queue.empty();
}

bool THeartbeatQueue::IsIdle() const {
    return Sources.empty();
}

ui32 THeartbeatQueue::GetWaitersCount() const {
    return WaitersCount;
}

THolder<TEvCdcAggregator::TEvWriteHeartbeats> THeartbeatQueue::NextBatch() {
    Y_ENSURE(InFlight.empty());

    auto ev = MakeHolder<TEvCdcAggregator::TEvWriteHeartbeats>();
    while (!Queue.empty() && InFlight.size() < MaxBatchSize) {
        const ui64 sourceId = Queue.front();
        Queue.pop_front();

        auto& source = Sources.at(sourceId);
        auto& item = *ev->Record.AddHeartbeats();
        item.SetSourceDataShardId(sourceId);
        source.Pending->Serialize(*item.MutableHeartbeat());

        InFlight.emplace(sourceId, source.Pending->Version);
        source.Pending.reset();
    }

    return ev;
}

TVector<THeartbeatQueue::TReply> THeartbeatQueue::OnResult(const NKikimrCdcAggregator::TEvWriteHeartbeatsResult& record) {
    using TResult = NKikimrCdcAggregator::TEvWriteHeartbeatsResult;

    TVector<TReply> replies;
    if (InFlight.empty()) {
        return replies;
    }

    switch (record.GetStatus()) {
    case TResult::SUCCESS:
        break;
    case TResult::BAD_REQUEST:
        CompleteInFlight(EStatus::Rejected, record.GetError(), replies);
        return replies;
    default:
        return Fail(TStringBuilder() << TResult::EStatus_Name(record.GetStatus()) << ": " << record.GetError());
    }

    for (const auto& result : record.GetResults()) {
        auto it = InFlight.find(result.GetSourceDataShardId());
        if (it == InFlight.end()) {
            continue;
        }

        auto& source = Sources.at(it->first);
        switch (result.GetStatus()) {
        case TResult::THeartbeatResult::SUCCESS: {
            const auto writtenVersion = TRowVersion::FromProto(result.GetWrittenVersion());
            Complete(source, writtenVersion, EStatus::Written, writtenVersion, {}, replies);
            break;
        }
        case TResult::THeartbeatResult::UNKNOWN_SOURCE:
        case TResult::THeartbeatResult::BAD_REQUEST:
            Complete(source, it->second, EStatus::Rejected, TRowVersion::Min(),
                TString(TResult::THeartbeatResult::EStatus_Name(result.GetStatus())), replies);
            break;
        default:
            break;
        }
    }

    CompleteInFlight(EStatus::Retry, "Heartbeat is not confirmed", replies);
    return replies;
}

TVector<THeartbeatQueue::TReply> THeartbeatQueue::Fail(const TString& error) {
    TVector<TReply> replies;
    for (auto& [_, source] : Sources) {
        Complete(source, TRowVersion::Max(), EStatus::Retry, TRowVersion::Min(), error, replies);
    }

    Sources.clear();
    Queue.clear();
    InFlight.clear();

    return replies;
}

void THeartbeatQueue::Complete(TSource& source, const TRowVersion& upTo, EStatus status,
        const TRowVersion& writtenVersion, const TString& error, TVector<TReply>& replies)
{
    auto it = std::stable_partition(source.Waiters.begin(), source.Waiters.end(), [&upTo](const TWaiter& waiter) {
        return upTo < waiter.Version;
    });

    for (auto waiter = it; waiter != source.Waiters.end(); ++waiter) {
        replies.push_back(TReply{
            .Sender = waiter->Sender,
            .Cookie = waiter->Cookie,
            .Status = status,
            .WrittenVersion = writtenVersion,
            .Error = error,
        });
    }

    WaitersCount -= std::distance(it, source.Waiters.end());
    source.Waiters.erase(it, source.Waiters.end());
}

void THeartbeatQueue::CompleteInFlight(EStatus status, const TString& error, TVector<TReply>& replies) {
    for (const auto& [sourceId, version] : InFlight) {
        auto it = Sources.find(sourceId);
        Y_ENSURE(it != Sources.end());

        Complete(it->second, version, status, TRowVersion::Min(), error, replies);
        if (!it->second.Pending && it->second.Waiters.empty()) {
            Sources.erase(it);
        }
    }

    InFlight.clear();
}

} // namespace NKikimr::NCdcAggregator
