#include "overload_subscribers.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/library/actors/core/interconnect.h>

namespace NKikimr::NColumnShard::NOverload {

namespace {

[[maybe_unused]] void SendViaSession(const TActorId& sessionId,
    const TActorId& target,
    const TActorId& src,
    IEventBase* event,
    ui32 flags = 0,
    ui64 cookie = 0,
    NWilson::TTraceId traceId = {}) {
    THolder<IEventHandle> ev = MakeHolder<IEventHandle>(target, src, event, flags, cookie, nullptr, std::move(traceId));

    if (sessionId) {
        ev->Rewrite(TEvInterconnect::EvForward, sessionId);
    }

    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "Send overload ready");

    TActivationContext::Send(ev.Release());
}

} // namespace

// void TOverloadSubscribers::AddPipeServer(const NActors::TActorId& serverId, const NActors::TActorId& interconnectSession) {
//     auto res = PipeServers.emplace(
//         std::piecewise_construct,
//         std::forward_as_tuple(serverId),
//         std::forward_as_tuple());
//     AFL_VERIFY(res.second)("serverId", serverId);

// res.first->second.InterconnectSession = interconnectSession;
// }

// void TOverloadSubscribers::RemovePipeServer(const NActors::TActorId& serverId) {
//     auto it = PipeServers.find(serverId);
//     AFL_VERIFY(it != PipeServers.end())("serverId", serverId);

// DiscardOverloadSubscribers(it->second);

// PipeServers.erase(it);
// }

void TOverloadSubscribers::AddOverloadSubscriber(const TColumnShardInfo& columnShardInfo,
    const TPipeServerInfo& pipeServerInfo,
    const TOverloadSubscriberInfo& overloadSubscriberInfo) {
    AFL_VERIFY(pipeServerInfo.PipeServerId == overloadSubscriberInfo.PipeServerId);

    auto& columnShardSubscriber = ColumnShardsOverloadSubscribers[columnShardInfo.ColumnShardId];
    auto subscriptionInfoIt = columnShardSubscriber.find(pipeServerInfo.PipeServerId);
    if (subscriptionInfoIt == columnShardSubscriber.end()) {
        subscriptionInfoIt = columnShardSubscriber.emplace(pipeServerInfo.PipeServerId, TSubscriptionInfo{
            .InterconnectSessionId = pipeServerInfo.InterconnectSessionId,
            .ColumnShardTabletId = columnShardInfo.TabletId,
        }).first;
    } else {
        auto& subscriptionInfo = subscriptionInfoIt->second;
        AFL_VERIFY(subscriptionInfo.InterconnectSessionId == pipeServerInfo.InterconnectSessionId);
        AFL_VERIFY(subscriptionInfo.ColumnShardTabletId == columnShardInfo.TabletId);
    }
    auto& subscriptionInfo = subscriptionInfoIt->second;
    if (auto& seqNo = subscriptionInfo.OverloadSubscribers[overloadSubscriberInfo.OverloadSubscriberId]; seqNo < overloadSubscriberInfo.SeqNo) {
        seqNo = overloadSubscriberInfo.SeqNo;
    }
}

void TOverloadSubscribers::RemoveOverloadSubscriber(const TColumnShardInfo& columnShardInfo, const TOverloadSubscriberInfo& overloadSubscriberInfo) {
    auto infoByPipeServerIdIt = ColumnShardsOverloadSubscribers.find(columnShardInfo.ColumnShardId);
    if (infoByPipeServerIdIt == ColumnShardsOverloadSubscribers.end()) {
        return;
    }
    auto& infoByPipeServerId = infoByPipeServerIdIt->second;

    auto subscriptionInfoIt = infoByPipeServerId.find(overloadSubscriberInfo.PipeServerId);
    if (subscriptionInfoIt == infoByPipeServerId.end()) {
        return;
    }
    auto& subscriptionInfo = subscriptionInfoIt->second;

    subscriptionInfo.OverloadSubscribers.erase(overloadSubscriberInfo.OverloadSubscriberId);
    if (subscriptionInfo.OverloadSubscribers.empty()) {
        infoByPipeServerId.erase(subscriptionInfoIt);
    }
    if (infoByPipeServerId.empty()) {
        ColumnShardsOverloadSubscribers.erase(infoByPipeServerIdIt);
    }
}

void TOverloadSubscribers::RemovePipeServer(const TColumnShardInfo& columnShardInfo, const TPipeServerInfo& pipeServerInfo) {
    auto infoByPipeServerIdIt = ColumnShardsOverloadSubscribers.find(columnShardInfo.ColumnShardId);
    if (infoByPipeServerIdIt == ColumnShardsOverloadSubscribers.end()) {
        return;
    }

    auto& infoByPipeServerId = infoByPipeServerIdIt->second;
    infoByPipeServerId.erase(pipeServerInfo.PipeServerId);
    if (infoByPipeServerId.empty()) {
        ColumnShardsOverloadSubscribers.erase(infoByPipeServerIdIt);
    }
}

void TOverloadSubscribers::NotifyAllOverloadSubscribers() {
    for (const auto& [source, infoByPipeServerId] : ColumnShardsOverloadSubscribers) {
        for (const auto& [_, subscriptionInfo] : infoByPipeServerId) {
            for (const auto& [target, seqNo] : subscriptionInfo.OverloadSubscribers) {
                SendViaSession(
                    subscriptionInfo.InterconnectSessionId,
                    target,
                    source,
                    new TEvColumnShard::TEvOverloadReady(subscriptionInfo.ColumnShardTabletId, seqNo));
            }
        }
    }
    ColumnShardsOverloadSubscribers.clear();
}

void TOverloadSubscribers::NotifyColumnShardSubscribers(const TColumnShardInfo& columnShardInfo) {
    auto infoByPipeServerIdIt = ColumnShardsOverloadSubscribers.find(columnShardInfo.ColumnShardId);
    if (infoByPipeServerIdIt == ColumnShardsOverloadSubscribers.end()) {
        return;
    }

    const auto& infoByPipeServerId = infoByPipeServerIdIt->second;
    for (const auto& [_, subscriptionInfo] : infoByPipeServerId) {
        for (const auto& [target, seqNo] : subscriptionInfo.OverloadSubscribers) {
            AFL_VERIFY(columnShardInfo.TabletId == subscriptionInfo.ColumnShardTabletId);
            SendViaSession(
                subscriptionInfo.InterconnectSessionId,
                target,
                columnShardInfo.ColumnShardId,
                new TEvColumnShard::TEvOverloadReady(subscriptionInfo.ColumnShardTabletId, seqNo));
        }
    }

    ColumnShardsOverloadSubscribers.erase(infoByPipeServerIdIt);
}

// void TOverloadSubscribers::DiscardOverloadSubscribers(TPipeServerInfo1& pipeServer) {
//     for (auto it = pipeServer.OverloadSubscribers.begin(); it != pipeServer.OverloadSubscribers.end(); ++it) {
//         TOverloadSubscriber& entry = it->second;
//         EnumerateRejectReasons(entry.Reasons, [&](ERejectReason reason) {
//             OverloadSubscribersByReason[RejectReasonIndex(reason)]--;
//         });
//     }
//     pipeServer.OverloadSubscribers.clear();
//     PipeServersWithOverloadSubscribers.Remove(&pipeServer);
// }

// bool TOverloadSubscribers::AddOverloadSubscriber(const TActorId& pipeServerId, const TActorId& actorId, ui64 seqNo, ERejectReasons reasons) {
//     auto it = PipeServers.find(pipeServerId);
//     if (it == PipeServers.end()) {
//         return false;
//     }

// bool wasEmpty = it->second.OverloadSubscribers.empty();
// auto& entry = it->second.OverloadSubscribers[actorId];
// if (entry.SeqNo <= seqNo) {
//     entry.SeqNo = seqNo;
//     // Increment counter for every new reason
//     EnumerateRejectReasons(reasons - entry.Reasons, [&](ERejectReason reason) {
//         OverloadSubscribersByReason[RejectReasonIndex(reason)]++;
//     });
//     entry.Reasons |= reasons;
// }

// if (wasEmpty) {
//     PipeServersWithOverloadSubscribers.PushBack(&it->second);
// }

// return true;
// }

// bool TOverloadSubscribers::HasPipeServer(const TActorId& pipeServerId) {
//     return PipeServers.contains(pipeServerId);
// }

// void TOverloadSubscribers::NotifyOverloadSubscribers(ERejectReason reason, const TActorId& sourceActorId, ui64 sourceTabletId) {
//     if (OverloadSubscribersByReason[RejectReasonIndex(reason)] == 0) {
//         // Avoid spending time when we know it is pointless
//         return;
//     }
//     ERejectReasons reasons = MakeRejectReasons(reason);

//     TPipeServersWithOverloadSubscribers left;
//     while (!PipeServersWithOverloadSubscribers.Empty()) {
//         TPipeServerInfo* pipeServer = PipeServersWithOverloadSubscribers.PopFront();
//         for (auto current = pipeServer->OverloadSubscribers.begin(); current != pipeServer->OverloadSubscribers.end(); ++current) {
//             const TActorId& actorId = current->first;
//             TOverloadSubscriber& entry = current->second;
//             if ((entry.Reasons & reasons) != reasons) {
//                 // Reasons don't match
//                 continue;
//             }
//             entry.Reasons -= reasons;
//             OverloadSubscribersByReason[RejectReasonIndex(reason)]--;
//             if (entry.Reasons == ERejectReasons::None) {
//                 SendViaSession(
//                     pipeServer->InterconnectSession,
//                     actorId,
//                     sourceActorId,
//                     new TEvColumnShard::TEvOverloadReady(sourceTabletId, entry.SeqNo));
//                 pipeServer->OverloadSubscribers.erase(current);
//             }
//         }
//         if (!pipeServer->OverloadSubscribers.empty()) {
//             left.PushBack(pipeServer);
//         }
//     }
//     PipeServersWithOverloadSubscribers.Append(left);
// }

// void TOverloadSubscribers::NotifyAllOverloadSubscribers(const TActorId& sourceActorId, ui64 sourceTabletId) {
//     bool clearedSubscribers = false;
//     while (!PipeServersWithOverloadSubscribers.Empty()) {
//         TPipeServerInfo* pipeServer = PipeServersWithOverloadSubscribers.PopFront();
//         for (auto it = pipeServer->OverloadSubscribers.begin(); it != pipeServer->OverloadSubscribers.end(); ++it) {
//             const TActorId& actorId = it->first;
//             TOverloadSubscriber& entry = it->second;
//             SendViaSession(
//                 pipeServer->InterconnectSession,
//                 actorId,
//                 sourceActorId,
//                 new TEvColumnShard::TEvOverloadReady(sourceTabletId, entry.SeqNo));
//         }
//         pipeServer->OverloadSubscribers.clear();
//         clearedSubscribers = true;
//     }

// if (clearedSubscribers) {
//     for (int i = 0; i < RejectReasonCount; ++i) {
//         OverloadSubscribersByReason[i] = 0;
//     }
// }
// }

// void TOverloadSubscribers::RemoveOverloadSubscriber(TSeqNo seqNo, const TActorId& recipient, const TActorId& sender) {
//     if (auto* pipeServer = PipeServers.FindPtr(recipient)) {
//         auto it = pipeServer->OverloadSubscribers.find(sender);
//         if (it != pipeServer->OverloadSubscribers.end()) {
//             if (it->second.SeqNo == seqNo) {
//                 EnumerateRejectReasons(it->second.Reasons, [&](ERejectReason reason) {
//                     OverloadSubscribersByReason[RejectReasonIndex(reason)]--;
//                 });
//                 pipeServer->OverloadSubscribers.erase(it);
//                 if (pipeServer->OverloadSubscribers.empty()) {
//                     PipeServersWithOverloadSubscribers.Remove(pipeServer);
//                 }
//             }
//         }
//     }
// }

// void TOverloadSubscribers::ScheduleNotification(const TActorId& actorId) {
//     if (InFlightNotification) {
//         return;
//     }
//     InFlightNotification = true;
//     TActivationContext::Schedule(TDuration::MilliSeconds(200), new IEventHandle(actorId, actorId, new NActors::TEvents::TEvWakeup(2)));
// }

// void TOverloadSubscribers::ProcessNotification() {
//     InFlightNotification = false;
// }

} // namespace NKikimr::NColumnShard::NOverload
