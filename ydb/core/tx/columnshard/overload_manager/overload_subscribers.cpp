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

    TActivationContext::Send(ev.Release());
}

} // namespace

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
                Counters.OnOverloadReady();
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
            Counters.OnOverloadReady();
        }
    }

    ColumnShardsOverloadSubscribers.erase(infoByPipeServerIdIt);
}

} // namespace NKikimr::NColumnShard::NOverload
