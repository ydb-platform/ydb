#include "ydb/core/persqueue/partition_scale_manager.h"

namespace NKikimr {
namespace NPQ {


TPartitionScaleManager::TPartitionScaleManager(
    const TString& topicName,
    const TString& databasePath,
    NKikimrPQ::TUpdateBalancerConfig& balancerConfig
)
    : TopicName(topicName)
    , DatabasePath(databasePath)
    , BalancerConfig(balancerConfig) {

    }

void TPartitionScaleManager::HandleScaleStatusChange(const TPartitionInfo& partition, NKikimrPQ::EScaleStatus scaleStatus, const TActorContext& ctx) {
    if (scaleStatus == NKikimrPQ::EScaleStatus::NEED_SPLIT) {
        PartitionsToSplit.emplace(partition.Id, partition);
        TrySendScaleRequest(ctx);
    } else {
        PartitionsToSplit.erase(partition.Id);
    }
}

void TPartitionScaleManager::TrySendScaleRequest(const TActorContext& ctx) {
    TInstant delayDeadline = LastResponseTime + RequestTimeout;
    if (DatabasePath.empty() || RequestInflight || delayDeadline > ctx.Now()) {
        return;
    }

    auto splitMergePair = BuildScaleRequest();
    if (splitMergePair.first.empty() && splitMergePair.second.empty()) {
        return;
    }

    RequestInflight = true;
    CurrentScaleRequest = ctx.Register(new TPartitionScaleRequest(
        TopicName,
        DatabasePath,
        BalancerConfig.PathId,
        BalancerConfig.PathVersion,
        splitMergePair.first,
        splitMergePair.second,
        ctx.SelfID
    ));
}


using TPartitionSplit = NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionSplit;
using TPartitionMerge = NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionMerge;

std::pair<std::vector<TPartitionSplit>, std::vector<TPartitionMerge>> TPartitionScaleManager::BuildScaleRequest() {
    std::vector<TPartitionSplit> splitsToApply;
    std::vector<TPartitionMerge> mergesToApply;

    size_t allowedSplitsCount = BalancerConfig.MaxActivePartitions > BalancerConfig.CurPartitions ? BalancerConfig.MaxActivePartitions - BalancerConfig.CurPartitions : 0;
    auto itSplit = PartitionsToSplit.begin();
    while (allowedSplitsCount > 0 && itSplit != PartitionsToSplit.end()) {
        const auto partitionId = itSplit->first;
        const auto& partition = itSplit->second;

        if (BalancerConfig.PartitionGraph.GetPartition(partitionId)->Children.empty()) {
            auto mid = GetRangeMid(partition.KeyRange.FromBound ? *partition.KeyRange.FromBound : "", partition.KeyRange.ToBound ?*partition.KeyRange.ToBound : "");
            if (mid.empty()) {
                itSplit = PartitionsToSplit.erase(itSplit);
                continue;
            }

            TPartitionSplit split;
            split.set_partition(partition.Id);
            split.set_splitboundary(mid);
            splitsToApply.push_back(split);

            allowedSplitsCount--;
            itSplit++;
        } else {
            itSplit = PartitionsToSplit.erase(itSplit);
        }
    }

    return {splitsToApply, mergesToApply};
}

void TPartitionScaleManager::HandleScaleRequestResult(TPartitionScaleRequest::TEvPartitionScaleRequestDone::TPtr& ev, const TActorContext& ctx) {
    RequestInflight = false;
    LastResponseTime = ctx.Now();
    auto result = ev->Get();
    if (result->Status == TEvTxUserProxy::TResultStatus::ExecComplete) {
        TrySendScaleRequest(ctx);
    } else {
        ui64 newTimeout = RequestTimeout.MilliSeconds() == 0 ? MIN_SCALE_REQUEST_REPEAT_SECONDS_TIMEOUT + RandomNumber<ui64>(50) : RequestTimeout.MilliSeconds() * 2;
        RequestTimeout = TDuration::MilliSeconds(std::min(newTimeout, static_cast<ui64>(MAX_SCALE_REQUEST_REPEAT_SECONDS_TIMEOUT)));
        ctx.Schedule(RequestTimeout, new TEvents::TEvWakeup(TRY_SCALE_REQUEST_WAKE_UP_TAG));
    }
}

void TPartitionScaleManager::Die(const TActorContext& ctx) {
    if (CurrentScaleRequest) {
        ctx.Send(CurrentScaleRequest, new TEvents::TEvPoisonPill());
    }
}

void TPartitionScaleManager::UpdateBalancerConfig(NKikimrPQ::TUpdateBalancerConfig& config) {
    BalancerConfig = TBalancerConfig(config);
}

void TPartitionScaleManager::UpdateDatabasePath(const TString& dbPath) {
    DatabasePath = dbPath;
}

TString TPartitionScaleManager::GetRangeMid(const TString& from, const TString& to) {
    if (from > to && to.size() != 0) {
        return "";
    }

    TStringBuilder result;

    unsigned char fromPadding = 0;
    unsigned char toPadding = 255;

    size_t maxSize = std::max(from.size(), to.size());
    for (size_t i = 0; i < maxSize; ++i) {
        ui16 fromChar = i < from.size() ? static_cast<ui16>(from[i]) : fromPadding;
        unsigned char toChar = i < to.size() ? static_cast<unsigned char>(to[i]) : toPadding;

        ui16 sum = fromChar + toChar;

        result += static_cast<unsigned char>(sum / 2);
    }

    if (result == from) {
        result += static_cast<unsigned char>(127);
    }
    return result;
}

} // namespace NPQ
} // namespace NKikimr
