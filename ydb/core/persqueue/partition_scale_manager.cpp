#include "partition_scale_manager.h"

#include <ydb/core/persqueue/partition_key_range/partition_key_range.h>

namespace NKikimr {
namespace NPQ {


TPartitionScaleManager::TPartitionScaleManager(
    const TString& topicName,
    const TString& databasePath,
    ui64 pathId,
    int version,
    const NKikimrPQ::TPQTabletConfig& config
)
    : TopicName(topicName)
    , DatabasePath(databasePath)
    , BalancerConfig(pathId, version, config) {
    }

void TPartitionScaleManager::HandleScaleStatusChange(const ui32 partitionId, NKikimrPQ::EScaleStatus scaleStatus, const TActorContext& ctx) {
    if (scaleStatus == NKikimrPQ::EScaleStatus::NEED_SPLIT) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, "TPartitionScaleManager::HandleScaleStatusChange "
            << "need to split partition " << partitionId);
        PartitionsToSplit.insert(partitionId);
        TrySendScaleRequest(ctx);
    } else {
        PartitionsToSplit.erase(partitionId);
    }
}

void TPartitionScaleManager::TrySendScaleRequest(const TActorContext& ctx) {
    TInstant delayDeadline = LastResponseTime + RequestTimeout;
    if (DatabasePath.empty() || RequestInflight || delayDeadline > ctx.Now()) {
        return;
    }

    auto splitMergePair = BuildScaleRequest(ctx);
    if (splitMergePair.first.empty() && splitMergePair.second.empty()) {
        return;
    }

    RequestInflight = true;
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, "TPartitionScaleManager::HandleScaleStatusChange "
        << "send split request");
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

const TString ToHex(const TString& value) {
    return TStringBuilder() << HexText(TBasicStringBuf(value));
}

std::pair<std::vector<TPartitionSplit>, std::vector<TPartitionMerge>> TPartitionScaleManager::BuildScaleRequest(const TActorContext& ctx) {
    std::vector<TPartitionSplit> splitsToApply;
    std::vector<TPartitionMerge> mergesToApply;

    size_t allowedSplitsCount = BalancerConfig.MaxActivePartitions > BalancerConfig.CurPartitions ? BalancerConfig.MaxActivePartitions - BalancerConfig.CurPartitions : 0;
    auto partitionId = PartitionsToSplit.begin();
    while (allowedSplitsCount > 0 && partitionId != PartitionsToSplit.end()) {
        auto* node = BalancerConfig.PartitionGraph.GetPartition(*partitionId);
        if (node->Children.empty()) {
            auto from = node->From;
            auto to = node->To;
            auto mid = MiddleOf(from, to);
            if (mid.empty()) {
                partitionId = PartitionsToSplit.erase(partitionId);
                LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                        "TPartitionScaleManager::BuildScaleRequest wrong partition key range. Can't get mid. Topic# " << TopicName << ", partition# " << *partitionId);
                continue;
            }
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                    "TPartitionScaleManager::BuildScaleRequest partition split ranges. From# '" << ToHex(from)
                    << "'. To# '" << ToHex(to) << "'. Mid# '" << ToHex(mid)
                    << "'. Topic# " << TopicName << ". Partition# " << *partitionId);

            TPartitionSplit split;
            split.set_partition(*partitionId);
            split.set_splitboundary(mid);
            splitsToApply.push_back(split);

            --allowedSplitsCount;
            ++partitionId;
        } else {
            partitionId = PartitionsToSplit.erase(partitionId);
        }
    }

    return {splitsToApply, mergesToApply};
}

void TPartitionScaleManager::HandleScaleRequestResult(TPartitionScaleRequest::TEvPartitionScaleRequestDone::TPtr& ev, const TActorContext& ctx) {
    RequestInflight = false;
    LastResponseTime = ctx.Now();
    auto result = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            "TPartitionScaleManager::HandleScaleRequestResult scale request result: " << result->Status << ". Topic# " << TopicName);
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

void TPartitionScaleManager::UpdateBalancerConfig(ui64 pathId, int version, const NKikimrPQ::TPQTabletConfig& config) {
    BalancerConfig = TBalancerConfig(pathId, version, config);
}

void TPartitionScaleManager::UpdateDatabasePath(const TString& dbPath) {
    DatabasePath = dbPath;
}

} // namespace NPQ
} // namespace NKikimr
