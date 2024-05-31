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
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, "TPartitionScaleManager::HandleScaleStatusChange "
            << "need to split partition " << partition.Id);
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
    auto itSplit = PartitionsToSplit.begin();
    while (allowedSplitsCount > 0 && itSplit != PartitionsToSplit.end()) {
        const auto partitionId = itSplit->first;
        const auto& partition = itSplit->second;

        if (BalancerConfig.PartitionGraph.GetPartition(partitionId)->Children.empty()) {
            auto from = partition.KeyRange.FromBound ? *partition.KeyRange.FromBound : "";
            auto to = partition.KeyRange.ToBound ?*partition.KeyRange.ToBound : "";
            auto mid = GetRangeMid(from, to);
            if (mid.empty()) {
                itSplit = PartitionsToSplit.erase(itSplit);
                LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                        "TPartitionScaleManager::BuildScaleRequest wrong partition key range. Can't get mid. Topic# " << TopicName << ", partition# " << partitionId);
                continue;
            }
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                    "TPartitionScaleManager::BuildScaleRequest partition split ranges. From# '" << ToHex(from)
                    << "'. To# '" << ToHex(to) << "'. Mid# '" << ToHex(mid)
                    << "'. Topic# " << TopicName << ". Partition# " << partitionId);

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

void TPartitionScaleManager::UpdateBalancerConfig(NKikimrPQ::TUpdateBalancerConfig& config) {
    BalancerConfig = TBalancerConfig(config);
}

void TPartitionScaleManager::UpdateDatabasePath(const TString& dbPath) {
    DatabasePath = dbPath;
}

std::pair<ui16, bool> Mid(ui16 a, ui16 b) {
    if (a == 0xFF) {
        return {0xFF, false};
    }
    if (a + 1 < b) {
        return {(a + b) / 2, true};
    }
    if (b < a) {
        ui16 n = (a + b + 0x100) / 2;
        return {(n < 0x100) ? n : 0xFF, true};
    }

    return {a, false};
}

TString TPartitionScaleManager::GetRangeMid(const TString& from, const TString& to) {
    if (from > to && to.size() != 0) {
        return "";
    }

    auto GetChar = [](const TString& str, size_t i, unsigned char defaultValue) {
        if (i >= str.size()) {
            return defaultValue;
        }
        return static_cast<unsigned char>(str[i]);
    };

    TStringBuilder result;
    if (from.empty() && to.empty()) {
        result << static_cast<unsigned char>(0x7F);
        return result;
    }

    bool splitted = false;

    size_t maxSize = std::max(from.size(), to.size());
    for (size_t i = 0; i < maxSize; ++i) {
        ui16 f = GetChar(from, i, 0);
        ui16 t = GetChar(to, i, 0xFF);

        if (!splitted) {
            auto [n, s] = Mid(f, t);
            result << static_cast<unsigned char>(n);
            splitted = s;
        } else {
            auto n = (f + t) / 2;
            result << static_cast<unsigned char>(n);
            break;
        }
    }

    if (result == from) {
        result << static_cast<unsigned char>(0xFF);
    }
    return result;
}

} // namespace NPQ
} // namespace NKikimr
