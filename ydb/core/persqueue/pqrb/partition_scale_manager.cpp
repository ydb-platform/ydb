#include "partition_scale_manager.h"
#include "read_balancer_log.h"

#include <ydb/core/persqueue/partition_key_range/partition_key_range.h>
#include <fmt/format.h>
#include <algorithm>

namespace NKikimr {
namespace NPQ {


TPartitionScaleManager::TPartitionScaleManager(
    const TString& topicName,
    const TString& topicPath,
    const TString& databasePath,
    ui64 pathId,
    int version,
    const NKikimrPQ::TPQTabletConfig& config,
    const TPartitionGraph& partitionGraph
)
    : TopicName(topicName)
    , TopicPath(topicPath)
    , DatabasePath(databasePath)
    , BalancerConfig(pathId, version, config)
    , PartitionGraph(partitionGraph)
    , MirroredFromSomewhere(MirroringEnabled(config)) {
    }

TString TPartitionScaleManager::LogPrefix() const {
    return TStringBuilder() << "[TPartitionScaleManager: " << TopicName << "] ";
}

void TPartitionScaleManager::HandleScaleStatusChange(const ui32 partitionId, NKikimrPQ::EScaleStatus scaleStatus, TMaybe<NKikimrPQ::TPartitionScaleParticipants> participants, const TActorContext& ctx) {
    if (scaleStatus == NKikimrPQ::EScaleStatus::NEED_SPLIT) {
        PQ_LOG_D("::HandleScaleStatusChange need to split partition " << partitionId);
        TPartitionScaleOperationInfo op{
            .PartitionId = partitionId,
            .PartitionScaleParticipants = std::move(participants),
        };
        PartitionsToSplit.insert_or_assign(partitionId, std::move(op));
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
    PQ_LOG_D("send split request");
    CurrentScaleRequest = ctx.Register(new TPartitionScaleRequest(
        TopicName,
        TopicPath,
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

struct TPartitionScaleManager::TBuildSplitScaleRequestResult {
    TMaybe<TPartitionSplit> Split;
    bool Remove = false;
};

std::vector<TPartitionScaleManager::TPartitionsToSplitMap::const_iterator> TPartitionScaleManager::ReorderSplits() const {
    // try to avoid gaps by using partitions with smaller children id
    auto proj = [](const auto& it) {
        const TPartitionScaleOperationInfo& info = it->second;
        const auto& childPartitionIds = info.PartitionScaleParticipants->GetChildPartitionIds();
        ui32 minChildPartitionId = childPartitionIds.empty() ? info.PartitionId : std::ranges::min(childPartitionIds);
        return std::make_tuple(minChildPartitionId, info.PartitionId);
    };
    std::vector<TPartitionScaleManager::TPartitionsToSplitMap::const_iterator> result;
    result.reserve(PartitionsToSplit.size());
    for (auto it = PartitionsToSplit.begin(); it != PartitionsToSplit.end(); ++it) {
        result.push_back(it);
    }
    std::ranges::sort(result, {}, proj);
    return result;
}

std::pair<std::vector<TPartitionSplit>, std::vector<TPartitionMerge>> TPartitionScaleManager::BuildScaleRequest(const TActorContext&) {
    std::vector<TPartitionSplit> splitsToApply;
    std::vector<TPartitionMerge> mergesToApply;

    const size_t allowedSplitsCountLimit = BalancerConfig.MaxActivePartitions > BalancerConfig.CurPartitions ? BalancerConfig.MaxActivePartitions - BalancerConfig.CurPartitions : 0;
    size_t allowedSplitsCount = allowedSplitsCountLimit;
    const std::vector splitCandidates = ReorderSplits();
    size_t checkedSplits = 0;
    for (const auto& partitionIt : splitCandidates) {
        if (allowedSplitsCount <= 0) {
            break;
        }
        ++checkedSplits;
        const auto& [_, splitParameters] = *partitionIt;
        TBuildSplitScaleRequestResult req = BuildSplitScaleRequest(splitParameters);
        if (req.Split) {
            splitsToApply.push_back(std::move(*req.Split));
            --allowedSplitsCount;
        }
        if (req.Remove) {
            PartitionsToSplit.erase(partitionIt);
        }
    }
    const size_t unprocessedSplits = splitCandidates.size() - checkedSplits;
    PQ_LOG_D(fmt::format("Scale request: #splits={}, #unprocessed={}, splitsLimit={}, #merges={}",
        splitsToApply.size(),
        unprocessedSplits,
        allowedSplitsCountLimit,
        mergesToApply.size()
    ));
    return {splitsToApply, mergesToApply};
}


TPartitionScaleManager::TBuildSplitScaleRequestResult TPartitionScaleManager::BuildSplitScaleRequest(const TPartitionScaleOperationInfo& splitParameters) const {
    const ui32 partitionId = splitParameters.PartitionId;
    if (MirroredFromSomewhere)  {
        if (!AppData()->FeatureFlags.GetEnableMirroredTopicSplitMerge()) {
            PQ_LOG_D("split request for mirrored topic is disabled. Partition# " << partitionId);
            return {.Split = Nothing(), .Remove = false};
        }
        if (!splitParameters.PartitionScaleParticipants.Defined()) {
            PQ_LOG_NOTICE("split request for mirrored topic doesn't have prescribed partition ids. Partition# " << partitionId);
            return {.Split = Nothing(), .Remove = true};
        }
    }
    const auto* node = PartitionGraph.GetPartition(partitionId);
    if (node == nullptr) {
        if (splitParameters.PartitionScaleParticipants.Defined()) {
            PQ_LOG_NOTICE("attempt to split partition that was not created yet. Partition# " << partitionId);
            return {.Split = Nothing(), .Remove = false};
        } else {
            PQ_LOG_ERROR("partition not found. Partition# " << partitionId);
            return {.Split = Nothing(), .Remove = true};
        }
    }
    if (node->DirectChildren.empty()) {
        auto from = node->From;
        auto to = node->To;
        auto mid = MiddleOf(from, to);
        if (mid.empty()) {
            PQ_LOG_ERROR("wrong partition key range. Can't get mid. Partition# " << partitionId);
            return {.Split = Nothing(), .Remove = true};
        }

        if (splitParameters.PartitionScaleParticipants.Defined() && splitParameters.PartitionScaleParticipants->AdjacentPartitionIdsSize() != 0) {
            PQ_LOG_ERROR("split request cannot have adjacent partitions. Partition# " << partitionId);
            return {.Split = Nothing(), .Remove = true};
        }

        PQ_LOG_D("partition split ranges. From# '" << ToHex(from)
                << "'. To# '" << ToHex(to) << "'. Mid# '" << ToHex(mid)
                << "'. Partition# " << partitionId);

        TPartitionSplit split;
        split.set_partition(partitionId);
        split.set_splitboundary(mid);
        if (splitParameters.PartitionScaleParticipants.Defined()) {
            for (const auto& childPartitionId : splitParameters.PartitionScaleParticipants->GetChildPartitionIds()) {
                split.add_childpartitionids(childPartitionId);
                if (const auto* childNode = PartitionGraph.GetPartition(childPartitionId); childNode != nullptr) {
                    PQ_LOG_NOTICE(fmt::format("Child partition# {} already exists. Performing unordered split. Partition# {}", childPartitionId, partitionId));
                    split.set_createrootlevelsibling(true);
                }
            }
        }
        return {.Split = std::move(split), .Remove = false};
    } else {
        if (splitParameters.PartitionScaleParticipants.Defined()) {
            const auto nodeChildrenIds = std::ranges::transform_view(node->DirectChildren, &TPartitionGraph::Node::Id);
            const auto& prescribedChildrenIds = splitParameters.PartitionScaleParticipants->GetChildPartitionIds();
            if (!std::ranges::is_permutation(nodeChildrenIds, prescribedChildrenIds)) {
                const std::string mappingStr = fmt::format("([{}]->[{}])", fmt::join(nodeChildrenIds, ","), fmt::join(prescribedChildrenIds, ","));
                PQ_LOG_ERROR("trying to split partition into different set of children partitions " << mappingStr << ". Partition# " << partitionId);
            }
        }
        return {.Split = Nothing(), .Remove = true};
    }
}

void TPartitionScaleManager::HandleScaleRequestResult(TPartitionScaleRequest::TEvPartitionScaleRequestDone::TPtr& ev, const TActorContext& ctx) {
    RequestInflight = false;
    LastResponseTime = ctx.Now();
    auto result = ev->Get();
    PQ_LOG_D("HandleScaleRequestResult scale request result: " << result->Status);
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
    MirroredFromSomewhere = MirroringEnabled(config);
}

void TPartitionScaleManager::UpdateDatabasePath(const TString& dbPath) {
    DatabasePath = dbPath;
}

} // namespace NPQ
} // namespace NKikimr
