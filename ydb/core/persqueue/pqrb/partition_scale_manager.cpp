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
    , MirroredFromSomewhere(MirrorFromEnabled(config)) {
    }

TString TPartitionScaleManager::LogPrefix() const {
    return TStringBuilder() << "[TPartitionScaleManager: " << TopicName << "] ";
}

void TPartitionScaleManager::HandleScaleStatusChange(const ui32 partition, NKikimrPQ::EScaleStatus scaleStatus, TMaybe<NKikimrPQ::TPartitionScaleParticipants> participants, const TActorContext& ctx) {
    if (scaleStatus == NKikimrPQ::EScaleStatus::NEED_SPLIT) {
        PQ_LOG_D("TPartitionScaleManager::HandleScaleStatusChange need to split partition " << partition);
        TPartitionScaleOperationInfo op{
            .PartitionId = partition,
            .PartitionScaleParticipants = std::move(participants),
        };
        PartitionsToSplit.insert_or_assign(partition, std::move(op));
        TrySendScaleRequest(ctx);
    } else {
        PartitionsToSplit.erase(partition);
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
    PQ_LOG_D( "TPartitionScaleManager::HandleScaleStatusChange send split request");
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

std::pair<std::vector<TPartitionSplit>, std::vector<TPartitionMerge>> TPartitionScaleManager::BuildScaleRequest(const TActorContext&) {
    std::vector<TPartitionSplit> splitsToApply;
    std::vector<TPartitionMerge> mergesToApply;

    size_t allowedSplitsCount = BalancerConfig.MaxActivePartitions > BalancerConfig.CurPartitions ? BalancerConfig.MaxActivePartitions - BalancerConfig.CurPartitions : 0;
    auto partitionIt = PartitionsToSplit.begin();
    while (allowedSplitsCount > 0 && partitionIt != PartitionsToSplit.end()) {
        const auto& [_, splitParameters] = *partitionIt;
        TBuildSplitScaleRequestResult req = BuildSplitScaleRequest(splitParameters);
        if (req.Split) {
            splitsToApply.push_back(std::move(*req.Split));
            --allowedSplitsCount;
        }
        if (req.Remove) {
            partitionIt = PartitionsToSplit.erase(partitionIt);
        } else {
            ++partitionIt;
        }
    }

    return {splitsToApply, mergesToApply};
}


TPartitionScaleManager::TBuildSplitScaleRequestResult TPartitionScaleManager::BuildSplitScaleRequest(const TPartitionScaleOperationInfo& splitParameters) const {
    const ui32 partitionId = splitParameters.PartitionId;
    if (MirroredFromSomewhere)  {
        if (!splitParameters.PartitionScaleParticipants.Defined()) {
            PQ_LOG_NOTICE("TPartitionScaleManager::BuildScaleRequest split request for mirrored topic doesn't have prescribed partition ids. Topic# " << TopicName << ", partition# " << partitionId);
            return {.Split = Nothing(), .Remove = true};
        }
    }
    const auto* node = PartitionGraph.GetPartition(partitionId);
    if (node == nullptr) {
        if (splitParameters.PartitionScaleParticipants.Defined()) {
            PQ_LOG_NOTICE("TPartitionScaleManager::BuildScaleRequest attempt to split partition that was not created yet. Topic# " << TopicName << ", partition# " << partitionId);
            return {.Split = Nothing(), .Remove = false};
        } else {
            PQ_LOG_ERROR("TPartitionScaleManager::BuildScaleRequest partition not found. Topic# " << TopicName << ", partition# " << partitionId);
            return {.Split = Nothing(), .Remove = true};
        }
    }
    if (node->DirectChildren.empty()) {
        auto from = node->From;
        auto to = node->To;
        auto mid = MiddleOf(from, to);
        if (mid.empty()) {
            PQ_LOG_ERROR("TPartitionScaleManager::BuildScaleRequest wrong partition key range. Can't get mid. Topic# " << TopicName << ", partition# " << partitionId);
            return {.Split = Nothing(), .Remove = true};
        }

        if (splitParameters.PartitionScaleParticipants.Defined() && splitParameters.PartitionScaleParticipants->AdjacentPartitionIdsSize() != 0) {
            PQ_LOG_ERROR("TPartitionScaleManager::BuildScaleRequest split request cannot have adjacent partitions. Topic# " << TopicName << ", partition# " << partitionId);
            return {.Split = Nothing(), .Remove = true};
        }

        PQ_LOG_D("TPartitionScaleManager::BuildScaleRequest partition split ranges. From# '" << ToHex(from)
                << "'. To# '" << ToHex(to) << "'. Mid# '" << ToHex(mid)
                << "'. Topic# " << TopicName << ". Partition# " << partitionId);

        TPartitionSplit split;
        split.set_partition(partitionId);
        split.set_splitboundary(mid);
        if (splitParameters.PartitionScaleParticipants.Defined()) {
            for (const auto& childPartitionId : splitParameters.PartitionScaleParticipants->GetChildPartitionIds()) {
                split.add_childpartitionids(childPartitionId);
            }
        }
        return {.Split = std::move(split), .Remove = false};
    } else {
        if (splitParameters.PartitionScaleParticipants.Defined()) {
            const auto nodeChildrenIds = std::ranges::transform_view(node->DirectChildren, &TPartitionGraph::Node::Id);
            const auto& prescribedChildrenIds = splitParameters.PartitionScaleParticipants->GetChildPartitionIds();
            if (!std::ranges::is_permutation(nodeChildrenIds, prescribedChildrenIds)) {
                const std::string mappingStr = fmt::format("([{}]->[{}])", fmt::join(nodeChildrenIds, ","), fmt::join(prescribedChildrenIds, ","));
                PQ_LOG_ERROR("TPartitionScaleManager::BuildScaleRequest trying to split partition into different set of children partitions " << mappingStr << ". Topic# " << TopicName << ", partition# " << partitionId);
            }
        }
        return {.Split = Nothing(), .Remove = true};
    }
}

void TPartitionScaleManager::HandleScaleRequestResult(TPartitionScaleRequest::TEvPartitionScaleRequestDone::TPtr& ev, const TActorContext& ctx) {
    RequestInflight = false;
    LastResponseTime = ctx.Now();
    auto result = ev->Get();
    PQ_LOG_D("TPartitionScaleManager::HandleScaleRequestResult scale request result: " << result->Status << ". Topic# " << TopicName);
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
    MirroredFromSomewhere = MirrorFromEnabled(config);
}

void TPartitionScaleManager::UpdateDatabasePath(const TString& dbPath) {
    DatabasePath = dbPath;
}

} // namespace NPQ
} // namespace NKikimr
