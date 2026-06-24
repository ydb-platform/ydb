#include "partition_scale_manager.h"
#include "read_balancer_log.h"

#include <ydb/core/persqueue/public/partition_key_range/partition_key_range.h>
#include <fmt/format.h>
#include <algorithm>
#include <ranges>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::PERSQUEUE_READ_BALANCER

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

void TPartitionScaleManager::HandleScaleStatusChange(const ui32 partitionId, NKikimrPQ::EScaleStatus scaleStatus,
    TMaybe<NKikimrPQ::TPartitionScaleParticipants> participants,
    TMaybe<TString> splitBoundary,
    const TActorContext& ctx) {
    YDB_LOG_DEBUG("Handle HandleScaleStatusChange. Scale",
        {"logPrefix", LogPrefix()},
        {"status", NKikimrPQ::EScaleStatus_Name(scaleStatus)});
    if (scaleStatus == NKikimrPQ::EScaleStatus::NEED_SPLIT) {
        YDB_LOG_DEBUG("::HandleScaleStatusChange need to split partition",
            {"logPrefix", LogPrefix()},
            {"partitionId", partitionId});
        TPartitionScaleOperationInfo op{
            .PartitionId = partitionId,
            .PartitionScaleParticipants = std::move(participants),
            .SplitBoundary = std::move(splitBoundary)
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

    auto splitMergeRequest = BuildScaleRequest(ctx);
    if (splitMergeRequest.Empty()) {
        YDB_LOG_DEBUG("SplitMergeRequest empty",
            {"logPrefix", LogPrefix()});
        return;
    }

    RequestInflight = true;
    RootPartitionsResetRequestInflight = !splitMergeRequest.SetBoundary.empty();
    YDB_LOG_DEBUG("Send split request",
        {"logPrefix", LogPrefix()});
    CurrentScaleRequest = ctx.Register(new TPartitionScaleRequest(
        TopicName,
        TopicPath,
        DatabasePath,
        BalancerConfig.PathId,
        BalancerConfig.PathVersion,
        splitMergeRequest.Split,
        splitMergeRequest.Merge,
        splitMergeRequest.SetBoundary,
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
        if (info.PartitionScaleParticipants.Defined()) {
            const auto& childPartitionIds = info.PartitionScaleParticipants->GetChildPartitionIds();
            if (!childPartitionIds.empty()) {
                ui32 minChildPartitionId = std::ranges::min(childPartitionIds);
                return std::make_tuple(minChildPartitionId, info.PartitionId);
            }
        }
        return std::make_tuple(info.PartitionId, info.PartitionId);
    };
    std::vector<TPartitionScaleManager::TPartitionsToSplitMap::const_iterator> result;
    result.reserve(PartitionsToSplit.size());
    for (auto it = PartitionsToSplit.begin(); it != PartitionsToSplit.end(); ++it) {
        result.push_back(it);
    }
    std::ranges::sort(result, {}, proj);
    return result;
}

TPartitionScaleManager::TScaleRequest TPartitionScaleManager::BuildScaleRequest(const TActorContext&) {
    const size_t allowedSplitsCountLimit = BalancerConfig.MaxActivePartitions > BalancerConfig.CurPartitions ? BalancerConfig.MaxActivePartitions - BalancerConfig.CurPartitions : 0;
    size_t allowedSplitsCount = allowedSplitsCountLimit;

    auto boundsToApply = BuildSetBoundaryRequest(allowedSplitsCount);
    if (!boundsToApply.Empty()) {
        // Root partitions boundary reset is a rare operation.
        // We should not mix them with other split/merge operations, as they may depend on non-existent partitions.
        return {
            .SetBoundary = std::move(boundsToApply.Requests),
        };
    }

    auto mergesToApply = BuildMergeRequest(allowedSplitsCount);
    auto splitsToApply = BuildSplitRequest(allowedSplitsCount);

    YDB_LOG_DEBUG("Dump logPrefix, #_num_0",
        {"logPrefix", LogPrefix()},
        {"scaleRequest", fmt::format("Scale request: #splits={}, #unprocessed={}, splitsLimit={}, #merges={}",         splitsToApply.Requests.size(),         splitsToApply.Unprocessed,         allowedSplitsCountLimit,         mergesToApply.Requests.size()     )});

    return {
        .Split = std::move(splitsToApply.Requests),
        .Merge = std::move(mergesToApply.Requests),
        .SetBoundary = std::move(boundsToApply.Requests),
    };
}

TPartitionScaleManager::TRequests<TPartitionScaleManager::TPartitionBoundary> TPartitionScaleManager::BuildSetBoundaryRequest(size_t& allowedSplitsCount) {
    std::vector<TPartitionBoundary> boundsToApply;
    UpdateMirrorRootPartitionsSet();
    if (RootPartitionsToCreate.has_value() && !RootPartitionsToCreate->empty()) {
        size_t modifyPartitions = 0;
        size_t createPartitions = 0;
        for (const auto& p : RootPartitionsToCreate.value()) {
            TPartitionBoundary part;
            part.SetPartition(p.Id);
            if (p.FromBound.has_value()) {
                part.MutableKeyRange()->SetFromBound(TString{p.FromBound.value()});
            }
            if (p.ToBound.has_value()) {
                part.MutableKeyRange()->SetToBound(TString{p.ToBound.value()});
            }
            part.SetCreatePartition(p.Action == NMirror::EPartitionAction::Create);
            size_t cost = (p.Action == NMirror::EPartitionAction::Create) ? 1 : 0;
            modifyPartitions += (p.Action == NMirror::EPartitionAction::Modify) ? 1 : 0;
            createPartitions += (p.Action == NMirror::EPartitionAction::Create) ? 1 : 0;
            if (allowedSplitsCount >= cost) {
                allowedSplitsCount -= cost;
                boundsToApply.push_back(std::move(part));
            } else {
                YDB_LOG_WARN("",
                    {"logPrefix", LogPrefix()},
                    {"warning", fmt::format("MaxActivePartitions ({}) is too low to recreate {} root partitions from the mirror source topic",                                      BalancerConfig.MaxActivePartitions,                                      RootPartitionsToCreate->size())});
                // don't send request at all, if there is not enough quota
                return {
                    .Unprocessed = RootPartitionsToCreate->size(),
                };
            }
        }
        YDB_LOG_DEBUG("Dump logPrefix, #_num_0",
            {"logPrefix", LogPrefix()},
            {"partitionBoundariesRequest", fmt::format("Set partition boundaries requsts: #modify={}, #create{}",             modifyPartitions,             createPartitions         )});
    }
    return {
        .Requests = std::move(boundsToApply),
        .Unprocessed = 0,
    };
}

TPartitionScaleManager::TRequests<TPartitionScaleManager::TPartitionSplit> TPartitionScaleManager::BuildSplitRequest(size_t& allowedSplitsCount) {
    std::vector<TPartitionSplit> splitsToApply;
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
    return {
        .Requests = std::move(splitsToApply),
        .Unprocessed = splitCandidates.size() - checkedSplits,
    };
}

TPartitionScaleManager::TRequests<TPartitionScaleManager::TPartitionMerge> TPartitionScaleManager::BuildMergeRequest(size_t& allowedSplitsCount) {
    Y_UNUSED(allowedSplitsCount);
    return {};
}


TPartitionScaleManager::TBuildSplitScaleRequestResult TPartitionScaleManager::BuildSplitScaleRequest(const TPartitionScaleOperationInfo& splitParameters) const {
    const ui32 partitionId = splitParameters.PartitionId;
    if (MirroredFromSomewhere)  {
        if (!AppData()->FeatureFlags.GetEnableMirroredTopicSplitMerge()) {
            YDB_LOG_DEBUG("Split request for mirrored topic is disabled",
                {"logPrefix", LogPrefix()},
                {"partition", partitionId});
            return {.Split = Nothing(), .Remove = false};
        }
        if (!splitParameters.PartitionScaleParticipants.Defined()) {
            YDB_LOG_NOTICE("Split request for mirrored topic doesn't have prescribed partition ids",
                {"logPrefix", LogPrefix()},
                {"partition", partitionId});
            return {.Split = Nothing(), .Remove = true};
        }
    }
    const auto* node = PartitionGraph.GetPartition(partitionId);
    if (node == nullptr) {
        if (splitParameters.PartitionScaleParticipants.Defined()) {
            YDB_LOG_NOTICE("Attempt to split partition that was not created yet",
                {"logPrefix", LogPrefix()},
                {"partition", partitionId});
            return {.Split = Nothing(), .Remove = false};
        } else {
            YDB_LOG_ERROR("Partition not found",
                {"logPrefix", LogPrefix()},
                {"partition", partitionId});
            return {.Split = Nothing(), .Remove = true};
        }
    }
    if (node->DirectChildren.empty()) {
        auto from = node->From;
        auto to = node->To;
        auto mid = splitParameters.SplitBoundary.GetOrElse(MiddleOf(from, to));
        if (mid.empty()) {
            YDB_LOG_ERROR("Wrong partition key range. Can't get mid",
                {"logPrefix", LogPrefix()},
                {"partition", partitionId});
            return {.Split = Nothing(), .Remove = true};
        }

        if (splitParameters.PartitionScaleParticipants.Defined() && splitParameters.PartitionScaleParticipants->AdjacentPartitionIdsSize() != 0) {
            YDB_LOG_ERROR("Split request cannot have adjacent partitions",
                {"logPrefix", LogPrefix()},
                {"partition", partitionId});
            return {.Split = Nothing(), .Remove = true};
        }

        YDB_LOG_DEBUG("Partition split ranges. Mid#",
            {"logPrefix", LogPrefix()},
            {"fromHex", ToHex(from)},
            {"toHex", ToHex(to)},
            {"midHex", ToHex(mid)},
            {"partition", partitionId},
            {"from", "To#"});

        TPartitionSplit split;
        split.set_partition(partitionId);
        split.set_splitboundary(mid);
        if (splitParameters.PartitionScaleParticipants.Defined()) {
            for (const auto& childPartitionId : splitParameters.PartitionScaleParticipants->GetChildPartitionIds()) {
                split.add_childpartitionids(childPartitionId);
                if (const auto* childNode = PartitionGraph.GetPartition(childPartitionId); childNode != nullptr) {
                    YDB_LOG_NOTICE("",
                        {"logPrefix", LogPrefix()},
                        {"notice", fmt::format("Child partition# {} already exists. Performing unordered split. Partition# {}", childPartitionId, partitionId)});
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
                YDB_LOG_ERROR("Trying to split partition into different set of children partitions",
                    {"logPrefix", LogPrefix()},
                    {"mappingStr", mappingStr},
                    {"partition", partitionId});
            }
        }
        return {.Split = Nothing(), .Remove = true};
    }
}

void TPartitionScaleManager::HandleScaleRequestResult(TPartitionScaleRequest::TEvPartitionScaleRequestDone::TPtr& ev, const TActorContext& ctx) {
    RequestInflight = false;
    LastResponseTime = ctx.Now();
    auto result = ev->Get();
    YDB_LOG_DEBUG("HandleScaleRequestResult scale request",
        {"logPrefix", LogPrefix()},
        {"result", result->Status});
    if (result->Status == TEvTxUserProxy::TResultStatus::ExecComplete) {
        RequestTimeout = TDuration::Zero();
        Backoff.Reset();
        TrySendScaleRequest(ctx);
    } else {
        RequestTimeout = Backoff.Next();
        ctx.Schedule(RequestTimeout, new TEvents::TEvWakeup(TRY_SCALE_REQUEST_WAKE_UP_TAG));
    }
}

void TPartitionScaleManager::ClearMirrorInfo() {
    MirrorTopicDescription.reset();
    RootPartitionsToCreate.reset();
    MirrorTopicError.reset();
}

void TPartitionScaleManager::UpdateMirrorRootPartitionsSet() {
    if (!MirrorTopicDescription.has_value() || !MirroredFromSomewhere) {
        ClearMirrorInfo();
        return;
    }

    NMirror::TMirrorGraphComparisonResult cmp = NMirror::ComparePartitionGraphs(PartitionGraph, MirrorTopicDescription->GetPartitions());
    if (!cmp.RootPartitionsMismatch.has_value()) {
        YDB_LOG_DEBUG("Topic has all root partitions from the source topic",
            {"logPrefix", LogPrefix()});
        RootPartitionsToCreate.reset();
        MirrorTopicError.reset();
        return;
    }
    auto& rootPartitionsMismatch = cmp.RootPartitionsMismatch.value();
    if (rootPartitionsMismatch.Error.has_value()) {
        std::string msg = TStringBuilder() << "Incompatable configuration of root partitions between source and target topics:" << rootPartitionsMismatch.Error.value();
        YDB_LOG_ERROR("",
            {"logPrefix", LogPrefix()},
            {"msg", msg});
        RootPartitionsToCreate.reset();
        MirrorTopicError = std::move(*rootPartitionsMismatch.Error);
        return;
    }
    const size_t existingPartitions = std::ranges::count(rootPartitionsMismatch.AlterRootPartitions, NMirror::EPartitionAction::Modify, &NMirror::TPartitionWithBounds::Action);
    const size_t newPartitions = std::ranges::count(rootPartitionsMismatch.AlterRootPartitions, NMirror::EPartitionAction::Create, &NMirror::TPartitionWithBounds::Action);
    YDB_LOG_INFO("",
        {"logPrefix", LogPrefix()},
        {"partitionConfiguration", fmt::format("Topic has less root partitions than the mirror source. New configuration has {}+{} partitions.",                          existingPartitions,                          newPartitions)});

    RootPartitionsToCreate = std::move(rootPartitionsMismatch.AlterRootPartitions);
    MirrorTopicError.reset();
}

std::expected<void, std::string> TPartitionScaleManager::HandleMirrorTopicDescriptionResult(TEvPQ::TEvMirrorTopicDescription::TPtr& ev, const TActorContext& ctx) {
    if (!MirroredFromSomewhere) {
        ClearMirrorInfo();
    } else {
        auto& description = ev->Get()->Description;
        if (!description.has_value() || !description.value().IsSuccess()) {
            YDB_LOG_WARN("Ignoring invalid mirror source description",
                {"logPrefix", LogPrefix()});
            return {};
        }
        MirrorTopicDescription.emplace(std::move(description->GetTopicDescription()));
    }
    UpdateMirrorRootPartitionsSet();

    if (MirrorTopicError.has_value()) {
        return std::unexpected(MirrorTopicError.value());
    }

    TrySendScaleRequest(ctx);
    return {};
}

void TPartitionScaleManager::Die(const TActorContext& ctx) {
    if (CurrentScaleRequest) {
        ctx.Send(CurrentScaleRequest, new TEvents::TEvPoisonPill());
    }
}

void TPartitionScaleManager::UpdateBalancerConfig(ui64 pathId, int version, const NKikimrPQ::TPQTabletConfig& config) {
    BalancerConfig = TBalancerConfig(pathId, version, config);
    MirroredFromSomewhere = MirroringEnabled(config);
    if (!MirroredFromSomewhere) {
        ClearMirrorInfo();
    }
}

void TPartitionScaleManager::UpdateDatabasePath(const TString& dbPath) {
    DatabasePath = dbPath;
}

} // namespace NPQ
} // namespace NKikimr
