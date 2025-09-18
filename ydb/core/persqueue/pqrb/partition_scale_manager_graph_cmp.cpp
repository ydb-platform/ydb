#include "partition_scale_manager_graph_cmp.h"

#include <format>

namespace NKikimr::NPQ::NMirror {

    static std::unordered_map<ui32, const NYdb::NTopic::TPartitionInfo*> BuildPartitionIndexMap(const std::span<const NYdb::NTopic::TPartitionInfo> source) {
        std::unordered_map<ui32, const NYdb::NTopic::TPartitionInfo*> result;
        for (const auto& partition : source) {
            result.emplace(partition.GetPartitionId(), &partition);
        }
        return result;
    }

    static std::optional<TRootPartitionsMismatch> CompareRootPartitionGraphs(const TPartitionGraph& target, const std::span<const NYdb::NTopic::TPartitionInfo> source) {
        std::vector<ui32> missingRootPartitionsIds;
        for (const auto& partition : source) {
            if (!partition.GetParentPartitionIds().empty()) {
                continue;
            }
            const ui32 id = partition.GetPartitionId();
            const TPartitionGraph::Node* node = target.GetPartition(id);
            if (node != nullptr) {
                // The node may refer root or non-root partition.
                // In the latter case the relative order of the messages may not be respected. But it is not an error.
                continue;
            }
            missingRootPartitionsIds.push_back(id);
        }
        if (missingRootPartitionsIds.empty()) [[likely]] {
            // The target topic may have more root partitions than the source.
            // That case will be handled later during the split event processing
            return std::nullopt;
        }

        std::ranges::sort(missingRootPartitionsIds);
        const ui32 firstMissingRootPartitionId = missingRootPartitionsIds.front();
        const ui32 lastMissingRootPartitionId = missingRootPartitionsIds.back();
        for (size_t i = 0; i < missingRootPartitionsIds.size(); ++i) {
            const ui32 expected = firstMissingRootPartitionId + i;
            if (expected != missingRootPartitionsIds[i]) {
                return TRootPartitionsMismatch{
                    .Error = std::format("Gap in the list of missing root partitions: #{} should precedes #{}", expected, missingRootPartitionsIds[i]),
                };
            }
        }
        for (ui32 id = 0; id < firstMissingRootPartitionId; ++id) {
            const auto* targetNode = target.GetPartition(id);
            if (targetNode == nullptr) {
                return TRootPartitionsMismatch{
                    .Error = std::format("Root partitions interleaved with non root ones. No info about partition #{} which precedes root #{}", id, firstMissingRootPartitionId),
                };
            };
            if (!targetNode->DirectChildren.empty()) {
                return TRootPartitionsMismatch{
                    .Error = std::format("Need to create new root partition #{}, but previous partition #{} already has children", firstMissingRootPartitionId, id),
                };
            }
        }

        const auto sourcePartitionIndexMap = BuildPartitionIndexMap(source);
        for (ui32 id = 0; id <= lastMissingRootPartitionId; ++id) {
            const auto sourceIt = sourcePartitionIndexMap.find(id);
            if (sourceIt == sourcePartitionIndexMap.end()) {
                return TRootPartitionsMismatch{
                    .Error = std::format("Missing partition #{} in the source topic description", id),
                };
            }
            AFL_ENSURE(sourceIt->second != nullptr)("d", "BuildPartitionIndexMap invariant failed")("id", id);
        }

        // Target topic has a set [0, ..., firstMissingRootPartitionId) of root partitions.
        // Construct new root partitions set [0, ..., lastMissingRootPartitionId]
        std::vector<TPartitionWithBounds> alterRootPartitions;
        alterRootPartitions.reserve(lastMissingRootPartitionId + 1);
        for (ui32 id = 0; id <= lastMissingRootPartitionId; ++id) {
            const NYdb::NTopic::TPartitionInfo& sourcePartitionInfo = *sourcePartitionIndexMap.at(id);
            const auto* targetNode = target.GetPartition(id);
            const bool newPart = targetNode == nullptr;

            TPartitionWithBounds part = TPartitionWithBounds{
                .Id = id,
                .Action = newPart ? EPartitionAction::Create : EPartitionAction::Modify,
                .FromBound = sourcePartitionInfo.GetFromBound(),
                .ToBound = sourcePartitionInfo.GetToBound(),
            };
            alterRootPartitions.push_back(std::move(part));
        }
        return TRootPartitionsMismatch{
            .AlterRootPartitions = std::move(alterRootPartitions),
        };
    }

    TMirrorGraphComparisonResult ComparePartitionGraphs(const TPartitionGraph& target, const std::span<const NYdb::NTopic::TPartitionInfo> source) {
        TMirrorGraphComparisonResult result;
        result.RootPartitionsMismatch = CompareRootPartitionGraphs(target, source);
        return result;
    }
} // namespace NKikimr::NPQ::NMirror
