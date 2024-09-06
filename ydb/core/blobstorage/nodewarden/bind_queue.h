#pragma once

#include "defs.h"

namespace NKikimr::NStorage {

    class TBindQueue {
        struct TItem {
            ui32 NodeId;
            TMonotonic NextTryTimestamp = TMonotonic::Zero();

            TItem(ui32 nodeId)
                : NodeId(nodeId)
            {}
        };

        // BindQueue is arranged in the following way:
        // <active items> ActiveEnd <processed items> ProcessedEnd <disabled items> end()
        std::vector<TItem> BindQueue;
        size_t ActiveEnd = 0;
        size_t ProcessedEnd = 0;
        THashMap<ui32, size_t> NodeIdToBindQueue;

#ifndef NDEBUG
        THashSet<ui32> Enabled, Disabled;
#endif

    public:
        bool Empty() const {
            return BindQueue.empty();
        }

        void Disable(ui32 nodeId) {
#ifndef NDEBUG
            Y_ABORT_UNLESS(Enabled.contains(nodeId));
            Enabled.erase(nodeId);
            Disabled.insert(nodeId);
#endif

            const auto it = NodeIdToBindQueue.find(nodeId);
            Y_VERIFY_S(it != NodeIdToBindQueue.end(), "NodeId# " << nodeId);
            size_t index = it->second;

            // ensure item is not yet disabled
            Y_ABORT_UNLESS(index < ProcessedEnd);

            // if item is active, move it to processed as a transit stage
            if (index < ActiveEnd) {
                Swap(index, --ActiveEnd);
                index = ActiveEnd;
            }

            // move it to disabled
            Swap(index, --ProcessedEnd);
        }

        void Enable(ui32 nodeId) {
#ifndef NDEBUG
            Y_ABORT_UNLESS(Disabled.contains(nodeId));
            Disabled.erase(nodeId);
            Enabled.insert(nodeId);
#endif

            const auto it = NodeIdToBindQueue.find(nodeId);
            Y_ABORT_UNLESS(it != NodeIdToBindQueue.end());
            size_t index = it->second;

            // ensure item is disabled
            Y_ABORT_UNLESS(ProcessedEnd <= index);

            // move it back to processed and then to active
            Swap(index, ProcessedEnd++);
            Swap(ProcessedEnd - 1, ActiveEnd++);
        }

        std::optional<ui32> Pick(TMonotonic now, TMonotonic *closest) {
            // scan through processed items and find matching if there are no active items
            if (!ActiveEnd) {
                for (size_t k = 0; k < ProcessedEnd; ++k) {
                    if (BindQueue[k].NextTryTimestamp <= now) {
                        // make it active
                        Swap(k, ActiveEnd++);
                    }
                }
            }

            // pick a random item from Active set, if there is any
            if (ActiveEnd) {
                const size_t index = RandomNumber(ActiveEnd);
                const ui32 nodeId = BindQueue[index].NodeId;
                BindQueue[index].NextTryTimestamp = now + TDuration::Seconds(1);
                Swap(index, --ActiveEnd); // move item to processed
                return nodeId;
            } else {
                *closest = TMonotonic::Max();
                for (size_t k = ActiveEnd; k < ProcessedEnd; ++k) {
                    *closest = Min(*closest, BindQueue[k].NextTryTimestamp);
                }
                return std::nullopt;
            }
        }

        void Update(const std::vector<ui32>& nodeIds) {
            // remember node ids that are still pending
            THashSet<ui32> processedNodeIds;
            THashSet<ui32> disabledNodeIds;
            for (size_t k = 0; k < BindQueue.size(); ++k) {
                if (ActiveEnd <= k && k < ProcessedEnd) {
                    processedNodeIds.insert(BindQueue[k].NodeId);
                } else if (ProcessedEnd <= k) {
                    disabledNodeIds.insert(BindQueue[k].NodeId);
                }
            }

            // create a set of available node ids (excluding this one)
            THashSet<ui32> nodeIdsSet(nodeIds.begin(), nodeIds.end());

            // remove deleted nodes from the BindQueue and filter our existing ones in nodeIdsSet
            auto removePred = [&](const TItem& item) {
                if (nodeIdsSet.erase(item.NodeId)) {
                    // there is such item in nodeIdsSet, so keep it in BindQueue
                    return false;
                } else {
                    // item has vanished, remove it from BindQueue and NodeIdToBindQueue map
                    NodeIdToBindQueue.erase(item.NodeId);
#ifndef NDEBUG
                    Enabled.erase(item.NodeId);
                    Disabled.erase(item.NodeId);
#endif
                    return true;
                }
            };
            BindQueue.erase(std::remove_if(BindQueue.begin(), BindQueue.end(), removePred), BindQueue.end());
            for (const ui32 nodeId : nodeIdsSet) {
                BindQueue.emplace_back(nodeId);
#ifndef NDEBUG
                Enabled.insert(nodeId);
#endif
            }

            // move known disabled nodes to the end
            auto disabledPred = [&](const TItem& item) { return !disabledNodeIds.contains(item.NodeId); };
            ProcessedEnd = std::partition(BindQueue.begin(), BindQueue.end(), disabledPred) - BindQueue.begin();

            // rearrange active and processed nodes -- keep processed ones in place, new nodes are added as active
            auto processedPred = [&](const TItem& item) { return !processedNodeIds.contains(item.NodeId); };
            ActiveEnd = std::partition(BindQueue.begin(), BindQueue.begin() + ProcessedEnd, processedPred) - BindQueue.begin();

            // update revmap
            NodeIdToBindQueue.clear();
            for (size_t k = 0; k < BindQueue.size(); ++k) {
                NodeIdToBindQueue[BindQueue[k].NodeId] = k;
            }
        }

    private:
        void Swap(size_t x, size_t y) {
            if (x != y) {
                std::swap(BindQueue[x], BindQueue[y]);
                std::swap(NodeIdToBindQueue[BindQueue[x].NodeId], NodeIdToBindQueue[BindQueue[y].NodeId]);
            }
        }
    };

} // NKikimr::NStorage
