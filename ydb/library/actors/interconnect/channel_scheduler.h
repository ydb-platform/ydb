#pragma once

#include "interconnect_channel.h"
#include "event_holder_pool.h"

#include <memory>

namespace NActors {

    class TChannelScheduler {
        const ui32 PeerNodeId;
        std::array<std::optional<TEventOutputChannel>, 16> ChannelArray;
        THashMap<ui16, TEventOutputChannel> ChannelMap;
        std::shared_ptr<IInterconnectMetrics> Metrics;
        const ui32 MaxSerializedEventSize;
        const TSessionParams Params;

        struct THeapItem {
            TEventOutputChannel *Channel;
            ui64 WeightConsumed = 0;

            friend bool operator <(const THeapItem& x, const THeapItem& y) {
                return x.WeightConsumed > y.WeightConsumed;
            }
        };

        std::vector<THeapItem> Heap;

    public:
        TChannelScheduler(ui32 peerNodeId, const TChannelsConfig& predefinedChannels,
                std::shared_ptr<IInterconnectMetrics> metrics, ui32 maxSerializedEventSize,
                TSessionParams params)
            : PeerNodeId(peerNodeId)
            , Metrics(std::move(metrics))
            , MaxSerializedEventSize(maxSerializedEventSize)
            , Params(std::move(params))
        {
            for (const auto& item : predefinedChannels) {
                GetOutputChannel(item.first);
            }
        }

        TEventOutputChannel *PickChannelWithLeastConsumedWeight() {
            Y_ABORT_UNLESS(!Heap.empty());
            return Heap.front().Channel;
        }

        void AddToHeap(TEventOutputChannel& channel, ui64 counter) {
            Y_DEBUG_ABORT_UNLESS(channel.IsWorking());
            ui64 weight = channel.WeightConsumedOnPause;
            weight -= Min(weight, counter - channel.EqualizeCounterOnPause);
            Heap.push_back(THeapItem{&channel, weight});
            std::push_heap(Heap.begin(), Heap.end());
        }

        void FinishPick(ui64 weightConsumed, ui64 counter) {
            std::pop_heap(Heap.begin(), Heap.end());
            auto& item = Heap.back();
            item.WeightConsumed += weightConsumed;
            if (item.Channel->IsWorking()) { // reschedule
                std::push_heap(Heap.begin(), Heap.end());
            } else { // remove from heap
                item.Channel->EqualizeCounterOnPause = counter;
                item.Channel->WeightConsumedOnPause = item.WeightConsumed;
                Heap.pop_back();
            }
        }

        TEventOutputChannel& GetOutputChannel(ui16 channel) {
            if (channel < ChannelArray.size()) {
                auto& res = ChannelArray[channel];
                if (Y_UNLIKELY(!res)) {
                    res.emplace(channel, PeerNodeId, MaxSerializedEventSize, Metrics,
                        Params);
                }
                return *res;
            } else {
                auto it = ChannelMap.find(channel);
                if (Y_UNLIKELY(it == ChannelMap.end())) {
                    it = ChannelMap.emplace(std::piecewise_construct, std::forward_as_tuple(channel),
                        std::forward_as_tuple(channel, PeerNodeId, MaxSerializedEventSize,
                        Metrics, Params)).first;
                }
                return it->second;
            }
        }

        ui64 Equalize() {
            if (Heap.empty()) {
                return 0; // nothing to do here -- no working channels
            }

            // find the minimum consumed weight among working channels and then adjust weights
            const ui64 min = Heap.front().WeightConsumed;
            for (THeapItem& item : Heap) {
                item.WeightConsumed -= min;
            }
            return min;
        }

        template<typename TCallback>
        void ForEach(TCallback&& callback) {
            for (auto& channel : ChannelArray) {
                if (channel) {
                    callback(*channel);
                }
            }
            for (auto& [id, channel] : ChannelMap) {
                callback(channel);
            }
        }
    };

} // NActors
