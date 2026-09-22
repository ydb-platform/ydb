#pragma once

#include "blobstorage.h"

#include <util/generic/map.h>
#include <util/generic/vector.h>

#include <iterator>
#include <set>
#include <unordered_set>
#include <vector>

namespace NKikimr {

class THistoryCutter {
public:
    explicit THistoryCutter(const TIntrusiveConstPtr<TTabletStorageInfo> info)
        : Info(info)
        , ChannelStats(info->Channels.size())
    {}

    THistoryCutter(THistoryCutter&&) = default;

    void SeenBlob(const TLogoBlobID& blob) {
        if (blob.TabletID() != Info->TabletID) {
            return;
        }
        ui32 channel = blob.Channel();
        Y_ABORT_UNLESS(channel < ChannelStats.size());
        ChannelStats[channel].SeenGenerations.insert(blob.Generation());
    }

    std::vector<const TTabletChannelInfo::THistoryEntry*> GetHistoryToCut(ui32 channel) const {
        std::vector<const TTabletChannelInfo::THistoryEntry*> result;
        if (channel >= ChannelStats.size()) {
            return result;
        }
        if (!ChannelStats[channel].Certain) {
            return result;
        }
        const auto& history = Info->Channels[channel].History;
        if (history.size() < 2) {
            return result;
        }
        auto historyIt = history.begin();
        auto historyNext = std::next(historyIt);
        const auto& seen = ChannelStats[channel].SeenGenerations;
        auto seenIt = seen.begin();
        for (; historyNext != history.end(); ++historyIt, ++historyNext) {
            while (seenIt != seen.end() && *seenIt < historyIt->FromGeneration) {
                ++seenIt;
            }
            if (seenIt == seen.end() || *seenIt >= historyNext->FromGeneration) {
                result.push_back(&*historyIt);
            }
        }
        return result;
    }

    // returns map group -> collect generation
    TMap<ui32, ui32> GetHardBarriers(ui32 channel) const {
        TMap<ui32, ui32> hardBarriers;

        if (channel >= Info->Channels.size()) {
            return hardBarriers;
        }

        const auto historyToCut = GetHistoryToCut(channel);
        const auto& channelHistory = Info->Channels[channel].History;

        std::unordered_set<ui32> seenGroups;
        auto allHistoryIt = channelHistory.begin();
        // We can create a hard barrier if all the history entries referencing it
        // up to a certain generation are cut. As a barrier removes everything before it,
        // only one barrier per group is needed.
        for (const auto* historyEntry : historyToCut) {
            while (allHistoryIt != channelHistory.end() && allHistoryIt->FromGeneration < historyEntry->FromGeneration) {
                seenGroups.insert(allHistoryIt->GroupID);
                ++allHistoryIt;
            }
            if (!seenGroups.contains(historyEntry->GroupID)) {
                Y_ENSURE(historyEntry != &channelHistory.back());
                const auto nextFromGeneration = std::next(historyEntry)->FromGeneration;
                auto& collectGeneration = hardBarriers[historyEntry->GroupID];
                collectGeneration = Max(collectGeneration, nextFromGeneration - 1);
            }
            ++allHistoryIt;
        }
        return hardBarriers;
    }

    void BecomeUncertain(ui32 channel) {
        Y_ABORT_UNLESS(channel < ChannelStats.size());
        ChannelStats[channel].Certain = false;
    }

private:
    struct TChannelStat {
        std::set<ui32> SeenGenerations;
        bool Certain = true;
    };

    const TIntrusiveConstPtr<TTabletStorageInfo> Info;
    TVector<TChannelStat> ChannelStats;
};

} // namespace NKikimr

