#pragma once

#include "blobstorage.h"

#include <util/generic/vector.h>

#include <set>
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

