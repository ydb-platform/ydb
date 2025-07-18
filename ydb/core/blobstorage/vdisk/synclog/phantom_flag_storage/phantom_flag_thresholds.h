#pragma once

#include <ydb/core/base/logoblob.h>
#include "phantom_flags.h"

#include <unordered_map>
#include <utility>
#include <vector>

namespace NKikimr {

namespace NSyncLog {
class TPhantomFlagThresholds {
public:
    TPhantomFlagThresholds(const TBlobStorageGroupType& gtype);

    void AddBlob(ui32 orderNumber, const TLogoBlobID& blob);
    void AddHardBarrier(ui32 orderNumber, ui64 tabletId, ui32 generation, ui32 step);
    bool IsBehindThreshold(const TLogoBlobID& blob) const;
    TPhantomFlags Sift(const TPhantomFlags& flags);

private:
    using TGenStep = std::pair<ui32, ui32>;

    // auxiliary classes
    class TNeighbourThresholds {
    public:
        void AddBlob(const TLogoBlobID& blob);
        void AddHardBarrier(ui64 tabletId, ui32 generation, ui32 step);
        bool IsBehindThreshold(const TLogoBlobID& blob) const;
    
    private:
        using TGenStep = std::pair<ui32, ui32>;
    
    private:
        class TTabletThresholds {
        public:
            void AddBlob(const TLogoBlobID& blob);
            void AddHardBarrier(ui32 generation, ui32 step);
            bool IsBehindThreshold(const TLogoBlobID& blob) const;
            bool IsEmpty() const;
    
        private:
            std::unordered_map<ui8, TGenStep> ChannelThresholds;
        };
    
    private:
        std::unordered_map<ui64, TTabletThresholds> TabletThresholds;
    };

private:
    TBlobStorageGroupType GType;
    std::vector<TNeighbourThresholds> NeighbourThresholds;
};

} // namespace NSyncLog

} // namespace NKikimr
