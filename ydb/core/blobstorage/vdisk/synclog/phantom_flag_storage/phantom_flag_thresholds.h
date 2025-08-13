#pragma once

#include <ydb/core/base/logoblob.h>
#include "phantom_flags.h"

#include <unordered_map>
#include <utility>
#include <vector>

namespace NKikimr {

namespace NSyncLog {

////////////////////////////////////////////////////////////////////////////////////////////
// TPhantomFlagThresholds is a structure that contains last known Kept blob for each
// tablet-channel on this VDisk. This value serves as thresholds for PhantomFlagStorage
// to decide whether to store a new DoNotKeep flag for this channel or to omit it: we keep
// flag if there is known Kept blob with greater id and omit otherwise.
// TPhantomFlagThresholds is pruned when we recieve hard barrier
////////////////////////////////////////////////////////////////////////////////////////////

class TPhantomFlagThresholds {
public:
    TPhantomFlagThresholds(const TBlobStorageGroupType& gtype);

    void AddBlob(ui32 orderNumber, const TLogoBlobID& blob);
    void AddHardBarrier(ui32 orderNumber, ui64 tabletId, ui32 channel, ui32 generation, ui32 step);
    bool IsBehindThreshold(const TLogoBlobID& blob) const;
    TPhantomFlags Sift(const TPhantomFlags& flags);

private:
    using TGenStep = std::pair<ui32, ui32>;

    // auxiliary classes
    class TNeighbourThresholds {
    public:
        void AddBlob(const TLogoBlobID& blob);
        void AddHardBarrier(ui64 tabletId, ui8 channel, TGenStep barrier);
        bool IsBehindThreshold(const TLogoBlobID& blob) const;

    private:
        class TTabletThresholds {
        public:
            void AddBlob(const TLogoBlobID& blob);
            void AddHardBarrier(ui8 channel, TGenStep barrier);
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
