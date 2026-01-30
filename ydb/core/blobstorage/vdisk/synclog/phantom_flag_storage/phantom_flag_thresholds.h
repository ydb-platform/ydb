#pragma once

#include <ydb/core/base/logoblob.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_context.h>
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
    void AddBlob(const TLogoBlobID& blob);
    void AddHardBarrier(ui32 orderNumber, ui64 tabletId, ui8 channel, ui32 generation, ui32 step);
    bool IsBehindThresholdOnUnsynced(const TLogoBlobID& blob, const TSyncedMask& syncedMask) const;
    TPhantomFlags Sift(const TPhantomFlags& flags, const TSyncedMask& syncedMask);
    ui64 EstimatedMemoryConsumption() const;
    void Merge(TPhantomFlagThresholds&& other);
    void Clear();

    TString ToString() const;

private:
    using TGenStep = std::pair<ui32, ui32>;
    static TGenStep MakeGenStep(const TLogoBlobID& blobId);

    using TTabletChannel = std::pair<ui64, ui8>;
    static TTabletChannel MakeTabletChannel(const TLogoBlobID& blobId);

    struct THasher {
        inline ui64 operator()(const TTabletChannel& x) const;
    };

private:
    // auxiliary classes
    class TTabletThresholds {
    public:
        TTabletThresholds();

        void AddBlob(ui32 orderNumber, TGenStep genStep);
        void AddBlob(TBlobStorageGroupType groupType, TGenStep genStep);
        // returns whether any blobs remain
        bool AddHardBarrier(ui32 orderNumber, TGenStep barrier);
        bool IsBehindThresholdOnUnsynced(TBlobStorageGroupType groupType, TGenStep genStep,
                const TSyncedMask& syncedMask) const;
        void Merge(TBlobStorageGroupType groupType, TTabletThresholds&& other);
        TString ToString(TBlobStorageGroupType groupType) const;

    private:
        TStackVec<std::optional<TGenStep>, MaxExpectedDisksInGroup> Thresholds;
        ui8 DisksWithThreshold = 0;
    };

private:
    TBlobStorageGroupType GType;
    std::unordered_map<TTabletChannel, TTabletThresholds, THasher> TabletThresholds;
};

} // namespace NSyncLog

} // namespace NKikimr
