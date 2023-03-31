#pragma once

#include "defs.h"
#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TSpaceMonitor {
        TBlobDepot* const Self;

        struct TGroupRecord {
            bool StatusRequestInFlight = false;
            TStorageStatusFlags StatusFlags;
            float ApproximateFreeSpaceShare = 0.0f;
            std::vector<ui8> Channels;
        };

        std::unordered_map<ui32, TGroupRecord> Groups;
        NKikimrBlobStorage::TPDiskSpaceColor::E SpaceColor = {};
        float ApproximateFreeSpaceShare = 0.0f;

        friend class TBlobDepot;

    public:
        TSpaceMonitor(TBlobDepot *self);
        ~TSpaceMonitor();

        void Handle(TEvBlobStorage::TEvStatusResult::TPtr ev);
        void Kick();

        ui64 GetGroupAllocationWeight(ui32 groupId, bool stopOnLightYellow) const;
        void SetSpaceColor(NKikimrBlobStorage::TPDiskSpaceColor::E spaceColor, float approximateFreeSpaceShare);
        NKikimrBlobStorage::TPDiskSpaceColor::E GetSpaceColor() const;
        float GetApproximateFreeSpaceShare() const;

    private:
        void Init();
        void HandleYellowChannels();
    };

} // NKikimr::NBlobDepot
