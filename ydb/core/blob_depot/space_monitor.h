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
        };

        std::unordered_map<ui32, TGroupRecord> Groups;

        friend class TBlobDepot;

    public:
        TSpaceMonitor(TBlobDepot *self);
        ~TSpaceMonitor();

        void Handle(TEvBlobStorage::TEvStatusResult::TPtr ev);
        void Kick();

    private:
        void Init();
    };

} // NKikimr::NBlobDepot
