#pragma once

#include "hull_ds_all_snap.h"

namespace NKikimr {

    struct TEvTakeHullSnapshot :
        public TEventLocal<TEvTakeHullSnapshot, TEvBlobStorage::EvTakeHullSnapshot>
    {
        const bool Index;

        explicit TEvTakeHullSnapshot(bool index)
            : Index(index)
        {}
    };

    struct TEvTakeHullSnapshotResult :
        public TEventLocal<TEvTakeHullSnapshotResult, TEvBlobStorage::EvTakeHullSnapshotResult>
    {
        THullDsSnap Snap;

        explicit TEvTakeHullSnapshotResult(THullDsSnap&& snap)
            : Snap(std::move(snap))
        {}
    };

} // namespace NKikimr
