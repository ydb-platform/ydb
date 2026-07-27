#pragma once

#include "hull_ds_all_snap.h"

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TEvTakeHullSnapshot
    // Take snapshot of local (Hull) database
    ////////////////////////////////////////////////////////////////////////////
    struct TEvTakeHullSnapshot :
        public TEventLocal<TEvTakeHullSnapshot, TEvBlobStorage::EvTakeHullSnapshot>
    {
        const bool Index;

        TEvTakeHullSnapshot(bool index)
            : Index(index)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvTakeHullSnapshotResult
    ////////////////////////////////////////////////////////////////////////////
    struct TEvTakeHullSnapshotResult :
        public TEventLocal<TEvTakeHullSnapshotResult, TEvBlobStorage::EvTakeHullSnapshot>
    {
        THullDsSnap Snap;

        TEvTakeHullSnapshotResult(THullDsSnap &&snap)
            : Snap(std::move(snap))
        {}
    };

} // NKikimr
