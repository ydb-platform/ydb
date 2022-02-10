#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>

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
