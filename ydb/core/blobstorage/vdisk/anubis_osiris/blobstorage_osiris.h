#pragma once

#include "defs.h"
#include "blobstorage_anubis_osiris.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

namespace NKikimr {


    ////////////////////////////////////////////////////////////////////////////
    // TEvCallOsiris
    ////////////////////////////////////////////////////////////////////////////
    struct TEvCallOsiris : public TEventLocal<TEvCallOsiris, TEvBlobStorage::EvCallOsiris>
    {};

    ////////////////////////////////////////////////////////////////////////////
    // TEvOsirisDone
    ////////////////////////////////////////////////////////////////////////////
    struct TEvOsirisDone : public TEventLocal<TEvOsirisDone, TEvBlobStorage::EvOsirisDone>
    {
        const ui64 DbBirthLsn = 0;
        TEvOsirisDone(ui64 dbBirthLsn)
            : DbBirthLsn(dbBirthLsn)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////////
    // CreateHullOsiris
    // Osiris was an Egyptian god, usually identified as the god of the afterlife,
    // the underworld, and the dead, but more appropriately as the god of transition,
    // resurrection, and regeneration. [Wikipedia]
    // The purpose of Osiris is to analyze Hull database after lost of data and
    // decide which blobs must be resurrected and which one must be intered.
    // NOTE: after recovery of data there can be some blobs that VDisk confirmed to
    // the DS Proxy, but didn't sync with other VDisks, these blobs must be resurrected.
    ////////////////////////////////////////////////////////////////////////////////
    IActor *CreateHullOsiris(
            const TActorId &notifyId,
            const TActorId &parentId,
            const TActorId &skeletonId,
            THullDsSnap &&fullSnap,
            ui64 confirmedLsn,
            ui64 maxInFly);

} // NKikimr
