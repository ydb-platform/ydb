#pragma once

#include "defs.h"
#include "blobstorage_anubis_osiris.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

namespace NKikimr {

    enum class EOsirisMode {
        Recovery,
        Reinforcement,
    };

    constexpr TDuration OsirisRetryInterval = TDuration::Minutes(5);

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
        const NKikimrProto::EReplyStatus Status;
        TEvOsirisDone(ui64 dbBirthLsn, NKikimrProto::EReplyStatus status)
            : DbBirthLsn(dbBirthLsn)
            , Status(status)
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
            TIntrusivePtr<TBlobStorageGroupInfo> groupInfo,
            THullDsSnap &&fullSnap,
            ui64 confirmedLsn,
            ui64 maxInFly,
            EOsirisMode mode);

} // NKikimr
