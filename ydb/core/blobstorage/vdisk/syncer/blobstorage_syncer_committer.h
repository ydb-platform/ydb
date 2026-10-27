#pragma once

#include "defs.h"
#include "syncer_context.h"
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/base/blobstorage_syncstate.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_vdisk_guids.h>
#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>

namespace NKikimr {

    namespace NSyncer {
        struct TPeerSyncState;
    } // NSyncer

    ////////////////////////////////////////////////////////////////////////
    // TEvSyncerCommit
    ////////////////////////////////////////////////////////////////////////
    struct TEvSyncerCommit :
        public TEventLocal<TEvSyncerCommit, TEvBlobStorage::EvSyncerCommit>
    {

        using ELocalState = NKikimrBlobStorage::TLocalGuidInfo::EState;
        using TLocalVal = NKikimrBlobStorage::TLocalGuidInfo;
        using ESyncState = NKikimrBlobStorage::TSyncGuidInfo::EState;
        using TSyncVal = NKikimrBlobStorage::TSyncGuidInfo;

        enum EModif {
            ENone,
            ELocalGuid,
            EVDiskEntry
        };

        // what we modify local state or info about other VDisk
        EModif Modif = ENone;
        // locally saved State
        ELocalState State = TLocalVal::Empty;
        // locally saved Guid
        TVDiskEternalGuid Guid;
        // if we modify local info this field is set
        NKikimrBlobStorage::TLocalGuidInfo LocalGuidInfo;
        // if we modify info about remote VDisk this field is set
        NKikimrVDiskData::TSyncerVDiskEntry VDiskEntry;
        // in case of Modif=EVDiskEntry we modify info about this VDisk
        TVDiskID VDiskId;
        // just cookie
        const void *Cookie = nullptr;

        TEvSyncerCommit();

        // create commit msg for local state (not Final)
        static std::unique_ptr<TEvSyncerCommit> Local(ELocalState state, TVDiskEternalGuid guid);
        // create commit msg for local state (Final)
        static std::unique_ptr<TEvSyncerCommit> LocalFinal(TVDiskEternalGuid guid, ui64 dbBirthLsn);
        // create commit msg for remote vdisk (save sync status, initiated locally)
        static std::unique_ptr<TEvSyncerCommit> Remote(const TVDiskID &vdisk,
                                               const NSyncer::TPeerSyncState &p);
        // create commit msg for remote vdisk (save eternal guid, initiated by remote VDisk)
        static std::unique_ptr<TEvSyncerCommit> Remote(const TVDiskID &vdisk,
                                               ESyncState state,
                                               TVDiskEternalGuid guid,
                                               void *cookie);
    };

    ////////////////////////////////////////////////////////////////////////
    // TEvSyncerCommitDone
    ////////////////////////////////////////////////////////////////////////
    struct TEvSyncerCommitDone :
        public TEventLocal<TEvSyncerCommitDone, TEvBlobStorage::EvSyncerCommitDone>
    {
        const void *Cookie = nullptr;
        TEvSyncerCommitDone(const void *cookie)
            : Cookie(cookie)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // VDISK SYNCER COMMITTER
    // Commits of syncer state go through this actor
    ////////////////////////////////////////////////////////////////////////////
    class TSyncerDataSerializer;
    IActor *CreateSyncerCommitter(const TIntrusivePtr<TSyncerContext> &sc,
                                  TSyncerDataSerializer &&sds);

} // NKikimr
