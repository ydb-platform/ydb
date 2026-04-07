#pragma once

#include "defs.h"
#include "blobstorage_synclogformat.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/base/blobstorage_syncstate.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_lsnmngr.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_sst.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>

namespace NKikimr {
    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // TEvSyncLogPut -- put a record to synclog
        ////////////////////////////////////////////////////////////////////////////
        struct TEvSyncLogPut :
            public TEventLocal<TEvSyncLogPut, TEvBlobStorage::EvSyncLogPut>
        {
        public:
            TEvSyncLogPut(const TBlobStorageGroupType &gtype,
                          ui64 lsn,
                          const TLogoBlobID &id,
                          const TIngress &ingress) {
                SeqOfRecs.SetLogoBlob(gtype, lsn, id, ingress);
            }

            TEvSyncLogPut(ui64 lsn, ui64 tabletId, ui32 gen, ui64 issuerGuid) {
                SeqOfRecs.SetBlock(lsn, tabletId, gen, issuerGuid);
            }

            TEvSyncLogPut(const TBlobStorageGroupType &gtype,
                          ui64 lsn,
                          const NKikimrBlobStorage::TEvVCollectGarbage &record,
                          const TBarrierIngress &ingress) {
                SeqOfRecs.SetGC(gtype, lsn, 0, record, ingress);
            }

            TEvSyncLogPut(const TBlobStorageGroupType &gtype,
                          ui64 lsn,
                          const TDeque<TLogoBlobID>& phantoms) {
                SeqOfRecs.SetGC(gtype, lsn, phantoms);
            }

            const TSequenceOfRecs &GetRecs() const {
                return SeqOfRecs;
            }

            TString ToString() const {
                return SeqOfRecs.ToString();
            }

        private:
            TSequenceOfRecs SeqOfRecs;
        };

        ////////////////////////////////////////////////////////////////////////////
        // TEvSyncLogPutSst -- put a sst to synclog
        ////////////////////////////////////////////////////////////////////////////
        struct TEvSyncLogPutSst :
            public TEventLocal<TEvSyncLogPutSst, TEvBlobStorage::EvSyncLogPutSst>
        {
            using TLevelSegment = NKikimr::TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>;
            using TLevelSegmentPtr = TIntrusivePtr<TLevelSegment>;

            TEvSyncLogPutSst(TLevelSegmentPtr levelSegment)
                : LevelSegment(std::move(levelSegment))
            {}

            TLevelSegmentPtr LevelSegment;
        };

        ////////////////////////////////////////////////////////////////////////////
        // TEvSyncLogDbBirthLsn -- tell synclog about DbBirthLsn
        ////////////////////////////////////////////////////////////////////////////
        struct TEvSyncLogDbBirthLsn :
            public TEventLocal<TEvSyncLogDbBirthLsn, TEvBlobStorage::EvSyncLogDbBirthLsn>
        {
            ui64 DbBirthLsn = 0;

            TEvSyncLogDbBirthLsn(ui64 dbBirthLsn)
                : DbBirthLsn(dbBirthLsn)
            {}
        };

        ////////////////////////////////////////////////////////////////////////////
        // TEvSyncLogRead -- read from synclog (sync)
        ////////////////////////////////////////////////////////////////////////////
        struct TEvSyncLogRead :
            public TEventLocal<TEvSyncLogRead, TEvBlobStorage::EvSyncLogRead>
        {
            TSyncState SyncState;
            TVDiskID SourceVDiskID;
            TVDiskID TargetVDiskID;
            TActorId Recipient;
            const TInstant Now;
            ui32 Channel;

            TEvSyncLogRead(const TSyncState &syncState,
                           const TVDiskID &sourceVDisk,
                           const TVDiskID &targetVDisk,
                           const TActorId &recipient,
                           const TInstant &now,
                           ui32 channel)
                : SyncState(syncState)
                , SourceVDiskID(sourceVDisk)
                , TargetVDiskID(targetVDisk)
                , Recipient(recipient)
                , Now(now)
                , Channel(channel)
            {}
        };
    } // NSyncLog
} // NKikimr
