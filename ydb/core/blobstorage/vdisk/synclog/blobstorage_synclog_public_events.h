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


        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogFirstLsnToKeep
        // Global tracker for FirstLsnToKeep for SyncLog
        ////////////////////////////////////////////////////////////////////////////
        class TSyncLogFirstLsnToKeep {
        private:
            TAtomic Lsn = 0;
        public:
            TSyncLogFirstLsnToKeep() = default;

            void Set(ui64 lsn) {
                AtomicSet(Lsn, lsn);
            }

            ui64 Get() const {
                return AtomicGet(Lsn);
            }
        };

        ////////////////////////////////////////////////////////////////////////////
        // TSyncLogCtx
        ////////////////////////////////////////////////////////////////////////////
        class TSyncLogCtx : public TThrRefBase {
        public:
            const TIntrusivePtr<TVDiskContext> VCtx;
            const TIntrusivePtr<TLsnMngr> LsnMngr;
            const TPDiskCtxPtr PDiskCtx;
            const TActorId LoggerID;
            const TActorId LogCutterID;

            const ui64 SyncLogMaxDiskAmount;
            const ui64 SyncLogMaxEntryPointSize;
            const ui64 SyncLogMaxMemAmount;
            const ui32 MaxResponseSize;
            std::shared_ptr<TSyncLogFirstLsnToKeep> SyncLogFirstLsnToKeep;

            NMonGroup::TSyncLogIFaceGroup IFaceMonGroup;
            NMonGroup::TSyncLogCountersGroup CountersMonGroup;

            const bool IsReadOnlyVDisk;

            TSyncLogCtx(TIntrusivePtr<TVDiskContext> vctx,
                    TIntrusivePtr<TLsnMngr> lsnMngr,
                    TPDiskCtxPtr pdiskCtx,
                    const TActorId &loggerId,
                    const TActorId &logCutterId,
                    ui64 syncLogMaxDiskAmount,
                    ui64 syncLogMaxEntryPointSize,
                    ui64 syncLogMaxMemAmount,
                    ui32 maxResponseSize,
                    std::shared_ptr<TSyncLogFirstLsnToKeep> syncLogFirstLsnToKeep,
                    bool isReadOnlyVDisk)
                : VCtx(std::move(vctx))
                , LsnMngr(std::move(lsnMngr))
                , PDiskCtx(std::move(pdiskCtx))
                , LoggerID(loggerId)
                , LogCutterID(logCutterId)
                , SyncLogMaxDiskAmount(syncLogMaxDiskAmount)
                , SyncLogMaxEntryPointSize(syncLogMaxEntryPointSize)
                , SyncLogMaxMemAmount(syncLogMaxMemAmount)
                , MaxResponseSize(maxResponseSize)
                , SyncLogFirstLsnToKeep(std::move(syncLogFirstLsnToKeep))
                , IFaceMonGroup(VCtx->VDiskCounters, "subsystem", "synclog")
                , CountersMonGroup(VCtx->VDiskCounters, "subsystem", "synclogcounters")
                , IsReadOnlyVDisk(isReadOnlyVDisk)
            {}
        };

    } // NSyncLog
} // NKikimr
