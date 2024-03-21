#pragma once
#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_hulllogctx.h>
#include <ydb/core/blobstorage/vdisk/hulldb/cache_block/cache_block.h>
#include <ydb/core/blobstorage/vdisk/hulldb/recovery/hulldb_recovery.h>
#include <ydb/core/blobstorage/vdisk/hulldb/bulksst_add/hulldb_bulksst_add.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_public_events.h>

namespace NKikimr {

    class TLsnMngr;
    class TPDiskCtx;
    struct TEvLocalStatusResult;
    using TPDiskCtxPtr = std::shared_ptr<TPDiskCtx>;

    ////////////////////////////////////////////////////////////////////////////
    // THullCheckStatus
    ////////////////////////////////////////////////////////////////////////////
    // Check* methods returns EReplyStatus and boolean flag can we reply now or shoud postpone
    // reply until commit to the log. It is used for BLOCKED status;
    struct THullCheckStatus {
        NKikimrProto::EReplyStatus Status;
        TString ErrorReason;
        ui64 Lsn;
        bool Postponed;

        THullCheckStatus(NKikimrProto::EReplyStatus status, TString errorReason, ui64 lsn = 0, bool postponed = false)
            : Status(status)
            , ErrorReason(std::move(errorReason))
            , Lsn(lsn)
            , Postponed(postponed)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // HULL
    ////////////////////////////////////////////////////////////////////////////
    class THull : public THullDbRecovery {
    private:
        struct TFields;
        std::unique_ptr<TFields> Fields;

        void ValidateWriteQuery(const TActorContext &ctx, const TLogoBlobID &id, bool *writtenBeyondBarrier);

        // validate GC barrier command against existing barriers metabase (ensure that keys are
        // coming in ascending order, CollectGen/CollectStep pairs do not decrease and that keys
        // do not repeat)
        NKikimrProto::EReplyStatus ValidateGCCmd(
                ui64 tabletID,
                ui32 channel,
                bool hard,
                ui32 recordGeneration,
                ui32 perGenCounter,
                ui32 collectGeneration,
                ui32 collectStep,
                const TBarrierIngress& ingress,
                const TActorContext& ctx);

    public:
        THull(
            TIntrusivePtr<TLsnMngr> lsnMngr,
            TPDiskCtxPtr pdiskCtx,
            const TActorId skeletonId,
            bool runHandoff,
            THullDbRecovery &&uncond,
            TActorSystem *as,
            bool barrierValidation);
        THull(const THull &) = delete;
        THull(THull &&) = default;
        THull &operator =(const THull &) = delete;
        THull &operator =(THull &&) = default;
        ~THull();

        // Run all required hull facilities, like actors that perform compactions, etc
        TActiveActors RunHullServices(
                TIntrusivePtr<TVDiskConfig> config,
                std::shared_ptr<THullLogCtx> hullLogCtx,
                std::shared_ptr<NSyncLog::TSyncLogFirstLsnToKeep> syncLogFirstLsnToKeep,
                TActorId loggerId,
                TActorId logCutterId,
                const TActorContext &ctx);

        // Request from PDisk to cut the recovery log
        void CutRecoveryLog(const TActorContext &ctx, std::unique_ptr<NPDisk::TEvCutLog> msg);

        void PostponeReplyUntilCommitted(IEventBase *msg, const TActorId &recipient, ui64 recipientCookie,
            NWilson::TTraceId traceId, ui64 lsn);

        ////////////////////////////////////////////////////////////////////////
        // LogoBlobs
        ////////////////////////////////////////////////////////////////////////
        THullCheckStatus CheckLogoBlob(
                const TActorContext &ctx,
                const TLogoBlobID &id,
                bool ignoreBlock,
                const NProtoBuf::RepeatedPtrField<NKikimrBlobStorage::TEvVPut::TExtraBlockCheck>& extraBlockChecks,
                bool *writtenBeyondBarrier);

        void AddLogoBlob(
                const TActorContext &ctx,
                const TLogoBlobID &id,
                ui8 partId,
                const TIngress &ingress,
                TRope buffer,
                ui64 lsn);

        void AddHugeLogoBlob(
                const TActorContext &ctx,
                const TLogoBlobID &id,
                const TIngress &ingress,
                const TDiskPart &diskAddr,
                ui64 lsn);

        void AddLogoBlob(
                const TActorContext &ctx,
                const TLogoBlobID &id,
                const TIngress &ingress,
                const TLsnSeg &seg);


        ////////////////////////////////////////////////////////////////////////
        // Add already contructed and COMMITTED SSTables
        ////////////////////////////////////////////////////////////////////////
        void AddBulkSst(
                const TActorContext &ctx,
                const TAddBulkSstEssence &essence,
                const TLsnSeg &seg);

        ///////////////// COMPLETE TABLE DELETION ///////////////////////////////
        // Complete table deletion is implemented as 2 commands:
        // 1. Set BLOCK with gen=Max<ui32>()
        // 2. Set BARRIER (i.e. GarbageCollect) with collectGeneration=Max<ui32>() and
        //    collectStep=Max<ui32>(). For this command perGenCounter must also be
        //    set to Max<ui32>()

        ////////////////////////////////////////////////////////////////////////
        // Blocks
        ////////////////////////////////////////////////////////////////////////
        using TReplySender = std::function<void (const TActorId &, ui64, NWilson::TTraceId, IEventBase *)>;

        THullCheckStatus CheckBlockCmdAndAllocLsn(
                ui64 tabletID,
                ui32 gen,
                ui64 issuerGuid,
                ui32 *actGen,
                TLsnSeg *seg);

        void AddBlockCmd(
                const TActorContext &ctx,
                ui64 tabletID,
                ui32 gen,
                ui64 issuerGuid,
                ui64 lsn,
                const TReplySender &replySender);

        bool GetBlocked(ui64 tabletID, ui32 *outGen) {
            return BlocksCache.Find(tabletID, outGen);
        }

        TBlocksCache::TBlockRes IsBlocked(ui64 tabletID, TBlocksCache::TBlockedGen tabletGeneration) {
            return BlocksCache.IsBlocked(tabletID, tabletGeneration);
        }

        ////////////////////////////////////////////////////////////////////////
        // GC
        ////////////////////////////////////////////////////////////////////////
        THullCheckStatus CheckGCCmdAndAllocLsn(
                const TActorContext &ctx,
                const NKikimrBlobStorage::TEvVCollectGarbage &record,
                const TBarrierIngress &ingress,
                TLsnSeg *seg);

        TLsnSeg AllocateLsnForGCCmd(const NKikimrBlobStorage::TEvVCollectGarbage &record);

        void AddGCCmd(
                const TActorContext &ctx,
                const NKikimrBlobStorage::TEvVCollectGarbage &record,
                const TBarrierIngress &ingress,
                const TLsnSeg &seg);

        TLsnSeg AllocateLsnForPhantoms(const TDeque<TLogoBlobID>& phantoms);
        void CollectPhantoms(const TActorContext& ctx, const TDeque<TLogoBlobID>& phantoms, const TLsnSeg &seg);

        // fast check for LogoBlob
        bool FastKeep(const TLogoBlobID& id, bool keepByIngress, TString *explanation = nullptr) const;

        ///////////////// SYNC //////////////////////////////////////////////////////
        TLsnSeg AllocateLsnForSyncDataCmd(const TString &data);
        TLsnSeg AllocateLsnForSyncDataCmd(const TFreshBatch &freshBatch);
        void AddSyncDataCmd(
                const TActorContext &ctx,
                const TString &data,
                const TLsnSeg &seg,
                const TReplySender &replySender);
        void AddSyncDataCmd(
                const TActorContext &ctx,
                TFreshBatch &&freshBatch,
                const TLsnSeg &seg,
                const TReplySender &replySender);

        ///////////////// STATUS REQUEST ////////////////////////////////////////////
        void StatusRequest(const TActorContext &ctx, TEvLocalStatusResult *result);

        ///////////////// GET SNAPSHOT //////////////////////////////////////////////
        THullDsSnap GetSnapshot() const;
        THullDsSnap GetIndexSnapshot() const;

        bool HasBlockRecordFor(ui64 tabletId) const {
            return BlocksCache.HasRecord(tabletId);
        }

        void PermitGarbageCollection(const TActorContext& ctx);
    };

    // FIXME:
    // FIXME: check that we store only logoblobs with PartId==0
    // FIXME:

    // FIXME:
    // FIXME: Query optimization (blobstorage_query.cpp has better description)
    // FIXME:

    // FIXME: implement windows for sync/fullsync/repl

    // FIXME: On a single range read request send several messages to bs proxy
    //
    // FIXME: garbage collection for Keep flag and none-synced data
    // if (KeepFlag && Barrier > LogoBlob && !QuorumForLogoBlob)
    //     erase;
    // FIXME: we can't collect garbage locally after start until sync with other nodes. This is because during
    //        running sync we update local database, sometimes inconsistently according to garbage collection
    //        processes

    // FIXME: optimize Blocks and Barriers database: if it has a small size, store snapshot in a log
    // Hint: jush have a 0 levels for compaction, if state is too large (megabytes)

    // FIXME: optimize data storing: we don't need to save FullDataSize in TDiskBlob

} // NKikimr
