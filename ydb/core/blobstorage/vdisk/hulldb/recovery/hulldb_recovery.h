#pragma once
#include <ydb/core/blobstorage/vdisk/hulldb/defs.h>

#include <ydb/core/blobstorage/vdisk/hulldb/barriers/hullds_cache_barrier.h>
#include <ydb/core/blobstorage/vdisk/hulldb/cache_block/cache_block.h>
#include <ydb/core/blobstorage/vdisk/protos/events.pb.h>
#include <ydb/core/blobstorage/vdisk/hulldb/bulksst_add/hulldb_bulksst_add.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // HULL UNCONDITIONAL (for local recovery)
    // All updates are taken unconditionally, all operations are synchronous
    ////////////////////////////////////////////////////////////////////////////
    class THullDbRecovery {
    public:
        enum EOpMode {
            RECOVERY,
            NORMAL
        };
        static const char *OpMode2Str(EOpMode mode);

        THullDbRecovery(TIntrusivePtr<THullCtx> hullCtx);
        THullDbRecovery(const THullDbRecovery &) = delete;
        THullDbRecovery(THullDbRecovery &&) = default;
        THullDbRecovery &operator=(const THullDbRecovery &) = delete;
        THullDbRecovery &operator=(THullDbRecovery &&) = default;

        ///////////////// LOGOBLOBS /////////////////////////////////////////////
        void ReplayAddLogoBlobCmd(
                const TActorContext &ctx,
                const TLogoBlobID &id,
                ui8 partId,
                const TIngress &ingress,
                TRope buffer,
                ui64 lsn,
                EOpMode mode);

        void ReplayAddLogoBlobCmd(
                const TActorContext &ctx,
                const TLogoBlobID &id,
                const TIngress &ingress,
                ui64 lsn,
                EOpMode mode);

        void ReplayAddHugeLogoBlobCmd(
                const TActorContext &ctx,
                const TLogoBlobID &id,
                const TIngress &ingress,
                const TDiskPart &diskAddr,
                ui64 lsn,
                EOpMode mode);

        ///////////////// SSTABLES //////////////////////////////////////////////
        void ReplayAddBulkSst(
                const TActorContext &ctx,
                const TAddBulkSstEssence &essence,
                const TLsnSeg &seg,
                EOpMode mode);

        ///////////////// BLOCKS ////////////////////////////////////////////////
        void ReplayAddBlockCmd(
                const TActorContext &ctx,
                ui64 tabletID,
                ui32 gen,
                ui64 issuerGuid,
                ui64 lsn,
                EOpMode mode);

        ///////////////// GC ////////////////////////////////////////////////////
        void ReplayAddBarrierCmd(
                const TActorContext &ctx,
                ui64 tabletID,
                ui32 channel,
                ui32 gen,
                ui32 genCounter,
                ui32 collectGen,
                ui32 collectStep,
                bool hard,
                const TBarrierIngress &ingress,
                ui64 lsn,
                EOpMode mode);

        void ReplayAddGCCmd(
                const TActorContext &ctx,
                const NKikimrBlobStorage::TEvVCollectGarbage &record,
                const TBarrierIngress &ingress,
                ui64 lsn);

        void ReplayAddGCCmd_BarrierSubcommand(
                const TActorContext &ctx,
                const NKikimrBlobStorage::TEvVCollectGarbage &record,
                const TBarrierIngress &ingress,
                ui64 lsn,
                EOpMode mode);

        void ReplayAddGCCmd_LogoBlobsSubcommand(
                const TActorContext &ctx,
                const NKikimrBlobStorage::TEvVCollectGarbage &record,
                ui64 lsn,
                EOpMode mode);

        void ReplaySyncDataCmd_LogoBlobsBatch(
                const TActorContext &ctx,
                std::shared_ptr<TFreshAppendixLogoBlobs> &&logoBlobs,
                TLsnSeg seg,
                EOpMode mode);

        void ReplaySyncDataCmd_BlocksBatch(
                const TActorContext &ctx,
                std::shared_ptr<TFreshAppendixBlocks> &&blocks,
                TLsnSeg seg,
                EOpMode mode);

        void ReplaySyncDataCmd_BarriersBatch(
                const TActorContext &ctx,
                std::shared_ptr<TFreshAppendixBarriers> &&barriers,
                TLsnSeg seg,
                EOpMode mode);

        void GetOwnedChunks(TSet<TChunkIdx>& chunks) const;
        void BuildBarrierCache();
        void BuildBlocksCache();
        TSatisfactionRank GetSatisfactionRank(EHullDbType t, ESatisfactionRankType s) const;
        void OutputHtmlForDb(IOutputStream &str) const;
        void OutputHtmlForHugeBlobDeleter(IOutputStream &str) const;

        void OutputProtoForDb(NKikimrVDisk::VDiskStat *proto) const;

        // FIXME: remove it when ready
        TIntrusivePtr<THullDs> GetHullDs() const {
            return HullDs;
        }

        TIntrusivePtr<THullCtx> GetHullCtx() const {
            return HullDs->HullCtx;
        }


    protected:
        TIntrusivePtr<THullDs> HullDs;
        TLogoBlobFilter Filter;
        TBlocksCache BlocksCache;
        TBarrierCache BarrierCache;

        // check on BLOCK incoming TEvVCollectGarbage message
        TBlocksCache::TBlockRes IsBlocked(const NKikimrBlobStorage::TEvVCollectGarbage &record) const;
        // function checks incoming garbage collection message for duplicates
        bool CheckGC(const TActorContext &ctx, const NKikimrBlobStorage::TEvVCollectGarbage &record);

    private:
        void UpdateBlocksCache(ui64 tabletId, ui32 gen, ui64 issuerGuid, ui64 lsn, EOpMode mode);
        void UpdateBlocksCache(const std::shared_ptr<TFreshAppendixBlocks> &blocks, TLsnSeg seg, EOpMode mode);
        void UpdateBarrierCache(const TKeyBarrier& key);
        void UpdateBarrierCache(const std::shared_ptr<TFreshAppendixBarriers>& barriers);
    };

} // NKikimr
