#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all.h>

namespace NKikimr {

    struct THullLogCtx;
    namespace NSyncLog {
        class TSyncLogFirstLsnToKeep;
    }

    //////////////////////////////////////////////////////////////////////////
    // TLevelIndexRunTimeCtx -- common data between THull and corresponding
    // TLevelIndexActor
    //////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TLevelIndexRunTimeCtx {
    public:
        TIntrusivePtr<TLsnMngr> LsnMngr;
        TPDiskCtxPtr PDiskCtx;
        const TActorId SkeletonId;
        const bool RunHandoff;
        const TIntrusivePtr<TLevelIndex<TKey, TMemRec>> LevelIndex;
        ui64 FreeUpToLsn = 0;
    private:
        // ActorId of the LogCutterNotifier, that aggregates cut log lsn for the Hull database
        TActorId LogNotifierActorId;

    public:
        TLevelIndexRunTimeCtx(TIntrusivePtr<TLsnMngr> lsnMngr,
                TPDiskCtxPtr pdiskCtx,
                const TActorId skeletonId,
                bool runHandoff,
                TIntrusivePtr<TLevelIndex<TKey, TMemRec>> levelIndex)
            : LsnMngr(std::move(lsnMngr))
            , PDiskCtx(std::move(pdiskCtx))
            , SkeletonId(skeletonId)
            , RunHandoff(runHandoff)
            , LevelIndex(std::move(levelIndex))
        {
            Y_ABORT_UNLESS(LsnMngr && PDiskCtx && LevelIndex);
        }

        void CutRecoveryLog(const TActorContext &ctx, std::unique_ptr<NPDisk::TEvCutLog> msg) {
            if (LevelIndex)
                ctx.Send(LevelIndex->LIActor, msg.release());
        }

        void SetFreeUpToLsn(ui64 freeUpToLsn) {
            FreeUpToLsn = freeUpToLsn;
        }

        ui64 GetFreeUpToLsn() const {
            return FreeUpToLsn;
        }

        void SetLogNotifierActorId(const TActorId &aid) {
            LogNotifierActorId = aid;
        }

        TActorId GetLogNotifierActorId() const {
            Y_ABORT_UNLESS(LogNotifierActorId);
            return LogNotifierActorId;
        }
    };

    //////////////////////////////////////////////////////////////////////////
    // TEvCompactionFinished
    //////////////////////////////////////////////////////////////////////////
    class TEvCompactionFinished : public TEventLocal<
                    TEvCompactionFinished,
                    TEvBlobStorage::EvCompactionFinished>
    {};

    ////////////////////////////////////////////////////////////////////////////
    // FRESH compaction
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    void CompactFreshSegment(
            TIntrusivePtr<THullDs> &hullDs,
            std::shared_ptr<TLevelIndexRunTimeCtx<TKey, TMemRec>> &rtCtx,
            const TActorContext &ctx,
            bool allowGarbageCollection);

    template <class TKey, class TMemRec>
    bool CompactFreshSegmentIfRequired(
            TIntrusivePtr<THullDs> &hullDs,
            std::shared_ptr<TLevelIndexRunTimeCtx<TKey, TMemRec>> &rtCtx,
            const TActorContext &ctx,
            bool force,
            bool allowGarbageCollection)
    {
        ui64 yardFreeUpToLsn = rtCtx->GetFreeUpToLsn();
        bool compact = hullDs->HullCtx->FreshCompaction && rtCtx->LevelIndex->NeedsFreshCompaction(yardFreeUpToLsn, force);
        LOG_DEBUG_S(ctx, NKikimrServices::BS_HULLCOMP, "CompactFreshSegmentIfRequired"
            << ", required: " << compact
            << ", yardFreeUpToLsn: " << yardFreeUpToLsn
            << ", force: " << force
            << ", allowGarbageCollection: " << allowGarbageCollection);
        if (compact) {
            CompactFreshSegment<TKey, TMemRec>(hullDs, rtCtx, ctx, allowGarbageCollection);
        }
        return compact;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Run an actor for every database
    ////////////////////////////////////////////////////////////////////////////
    NActors::IActor* CreateLogoBlobsActor(
            TIntrusivePtr<TVDiskConfig> config,
            TIntrusivePtr<THullDs> hullDs,
            std::shared_ptr<THullLogCtx> hullLogCtx,
            TActorId loggerId,
            std::shared_ptr<TLevelIndexRunTimeCtx<TKeyLogoBlob, TMemRecLogoBlob>> rtCtx,
            std::shared_ptr<NSyncLog::TSyncLogFirstLsnToKeep> syncLogFirstLsnToKeep);

    NActors::IActor* CreateBlocksActor(
            TIntrusivePtr<TVDiskConfig> config,
            TIntrusivePtr<THullDs> hullDs,
            std::shared_ptr<THullLogCtx> hullLogCtx,
            TActorId loggerId,
            std::shared_ptr<TLevelIndexRunTimeCtx<TKeyBlock, TMemRecBlock>> rtCtx,
            std::shared_ptr<NSyncLog::TSyncLogFirstLsnToKeep> syncLogFirstLsnToKeep);

    NActors::IActor* CreateBarriersActor(
            TIntrusivePtr<TVDiskConfig> config,
            TIntrusivePtr<THullDs> hullDs,
            std::shared_ptr<THullLogCtx> hullLogCtx,
            TActorId loggerId,
            std::shared_ptr<TLevelIndexRunTimeCtx<TKeyBarrier, TMemRecBarrier>> rtCtx,
            std::shared_ptr<NSyncLog::TSyncLogFirstLsnToKeep> syncLogFirstLsnToKeep);

} // NKikimr
