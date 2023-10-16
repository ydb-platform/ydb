#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_idx.h>
#include <ydb/core/blobstorage/vdisk/hulldb/barriers/barriers_public.h>
#include "hull_ds_all_snap.h"

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // NOTES: include this file if you want to change VDisk Local DB (Hull)
    //        datastructures; for read-only scenarios include hull_ds_all_snap.h
    //        only
    ////////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////////
    // Sst (string sorted table) types for LogoBlobs, Blocks, Barriers
    ////////////////////////////////////////////////////////////////////////////
    using TLogoBlobsSst = TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>;
    using TBlocksSst = TLevelSegment<TKeyBlock, TMemRecBlock>;
    using TBarriersSst = NBarriers::TBarriersSst;

    using TLogoBlobsSstPtr = TIntrusivePtr<TLogoBlobsSst>;
    using TBlocksSstPtr = TIntrusivePtr<TBlocksSst>;
    using TBarriersSstPtr = TIntrusivePtr<TBarriersSst>;

    ////////////////////////////////////////////////////////////////////////////
    // DS (data structure) types for LogoBlobs, Blocks, Barriers
    ////////////////////////////////////////////////////////////////////////////
    using TLogoBlobsDs = TLevelIndex<TKeyLogoBlob, TMemRecLogoBlob>;
    using TBlocksDs = TLevelIndex<TKeyBlock, TMemRecBlock>;
    using TBarriersDs = NBarriers::TBarriersDs;

    ////////////////////////////////////////////////////////////////////////////
    // Batch update for fresh DS
    ////////////////////////////////////////////////////////////////////////////
    using TFreshAppendixLogoBlobs = TFreshAppendix<TKeyLogoBlob, TMemRecLogoBlob>;
    using TFreshAppendixBlocks = TFreshAppendix<TKeyBlock, TMemRecBlock>;
    using TFreshAppendixBarriers = TFreshAppendix<TKeyBarrier, TMemRecBarrier>;

    struct TFreshBatch {
        std::shared_ptr<TFreshAppendixLogoBlobs> LogoBlobs;
        std::shared_ptr<TFreshAppendixBlocks> Blocks;
        std::shared_ptr<TFreshAppendixBarriers> Barriers;

        bool IsReady() const { return LogoBlobs || Blocks || Barriers; }
        void Clear() {
            LogoBlobs.reset();
            Blocks.reset();
            Barriers.reset();
        }
        ui64 GetRecords() const {
            return (LogoBlobs ? LogoBlobs->GetSize() : 0)
                + (Blocks ? Blocks->GetSize() : 0) + (Barriers ? Barriers->GetSize() : 0);
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // THullDs
    // Union of all data structure types
    ////////////////////////////////////////////////////////////////////////////
    struct THullDs : public TThrRefBase {
        TIntrusivePtr<THullCtx> HullCtx;
        TIntrusivePtr<TLogoBlobsDs> LogoBlobs;
        TIntrusivePtr<TBlocksDs> Blocks;
        TIntrusivePtr<TBarriersDs> Barriers;

        THullDs(TIntrusivePtr<THullCtx> hullCtx)
            : HullCtx(std::move(hullCtx))
        {}

        ui64 GetFirstLsnToKeep() const {
            ui64 logoBlobLsn = LogoBlobs->GetFirstLsnToKeep();
            ui64 blocksLsn = Blocks->GetFirstLsnToKeep();
            ui64 barriersLsn = Barriers->GetFirstLsnToKeep();
            return Min(logoBlobLsn, Min(blocksLsn, barriersLsn));
        }

        inline TSatisfactionRank GetSatisfactionRank(EHullDbType t, ESatisfactionRankType s) const {
            switch (t) {
                case EHullDbType::LogoBlobs:    return LogoBlobs->GetSatisfactionRank(s);
                case EHullDbType::Blocks:       return Blocks->GetSatisfactionRank(s);
                case EHullDbType::Barriers:     return Barriers->GetSatisfactionRank(s);
                default:                        Y_ABORT("Unexpected t=%d", int(t));
            }
        }

        THullDsSnap GetSnapshot(TActorSystem *as) const {
            return THullDsSnap {
                HullCtx,
                LogoBlobs->GetSnapshot(as),
                Blocks->GetSnapshot(as),
                Barriers->GetSnapshot(as)
            };
        }

        THullDsSnap GetIndexSnapshot() const {
            return THullDsSnap {
                HullCtx,
                LogoBlobs->GetSnapshot(nullptr),
                Blocks->GetSnapshot(nullptr),
                Barriers->GetSnapshot(nullptr)
            };
        }
    };

} // NKikimr

