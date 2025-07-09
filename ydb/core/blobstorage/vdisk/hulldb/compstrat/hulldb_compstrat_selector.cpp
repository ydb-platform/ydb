#include "hulldb_compstrat_selector.h"
#include "hulldb_compstrat_balance.h"
#include "hulldb_compstrat_delsst.h"
#include "hulldb_compstrat_promote.h"
#include "hulldb_compstrat_ratio.h"
#include "hulldb_compstrat_space.h"
#include "hulldb_compstrat_squeeze.h"
#include "hulldb_compstrat_explicit.h"

namespace NKikimr {
    namespace NHullComp {

        const ui64 TBoundaries::MaxPossibleDiskSize = ui64(16) << ui64(40);

        ///////////////////////////////////////////////////////////////////////////////////////
        // LogoBlobs
        ///////////////////////////////////////////////////////////////////////////////////////
        template <>
        EAction TStrategy<TKeyLogoBlob, TMemRecLogoBlob>::Select() {
            EAction action = ActNothing;

            using TStrategyExplicit = NHullComp::TStrategyExplicit<TKeyLogoBlob, TMemRecLogoBlob>;
            using TStrategyBalance = NHullComp::TStrategyBalance<TKeyLogoBlob, TMemRecLogoBlob>;
            using TStrategyDelSst = NHullComp::TStrategyDelSst<TKeyLogoBlob, TMemRecLogoBlob>;
            using TStrategyFreeSpace = NHullComp::TStrategyFreeSpace<TKeyLogoBlob, TMemRecLogoBlob>;
            using TStrategyPromoteSsts = NHullComp::TStrategyPromoteSsts<TKeyLogoBlob, TMemRecLogoBlob>;
            using TStrategyStorageRatio = NHullComp::TStrategyStorageRatio<TKeyLogoBlob, TMemRecLogoBlob>;
            using TStrategySqueeze = NHullComp::TStrategySqueeze<TKeyLogoBlob, TMemRecLogoBlob>;

            // calculate storage ratio and gather space consumption statistics
            TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> barriersEssence = BarriersSnap.CreateEssence(HullCtx);
            BarriersSnap.Destroy();
            TStrategyStorageRatio(HullCtx, LevelSnap, std::move(barriersEssence), AllowGarbageCollection).Work();

            // delete free ssts
            action = TStrategyDelSst(HullCtx, LevelSnap, Task).Select();
            if (action != ActNothing) {
                ++HullCtx->CompactionStrategyGroup.BlobsDelSst();
                return action;
            }

            // try to promote ssts on higher levels w/o merging
            action = TStrategyPromoteSsts(HullCtx, Params.Boundaries, LevelSnap, Task).Select();
            if (action != ActNothing) {
                ++HullCtx->CompactionStrategyGroup.BlobsPromoteSsts();
                return action;
            }

            // compact explicitly defined SST's, if set
            action = TStrategyExplicit(HullCtx, Params, LevelSnap, Task).Select();
            if (action != ActNothing) {
                ++HullCtx->CompactionStrategyGroup.BlobsExplicit();
                return action;
            }

            // try to find what to compact based on levels balance
            action = TStrategyBalance(HullCtx, Params, LevelSnap, Task).Select();
            if (action != ActNothing) {
                ++HullCtx->CompactionStrategyGroup.BlobsBalance();
                return action;
            }

            // try to find what to compact base on storage consumption
            action = TStrategyFreeSpace(HullCtx, LevelSnap, Task).Select();
            if (action != ActNothing) {
                ++HullCtx->CompactionStrategyGroup.BlobsFreeSpace();
                return action;
            }

            // try to squeeze if required
            if (Params.SqueezeBefore) {
                action = TStrategySqueeze(HullCtx, LevelSnap, Task, Params.SqueezeBefore).Select();
                if (action != ActNothing) {
                    ++HullCtx->CompactionStrategyGroup.BlobsSqueeze();
                    return action;
                }
            }
            return action;
        }

        ///////////////////////////////////////////////////////////////////////////////////////
        // Blocks
        ///////////////////////////////////////////////////////////////////////////////////////
        template <>
        EAction TStrategy<TKeyBlock, TMemRecBlock>::Select() {
            using TStrategyBalance = ::NKikimr::NHullComp::TStrategyBalance<TKeyBlock, TMemRecBlock>;
            using TStrategyPromoteSsts = ::NKikimr::NHullComp::TStrategyPromoteSsts<TKeyBlock, TMemRecBlock>;

            EAction action = ActNothing;

            // free barriers snapshot: don't need it
            BarriersSnap.Destroy();

            // try to promote ssts on higher levels w/o merging
            action = TStrategyPromoteSsts(HullCtx, Params.Boundaries, LevelSnap, Task).Select();
            if (action != ActNothing) {
                ++HullCtx->CompactionStrategyGroup.BlocksPromoteSsts();
                return action;
            }

            // compact explicitly defined SST's, if set
            action = TStrategyExplicit(HullCtx, Params, LevelSnap, Task).Select();
            if (action != ActNothing) {
                ++HullCtx->CompactionStrategyGroup.BlocksExplicit();
                return action;
            }

            // try to find what to compact based on levels balance
            action = TStrategyBalance(HullCtx, Params, LevelSnap, Task).Select();
            if (action != ActNothing) {
                ++HullCtx->CompactionStrategyGroup.BlocksBalance();
                return action;
            }

            return action;
        }

        ///////////////////////////////////////////////////////////////////////////////////////
        // Barriers
        ///////////////////////////////////////////////////////////////////////////////////////
        template <>
        EAction TStrategy<TKeyBarrier, TMemRecBarrier>::Select() {
            using TStrategyBalance = ::NKikimr::NHullComp::TStrategyBalance<TKeyBarrier, TMemRecBarrier>;
            using TStrategyPromoteSsts = ::NKikimr::NHullComp::TStrategyPromoteSsts<TKeyBarrier, TMemRecBarrier>;

            EAction action = ActNothing;

            // free barriers snapshot: don't need it
            BarriersSnap.Destroy();

            // try to promote ssts on higher levels w/o merging
            action = TStrategyPromoteSsts(HullCtx, Params.Boundaries, LevelSnap, Task).Select();
            if (action != ActNothing) {
                ++HullCtx->CompactionStrategyGroup.BarriersPromoteSsts();
                return action;
            }

            // compact explicitly defined SST's, if set
            action = TStrategyExplicit(HullCtx, Params, LevelSnap, Task).Select();
            if (action != ActNothing) {
                ++HullCtx->CompactionStrategyGroup.BarriersExplicit();
                return action;
            }

            // try to find what to compact based on levels balance
            action = TStrategyBalance(HullCtx, Params, LevelSnap, Task).Select();
            if (action != ActNothing) {
                ++HullCtx->CompactionStrategyGroup.BarriersBalance();
                return action;
            }

            return action;
        }

    } // NHullComp
} // NKikimr
