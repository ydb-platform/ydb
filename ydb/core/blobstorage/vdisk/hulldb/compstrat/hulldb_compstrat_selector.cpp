#include "hulldb_compstrat_selector.h"
#include "hulldb_compstrat_balance.h"
#include "hulldb_compstrat_delsst.h"
#include "hulldb_compstrat_lazy.h"
#include "hulldb_compstrat_promote.h"
#include "hulldb_compstrat_ratio.h"
#include "hulldb_compstrat_space.h"
#include "hulldb_compstrat_squeeze.h"

namespace NKikimr {
    namespace NHullComp {

        const ui64 TBoundaries::MaxPossibleDiskSize = ui64(16) << ui64(40);

        ///////////////////////////////////////////////////////////////////////////////////////
        // LogoBlobs
        ///////////////////////////////////////////////////////////////////////////////////////
        template <>
        EAction TStrategy<TKeyLogoBlob, TMemRecLogoBlob>::Select() {
            EAction action = ActNothing;

            using TStrategyBalance = ::NKikimr::NHullComp::TStrategyBalance<TKeyLogoBlob, TMemRecLogoBlob>;
            using TStrategyDelSst = ::NKikimr::NHullComp::TStrategyDelSst<TKeyLogoBlob, TMemRecLogoBlob>;
            using TStrategyFreeSpace = ::NKikimr::NHullComp::TStrategyFreeSpace<TKeyLogoBlob, TMemRecLogoBlob>;
            using TStrategyPromoteSsts = ::NKikimr::NHullComp::TStrategyPromoteSsts<TKeyLogoBlob, TMemRecLogoBlob>;
            using TStrategyStorageRatio = ::NKikimr::NHullComp::TStrategyStorageRatio<TKeyLogoBlob, TMemRecLogoBlob>;
            using TStrategySqueeze = ::NKikimr::NHullComp::TStrategySqueeze<TKeyLogoBlob, TMemRecLogoBlob>;


            // calculate storage ratio and gather space consumption statistics
            TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> barriersEssence = BarriersSnap.CreateEssence(HullCtx);
            BarriersSnap.Destroy();
            TStrategyStorageRatio(HullCtx, LevelSnap, std::move(barriersEssence), AllowGarbageCollection).Work();

            // delete free ssts
            action = TStrategyDelSst(HullCtx, LevelSnap, Task).Select();
            if (action != ActNothing) {
                return action;
            }

            // try to promote ssts on higher levels w/o merging
            action = TStrategyPromoteSsts(HullCtx, Params.Boundaries, LevelSnap, Task).Select();
            if (action != ActNothing) {
                return action;
            }

            // try to find what to compact based on levels balance
            action = TStrategyBalance(HullCtx, Params, LevelSnap, Task).Select();
            if (action != ActNothing) {
                return action;
            }

            // try to find what to compact base on storage consumption
            action = TStrategyFreeSpace(HullCtx, LevelSnap, Task).Select();
            if (action != ActNothing) {
                return action;
            }

            // try to squeeze if required
            if (Params.SqueezeBefore) {
                action = TStrategySqueeze(HullCtx, LevelSnap, Task, Params.SqueezeBefore).Select();
                if (action != ActNothing) {
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
                return action;
            }

            // try to find what to compact based on levels balance
            action = TStrategyBalance(HullCtx, Params, LevelSnap, Task).Select();
            if (action != ActNothing) {
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
                return action;
            }

            // try to find what to compact based on levels balance
            action = TStrategyBalance(HullCtx, Params, LevelSnap, Task).Select();
            if (action != ActNothing) {
                return action;
            }

            return action;
        }

    } // NHullComp
} // NKikimr

