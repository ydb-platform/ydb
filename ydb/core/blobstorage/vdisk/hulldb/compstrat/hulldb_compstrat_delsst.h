#pragma once

#include "defs.h"
#include "hulldb_compstrat_defs.h"

namespace NKikimr {
    namespace NHullComp {

        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TStrategyDelSst
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec>
        class TStrategyDelSst {
        public:
            using TLevelIndexSnapshot = ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
            using TLevelSliceSnapshot = ::NKikimr::TLevelSliceSnapshot<TKey, TMemRec>;
            using TTask = ::NKikimr::NHullComp::TTask<TKey, TMemRec>;
            using TSstIterator = typename TLevelSliceSnapshot::TSstIterator;
            using TLevelSegment = ::NKikimr::TLevelSegment<TKey, TMemRec>;
            using TLevelSstPtr = typename TLevelSegment::TLevelSstPtr;

            TStrategyDelSst(
                    TIntrusivePtr<THullCtx> hullCtx,
                    const TLevelIndexSnapshot &levelSnap,
                    TTask *task)
                : HullCtx(std::move(hullCtx))
                , LevelSnap(levelSnap)
                , Task(task)
            {}

            EAction Select() {
                TInstant startTime(TAppData::TimeProvider->Now());
                EAction action = FindSstsToRemove();
                // try to find what to delete
                if (action != ActNothing) {
                    Task->SetupAction(action);
                }

                TInstant finishTime(TAppData::TimeProvider->Now());
                if (HullCtx->VCtx->ActorSystem) {
                    LOG_LOG(*HullCtx->VCtx->ActorSystem, action == ActNothing ? NLog::PRI_DEBUG : NLog::PRI_INFO,
                            NKikimrServices::BS_HULLCOMP,
                            VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                                "%s: DelSst: action# %s timeSpent# %s sstsToDelete# %" PRIu32,
                                PDiskSignatureForHullDbKey<TKey>().ToString().data(),
                                ActionToStr(action), (finishTime - startTime).ToString().data(),
                                SstToDelete));
                }

                return action;
            }

        private:
            TIntrusivePtr<THullCtx> HullCtx;
            const TLevelIndexSnapshot &LevelSnap;
            TTask *Task = nullptr;
            ui32 SstToDelete = 0;

            EAction FindSstsToRemove() {
                EAction action = ActNothing;

                // traverse index and find ssts to remove
                TSstIterator it(&LevelSnap.SliceSnap);
                it.SeekToFirst();
                while (it.Valid()) {
                    TLevelSstPtr p = it.Get();
                    TSstRatioPtr ratio = p.SstPtr->StorageRatio.Get();
                    if (p.Level > 0 && ratio && ratio->CanDeleteSst()) {
                        action = ActDeleteSsts;
                        if (HullCtx->VCtx->ActorSystem) {
                            LOG_INFO_S(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP,
                                HullCtx->VCtx->VDiskLogPrefix << " TStrategyDelSst going to delete SST# " << p.ToString() << " because of ration# " << ratio->ToString());
                        }
                        Task->DeleteSsts.DeleteSst(p.Level, p.SstPtr);
                        SstToDelete++;
                    }

                    it.Next();
                }

                return action;
            }
        };

    } // NHullComp
} // NKikimr
