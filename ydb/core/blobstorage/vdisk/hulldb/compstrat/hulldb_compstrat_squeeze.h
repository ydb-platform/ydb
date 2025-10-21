#pragma once

#include "defs.h"
#include "hulldb_compstrat_utils.h"

namespace NKikimr {
    namespace NHullComp {

        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TStrategySqueeze
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec>
        class TStrategySqueeze {
        public:
            using TTask = ::NKikimr::NHullComp::TTask<TKey, TMemRec>;
            using TLevelSegment = ::NKikimr::TLevelSegment<TKey, TMemRec>;
            using TLevelSegmentPtr = TIntrusivePtr<TLevelSegment>;
            using TLevelIndex = ::NKikimr::TLevelIndex<TKey, TMemRec>;
            using TLevelIndexSnapshot = ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
            using TLevelSliceSnapshot = ::NKikimr::TLevelSliceSnapshot<TKey, TMemRec>;
            using TSstIterator = typename TLevelSliceSnapshot::TSstIterator;
            using TSortedLevelsIter = typename TLevelSliceSnapshot::TSortedLevelsIter;
            using TLevelSstPtr = typename TLevelSegment::TLevelSstPtr;
            using TUtils = ::NKikimr::NHullComp::TUtils<TKey, TMemRec>;


            TStrategySqueeze(
                    TIntrusivePtr<THullCtx> hullCtx,
                    const TLevelIndexSnapshot &levelSnap,
                    TTask *task,
                    TInstant squeezeBefore)
                : HullCtx(std::move(hullCtx))
                , LevelSnap(levelSnap)
                , Task(task)
                , SqueezeBefore(squeezeBefore)
            {}

            EAction Select() {
                TInstant startTime(TAppData::TimeProvider->Now());
                EAction action = SelectQuantum();
                if (action != ActNothing) {
                    Task->SetupAction(action);
                }

                TInstant finishTime(TAppData::TimeProvider->Now());
                if (HullCtx->VCtx->ActorSystem) {
                    LOG_LOG(*HullCtx->VCtx->ActorSystem, action == ActNothing ? NLog::PRI_DEBUG : NLog::PRI_INFO,
                            NKikimrServices::BS_HULLCOMP,
                            VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                                "%s: FreeSpace: action# %s timeSpent# %s",
                                PDiskSignatureForHullDbKey<TKey>().ToString().data(),
                                ActionToStr(action), (finishTime - startTime).ToString().data()));
                }

                return action;
            }

        private:
            ////////////////////////////////////////////////////////////////////////
            // Private Fields
            ////////////////////////////////////////////////////////////////////////
            TIntrusivePtr<THullCtx> HullCtx;
            const TLevelIndexSnapshot &LevelSnap;
            TTask *Task;
            const TInstant SqueezeBefore;

            EAction SelectQuantum() {
                // FIXME: compact level 0
                // use fuction from balance strategy to compact

                // find most abusing sst (which wastes space)
                TLevelSliceSnapshot sliceSnap = LevelSnap.SliceSnap;
                TSstIterator it(&sliceSnap);
                it.SeekToFirst();
                while (it.Valid()) {
                    TLevelSstPtr p = it.Get();
                    if (p.Level > 0) {
                        if (p.SstPtr->Info.CTime < SqueezeBefore) {
                            LOG_INFO_S(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP,
                                HullCtx->VCtx->VDiskLogPrefix << " TStrategySqueeze decided to compact Sst " << p.ToString());
                            // rewrite this SST squeezed
                            TUtils::SqueezeOneSst(LevelSnap.SliceSnap, p, Task->CompactSsts);
                            return ActCompactSsts;
                        }
                    }

                    it.Next();
                }

                return ActNothing;
            }
        };

    } // NHullComp
} // NKikimr
