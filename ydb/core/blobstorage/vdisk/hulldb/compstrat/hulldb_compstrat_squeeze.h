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
                    const TSelectorParams &params,
                    const TLevelIndexSnapshot &levelSnap,
                    TTask *task,
                    TInstant squeezeBefore)
                : HullCtx(std::move(hullCtx))
                , Params(params)
                , LevelSnap(levelSnap)
                , Task(task)
                , SqueezeBefore(squeezeBefore)
            {}

            EAction Select() {
                TInstant startTime(TAppData::TimeProvider->Now());
                EAction action = SelectQuantum();
                if (action != ActNothing) {
                    Task->SetupAction(action);
                    Task->SelectStrategy = ESelectStrategy::Squeeze;
                }

                TInstant finishTime(TAppData::TimeProvider->Now());
                if (HullCtx->VCtx->ActorSystem) {
                    YDB_LOG_CTX_COMP(*HullCtx->VCtx->ActorSystem, action == ActNothing ? NLog::PRI_DEBUG : NLog::PRI_INFO, NKikimrServices::BS_HULLCOMP, VDISKP(HullCtx->VCtx->VDiskLogPrefix, "%s: FreeSpace: action# %s timeSpent# %s", PDiskSignatureForHullDbKey<TKey>().ToString().data(), ActionToStr(action), (finishTime - startTime).ToString().data()));
                }

                return action;
            }

        private:
            ////////////////////////////////////////////////////////////////////////
            // Private Fields
            ////////////////////////////////////////////////////////////////////////
            TIntrusivePtr<THullCtx> HullCtx;
            const TSelectorParams &Params;
            const TLevelIndexSnapshot &LevelSnap;
            TTask *Task;
            const TInstant SqueezeBefore;

            // The budget is what this VDisk may allocate for compaction output; the default is
            // unbounded, for the case where no space observation has arrived yet.
            bool FitsBudget(const TLevelSegment &sst) const {
                if (Params.FreeChunksBudget == Max<ui32>()) {
                    return true;
                }
                return TUtils::EstimateOutputChunks(TUtils::SstKeepBytes(sst), HullCtx->ChunkSize)
                    <= Params.FreeChunksBudget;
            }

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
                            if (!FitsBudget(*p.SstPtr)) {
                                // Skip it rather than give up on the whole scan: another,
                                // smaller stale sst may still fit what this VDisk was granted.
                                it.Next();
                                continue;
                            }
                            if (HullCtx->VCtx->ActorSystem) {
                                YDB_LOG_INFO_CTX_COMP(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP, "TStrategySqueeze decided to compact Sst",
                                    {"VDiskLogPrefix", HullCtx->VCtx->VDiskLogPrefix},
                                    {"p", p});
                            }
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
