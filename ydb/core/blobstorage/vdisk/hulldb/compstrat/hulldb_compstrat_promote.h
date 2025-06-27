#pragma once

#include "defs.h"
#include "hulldb_compstrat_defs.h"

namespace NKikimr {
    namespace NHullComp {

        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TStrategyPromoteSsts
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec>
        class TStrategyPromoteSsts {
        public:
            typedef ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec> TLevelIndexSnapshot;
            typedef ::NKikimr::TLevelSliceSnapshot<TKey, TMemRec> TLevelSliceSnapshot;
            typedef typename TLevelSliceSnapshot::TSstIterator TSstIterator;
            typedef ::NKikimr::NHullComp::TTask<TKey, TMemRec> TTask;
            typedef ::NKikimr::TLevelSegment<TKey, TMemRec> TLevelSegment;
            typedef TIntrusivePtr<TLevelSegment> TLevelSegmentPtr;
            typedef typename TLevelSegment::TLevelSstPtr TLevelSstPtr;
            typedef ::NKikimr::TSortedLevel<TKey, TMemRec> TSortedLevel;
            typedef ::NKikimr::TOrderedLevelSegments<TKey, TMemRec> TOrderedLevelSegments;
            typedef TIntrusivePtr<TOrderedLevelSegments> TOrderedLevelSegmentsPtr;

            TStrategyPromoteSsts(
                    TIntrusivePtr<THullCtx> hullCtx,
                    TBoundariesConstPtr &boundaries,
                    const TLevelIndexSnapshot &levelSnap,
                    TTask *task)
                : HullCtx(std::move(hullCtx))
                , Boundaries(boundaries)
                , LevelSnap(levelSnap)
                , Task(task)
            {}

            EAction Select() {
                TInstant startTime(TAppData::TimeProvider->Now());
                EAction action = PromoteSsts();
                if (action != ActNothing) {
                    Task->SetupAction(action);
                }

                TInstant finishTime(TAppData::TimeProvider->Now());
                if (HullCtx->VCtx->ActorSystem) {
                    LOG_LOG(*HullCtx->VCtx->ActorSystem, action == ActNothing ? NLog::PRI_DEBUG : NLog::PRI_INFO,
                            NKikimrServices::BS_HULLCOMP,
                            VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                                "%s: PromoteSsts: action# %s timeSpent# %s",
                                PDiskSignatureForHullDbKey<TKey>().ToString().data(),
                                ActionToStr(action), (finishTime - startTime).ToString().data()));
                }

                return action;
            }

        private:
            TIntrusivePtr<THullCtx> HullCtx;
            TBoundariesConstPtr Boundaries;
            const TLevelIndexSnapshot &LevelSnap;
            TTask *Task;

            EAction PromoteSsts() {
                ui32 maxLevel = LevelSnap.SliceSnap.GetLevelXNumber();

                EAction action = ActNothing;
                TSstIterator it(&LevelSnap.SliceSnap);
                it.SeekToFirst();
                while (it.Valid()) {
                    TLevelSstPtr p = it.Get();
                    const ui32 level = p.Level;

                    if (level >= 2 * Boundaries->SortedParts + 1) {
                        TLevelSegmentPtr sst = p.SstPtr;
                        if (level >= maxLevel)
                            break;

                        const TSortedLevel &nextLevel = LevelSnap.SliceSnap.GetLevelXRef(level);
                        if (!DoIntersect(sst, nextLevel.Segs)) {
                            action = ActMoveSsts;
                            Task->MoveSsts.MoveSst(level, level + 1, sst);
                            if (HullCtx->VCtx->ActorSystem) {
                                LOG_INFO_S(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP,
                                    HullCtx->VCtx->VDiskLogPrefix << " TStrategyPromoteSsts: move sst# " << p.ToString() << " to level " << level + 1);
                            }
                            break;
                        }
                    }
                    it.Next();
                }

                // FIXME: we can even create a new level, implement it

                return action;
            }

            bool DoIntersect(TLevelSegmentPtr sst, TOrderedLevelSegmentsPtr segs) {
                if (segs->Empty())
                    return false;

                typename TOrderedLevelSegments::TSstIterator it(segs.Get());
                it.Seek(sst->LastKey());
                if (it.Valid()) {
                    if (it.Get()->FirstKey() == sst->LastKey()) {
                        return true; // intersection by one key
                    } else {
                        it.Prev();
                        if (!it.Valid()) {
                            // we don't have sst in segs before
                            return false;
                        } else {
                            return sst->FirstKey() <= it.Get()->LastKey();
                        }
                    }
                } else {
                    // we stay behind all elems in segs
                    it.SeekToLast();
                    Y_DEBUG_ABORT_UNLESS(it.Valid());
                    // compare
                    return sst->FirstKey() <= it.Get()->LastKey();
                }
            }
        };

    } // NHullComp
} // NKikimr
