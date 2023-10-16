#pragma once

#include "defs.h"
#include "hulldb_compstrat_defs.h"

// FIXME:
// 1. Check how we calculate compaction rank (FreshRank/LevelRank)
// 2. We now have much more levels, check monitoring

namespace NKikimr {
    namespace NHullComp {

        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::IStorageLayer
        // VDisk Database has multiple storage layers:
        // 1. So called Level0 which is built of SSTs after Fresh (MemTable) compaction
        // 2. StorageLayer1 -- SSTs which were compacted at least once
        // 3. StorageLayer2 -- SSTs which were compacted multiple times
        //
        // SST -- String Sorted Table
        //
        //    == Unordered Level (at one level SSTs are not ordered between each other)
        // StorageLayer0:  SST  SST  ...  SST
        //
        //    == Sorted Levels (at one level SSTs are ordered by key)
        // StorageLayer1:
        //    0:  SST  SST  ...  SST
        //    1:  SST  SST  ...  SST
        //    ......................
        //    15: SST  SST  ...  SST
        // StorageLayer2:
        //    16: SST  SST  ...  SST
        //    17: SST  SST  ...  SST
        //    ......................
        //    31: SST  SST  ...  SST
        //
        ////////////////////////////////////////////////////////////////////////////
        class IStorageLayer {
        public:
            IStorageLayer() = default;
            virtual ~IStorageLayer() = default;

            double GetCompactionScore() {
                if (!CompScore) {
                    CompScore = CalculateCompactionScore();
                }
                return *CompScore;
            }

            virtual void BuildCompactionTask() = 0;
            virtual double CalculateCompactionScore() = 0;

        private:
            TMaybe<double> CompScore;
        };

        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TStorageLayerBase
        // Base class for Storage Layer
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec>
        class TStorageLayerBase : public IStorageLayer {
        public:
            using TLevelSegment = ::NKikimr::TLevelSegment<TKey, TMemRec>;
            using TLevelSegmentPtr = TIntrusivePtr<TLevelSegment>;
            using TSegments = TVector<TLevelSegmentPtr>;
            using TLevelIndex = ::NKikimr::TLevelIndex<TKey, TMemRec>;
            using TLevelSliceSnapshot = ::NKikimr::TLevelSliceSnapshot<TKey, TMemRec>;
            using TTask = ::NKikimr::NHullComp::TTask<TKey, TMemRec>;
            using TOrderedLevelSegments = ::NKikimr::TOrderedLevelSegments<TKey, TMemRec>;
            using TOrderedLevelSegmentsPtr = TIntrusivePtr<TOrderedLevelSegments>;
            using TLevelSstPtr = typename TLevelSegment::TLevelSstPtr;
            using TLevelIndexSnapshot = ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
            using TSortedLevel = ::NKikimr::TSortedLevel<TKey, TMemRec>;

            TStorageLayerBase(
                    const TIntrusivePtr<THullCtx> &hullCtx,
                    const TBoundariesConstPtr &boundaries,
                    const TLevelIndexSnapshot &levelSnap,
                    TTask *task)
                : HullCtx(hullCtx)
                , Boundaries(boundaries)
                , LevelSnap(levelSnap)
                , Task(task)
            {}


        protected:
            TIntrusivePtr<THullCtx> HullCtx;
            TBoundariesConstPtr Boundaries;
            const TLevelIndexSnapshot &LevelSnap;
            TTask *Task;

            double CompactionScoreBasedOnFreeLevels(ui32 freeLevels, ui32 totalLevels) {
                Y_ABORT_UNLESS(freeLevels <= totalLevels);
                if (freeLevels == 0) {
                    return 1000000.0;
                } else {
                    // rate grows up linearly with the number of used levels;
                    // it has boot of 12.5% (1/8)
                    double step = 1.0 / totalLevels;
                    return step * (1.125 * totalLevels - freeLevels);
                }
            }
        };


        ////////////////////////////////////////////////////////////////////////////
        // TStorageLayer0
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec>
        class TStorageLayer0 : public TStorageLayerBase<TKey, TMemRec> {
        public:
            using TBase = TStorageLayerBase<TKey, TMemRec>;
            using TBase::LevelSnap;
            using TBase::Boundaries;
            using TBase::Task;
            using typename TBase::TSortedLevel;
            using typename TBase::TLevelSegmentPtr;

            TStorageLayer0(
                    const TIntrusivePtr<THullCtx> &hullCtx,
                    const TBoundariesConstPtr &boundaries,
                    const typename TBase::TLevelIndexSnapshot &levelSnap,
                    typename TBase::TTask *task)
                : TBase(hullCtx, boundaries, levelSnap, task)
            {}

            ui32 FindEmptyLevel(ui32 layer1Levels) {
                ui32 sortedLevelsNum = LevelSnap.SliceSnap.GetLevelXNumber();
                ui32 iterations = Min(layer1Levels, sortedLevelsNum);
                for (ui32 i = 0; i < iterations; ++i) {
                    const TSortedLevel &level = LevelSnap.SliceSnap.GetLevelXRef(i);
                    if (level.Empty()) {
                        return i;
                    }
                }

                Y_ABORT_UNLESS(sortedLevelsNum + 1 < layer1Levels);
                return sortedLevelsNum + 1;
            }

            ui32 AddSSTsForCompaction() {
                ui32 added = 0;
                auto it = LevelSnap.SliceSnap.GetLevel0SstIterator();
                it.SeekToFirst();
                Y_DEBUG_ABORT_UNLESS(it.Valid());
                // FIXME: check why we have a limit here
                while (it.Valid() && added < Boundaries->Level0MaxSstsAtOnce) {
                    // push to the task
                    TLevelSegmentPtr sst(it.Get());
                    Task->CompactSsts.PushOneSst(0, sst);
                    added++;
                    it.Next();
                }
                Y_ABORT_UNLESS(added > 0);
                return added;
            }

            virtual void BuildCompactionTask() override {
                const ui32 layer1Levels = Boundaries->SortedParts * 2;
                const ui32 emptyLevel = FindEmptyLevel(layer1Levels);
                Task->CompactSsts.TargetLevel = emptyLevel + 1;
                AddSSTsForCompaction();
            }

            virtual double CalculateCompactionScore() override {
                // calculate score as a ration of number of SSTs to max allowed SSTs at 0 level
                const double curSSTs = LevelSnap.SliceSnap.GetLevel0SstsNum();
                const double maxSSTs = Boundaries->Level0MaxSstsAtOnce;
                return curSSTs / maxSSTs + 0.001;
            }
        };

        ////////////////////////////////////////////////////////////////////////////
        // TStorageLayer1
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec>
        class TStorageLayer1 : public TStorageLayerBase<TKey, TMemRec> {
        public:
            using TBase = TStorageLayerBase<TKey, TMemRec>;
            using TBase::LevelSnap;
            using TBase::Boundaries;
            using typename TBase::TSortedLevel;
            using TBase::CompactionScoreBasedOnFreeLevels;

            TStorageLayer1(
                    const TIntrusivePtr<THullCtx> &hullCtx,
                    const TBoundariesConstPtr &boundaries,
                    const typename TBase::TLevelIndexSnapshot &levelSnap,
                    typename TBase::TTask *task)
                : TBase(hullCtx, boundaries, levelSnap, task)
            {}

            // Calculate free levels
            ui32 FreeLevels(ui32 layer1Levels) {
                ui32 sortedLevelsNum = LevelSnap.SliceSnap.GetLevelXNumber();
                ui32 freeLevels = 0;
                ui32 iterations = Min(layer1Levels, sortedLevelsNum);
                for (ui32 i = 0; i < iterations; ++i) {
                    const TSortedLevel &level = LevelSnap.SliceSnap.GetLevelXRef(i);
                    freeLevels += !!level.Empty();
                }

                if (layer1Levels > sortedLevelsNum) {
                    freeLevels += layer1Levels - sortedLevelsNum;
                }

                return freeLevels;
            }

            // FIXME: implement
            virtual void BuildCompactionTask() = 0;
            virtual double CalculateCompactionScore() override {
                // total partially sorted levels
                const ui32 layer1Levels = Boundaries->SortedParts * 2;
                const ui32 freeLevels = FreeLevels(layer1Levels);
                return CompactionScoreBasedOnFreeLevels(freeLevels, layer1Levels);
            }
        };



        ////////////////////////////////////////////////////////////////////////////
        // TStorageLayer2
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec>
        class TStorageLayer2 : public TStorageLayerBase<TKey, TMemRec> {
        public:
            using TBase = TStorageLayerBase<TKey, TMemRec>;
            using TBase::LevelSnap;
            using TBase::Boundaries;
            using typename TBase::TSortedLevel;
            using TBase::CompactionScoreBasedOnFreeLevels;

            TStorageLayer2(
                    const TIntrusivePtr<THullCtx> &hullCtx,
                    const TBoundariesConstPtr &boundaries,
                    const typename TBase::TLevelIndexSnapshot &levelSnap,
                    typename TBase::TTask *task)
                : TBase(hullCtx, boundaries, levelSnap, task)
            {}

            // Calculate free levels
            ui32 FreeLevels(ui32 layer1Levels, ui32 layer2Levels) {
                ui32 sortedLevelsNum = LevelSnap.SliceSnap.GetLevelXNumber();
                Y_ABORT_UNLESS(layer1Levels + layer2Levels >= sortedLevelsNum);

                if (sortedLevelsNum > layer1Levels) {
                    ui32 freeLevels = 0;
                    for (ui32 i = layer1Levels; i < sortedLevelsNum; ++i) {
                        const TSortedLevel &level = LevelSnap.SliceSnap.GetLevelXRef(i);
                        freeLevels += !!level.Empty();
                    }
                    freeLevels += layer2Levels + layer1Levels - sortedLevelsNum;
                    return freeLevels;
                } else {
                    return layer2Levels;
                }
            }

            // FIXME: implement
            virtual void BuildCompactionTask() = 0;
            virtual double CalculateCompactionScore() override {
                // total partially sorted levels
                const ui32 layer1Levels = Boundaries->SortedParts * 2;
                const ui32 layer2Levels = Boundaries->SortedParts * 2;
                const ui32 freeLevels = FreeLevels(layer1Levels, layer2Levels);
                return CompactionScoreBasedOnFreeLevels(freeLevels, layer2Levels);
            }
        };

        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TStrategyBalance
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec>
        class TStrategyLazy {
        public:
            using TLevelIndexSnapshot = ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
            using TTask = ::NKikimr::NHullComp::TTask<TKey, TMemRec>;

            using TStorageLayer0 = NHullComp::TStorageLayer0<TKey, TMemRec>;
            using TStorageLayer1 = NHullComp::TStorageLayer1<TKey, TMemRec>;
            using TStorageLayer2 = NHullComp::TStorageLayer2<TKey, TMemRec>;

            TStrategyLazy(
                    const TIntrusivePtr<THullCtx> &hullCtx,
                    TBoundariesConstPtr &boundaries,
                    const TLevelIndexSnapshot &levelSnap,
                    TTask *task,
                    double scoreThreshold)
                : HullCtx(hullCtx)
                , Task(task)
                , ScoreThreshold(scoreThreshold)
                , Layer0(hullCtx, boundaries, levelSnap, task)
                , Layer1(hullCtx, boundaries, levelSnap, task)
                , Layer2(hullCtx, boundaries, levelSnap, task)
            {}

            EAction Select() {
                TInstant startTime(TAppData::TimeProvider->Now());
                EAction action = Balance();
                TInstant finishTime(TAppData::TimeProvider->Now());
                if (HullCtx->VCtx->ActorSystem) {
                    LOG_INFO(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP,
                            VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                                "%s: Balance: action# %s timeSpent# %s ScoreThreshold# %e",
                                PDiskSignatureForHullDbKey<TKey>().ToString().data(),
                                ActionToStr(action), (finishTime - startTime).ToString().data(),
                                ScoreThreshold));
                }

                return action;
            }

        private:
            TIntrusivePtr<THullCtx> HullCtx;
            TTask *Task;
            const double ScoreThreshold;
            TStorageLayer0 Layer0;
            TStorageLayer1 Layer1;
            TStorageLayer2 Layer2;

            EAction Balance() {
                IStorageLayer *s = &Layer0;
                for (IStorageLayer *x : {&Layer1, &Layer2}) {
                    if (x->GetCompactionScore() > s->GetCompactionScore()) {
                        s = x;
                    }
                }
                if (s->GetCompactionScore() >= ScoreThreshold) {
                    s->BuildCompactionTask();
                    return ActCompactSsts;
                } else {
                    Task->SetupAction(ActNothing);
                    return ActNothing;
                }
            }
        };

    } // NHullComp
} // NKikimr

