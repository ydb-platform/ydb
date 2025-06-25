#pragma once

#include "defs.h"
#include "hulldb_compstrat_utils.h"

namespace NKikimr {
    namespace NHullComp {

        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TStrategyFreeSpace
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec>
        class TStrategyFreeSpace {
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


            TStrategyFreeSpace(
                    TIntrusivePtr<THullCtx> hullCtx,
                    const TLevelIndexSnapshot &levelSnap,
                    TTask *task)
                : HullCtx(std::move(hullCtx))
                , LevelSnap(levelSnap)
                , Task(task)
                , Candidates(HullCtx->HullCompHugeGarbageThreshold)
            {}

            EAction Select() {
                TInstant startTime(TAppData::TimeProvider->Now());
                EAction action = FreeSpace();
                if (action != ActNothing) {
                    Task->SetupAction(action);
                }

                TInstant finishTime(TAppData::TimeProvider->Now());
                if (HullCtx->VCtx->ActorSystem) {
                    LOG_INFO(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP,
                            VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                                "%s: FreeSpace: action# %s timeSpent# %s candidates# %s",
                                PDiskSignatureForHullDbKey<TKey>().ToString().data(),
                                ActionToStr(action), (finishTime - startTime).ToString().data(),
                                Candidates.ToString().data()));
                }

                return action;
            }

        private:
            ////////////////////////////////////////////////////////////////////////
            // NHullComp::NPriv::TMostAbusingSsts
            ////////////////////////////////////////////////////////////////////////
            struct TMostAbusingSsts {
                // constants
                const double GarbageThreshold;

                std::vector<TLevelSstPtr> SstsToCompact;
                ui32 MaxLevel = 0;

                TMostAbusingSsts(double garbageThreshold)
                    : GarbageThreshold(garbageThreshold)
                {}

                void Add(TLevelSstPtr &&p) {
                    TSstRatioPtr ratio = p.SstPtr->StorageRatio.Get();
                    if (ratio) {
                        // garbage changes in range [0, +inf), 0 for full chunk, +inf for chunk containing only garbage
                        const double garbage = (double)(ratio->HugeDataTotal - ratio->HugeDataKeep) / std::max<ui64>(1, ratio->HugeDataKeep);
                        if (garbage > GarbageThreshold) {
                            // only one-level compaction is possible, so save max level and in the end filter out SSTs for other levels
                            MaxLevel = std::max(MaxLevel, p.Level);
                            SstsToCompact.emplace_back(std::move(p));
                        }
                    }
                }

                void FilterOutSstsNotFromMaxLevel() {
                    std::remove_if(SstsToCompact.begin(), SstsToCompact.end(), 
                        [maxLevel=MaxLevel] (const auto &p) {
                            return p.Level != maxLevel;
                    });
                }

                TString ToString() const {
                    TStringStream str;
                    str << "{MaxLevel# " << MaxLevel;
                    for (const auto& p : SstsToCompact) {
                        str << p.ToString() << ", ";
                    }
                    str << "}";
                    return str.Str();
                }
            };

            ////////////////////////////////////////////////////////////////////////
            // Private Fields
            ////////////////////////////////////////////////////////////////////////
            TIntrusivePtr<THullCtx> HullCtx;
            const TLevelIndexSnapshot &LevelSnap;
            TTask *Task;
            TMostAbusingSsts Candidates;

            EAction FreeSpace() {
                EAction action = ActNothing;

                // find most abusing sst (which wastes space)
                TLevelSliceSnapshot sliceSnap = LevelSnap.SliceSnap;
                TSstIterator it(&sliceSnap);
                it.SeekToFirst();
                while (it.Valid()) {
                    TLevelSstPtr p = it.Get();
                    if (p.Level > 0) {
                        Candidates.Add(std::move(p));
                    }

                    it.Next();
                }

                Candidates.FilterSstsFromOnlyMaxLevel();

                auto& ssts = Candidates.SstsToCompact;
                if (!ssts.empty()) {
                    // free space by compacting these SSTs
                    action = ActCompactSsts;
                    for (auto& sst : ssts) {
                        TUtils::SqueezeOneSst(LevelSnap.SliceSnap, sst, Task->CompactSsts);
                    }
                }

                return action;
            }
        };

    } // NHullComp
} // NKikimr

