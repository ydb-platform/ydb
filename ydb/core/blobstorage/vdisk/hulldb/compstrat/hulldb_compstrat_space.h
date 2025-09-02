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
                , Candidate(HullCtx->ChunkSize, HullCtx->HullCompFreeSpaceThreshold)
            {}

            EAction Select() {
                TInstant startTime(TAppData::TimeProvider->Now());
                EAction action = FreeSpace();
                if (action != ActNothing) {
                    Task->SetupAction(action);
                }

                TInstant finishTime(TAppData::TimeProvider->Now());
                if (HullCtx->VCtx->ActorSystem) {
                    LOG_LOG(*HullCtx->VCtx->ActorSystem, action == ActNothing ? NLog::PRI_DEBUG : NLog::PRI_INFO,
                            NKikimrServices::BS_HULLCOMP,
                            VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                                "%s: FreeSpace: action# %s timeSpent# %s candidate# %s",
                                PDiskSignatureForHullDbKey<TKey>().ToString().data(),
                                ActionToStr(action), (finishTime - startTime).ToString().data(),
                                Candidate.ToString().data()));
                }

                return action;
            }

        private:
            ////////////////////////////////////////////////////////////////////////
            // NHullComp::NPriv::TMostAbusingSst
            ////////////////////////////////////////////////////////////////////////
            struct TMostAbusingSst {
                // constants
                const ui64 ChunkSize;
                const double FreeSpaceThreshold;
                // fields
                TLevelSstPtr LevelSstPtr;
                double Rank = 0;
                bool Present = false;

                TMostAbusingSst(ui64 chunkSize, double freeSpaceThreshold)
                    : ChunkSize(chunkSize)
                    , FreeSpaceThreshold(freeSpaceThreshold)
                {}

                void Add(TLevelSstPtr &&p) {
                    TSstRatioPtr ratio = p.SstPtr->StorageRatio.Get();
                    if (ratio) {
                        const ui64 garbageHugeSize = ratio->HugeDataTotal - ratio->HugeDataKeep;
                        const double rank = (double)garbageHugeSize / ChunkSize;
                        if (rank > Rank) {
                            LevelSstPtr = std::move(p);
                            Rank = rank;
                            Present = true;
                        }
                    }
                }

                bool CompactSstToFreeSpace() const {
                    return Present && Rank > FreeSpaceThreshold;
                }

                TString ToString() const {
                    TStringStream str;
                    str << "{Rank# " << Rank << " Level# " << LevelSstPtr.Level << "}";
                    return str.Str();
                }
            };

            ////////////////////////////////////////////////////////////////////////
            // Private Fields
            ////////////////////////////////////////////////////////////////////////
            TIntrusivePtr<THullCtx> HullCtx;
            const TLevelIndexSnapshot &LevelSnap;
            TTask *Task;
            TMostAbusingSst Candidate;

            EAction FreeSpace() {
                EAction action = ActNothing;

                // find most abusing sst (which wastes space)
                TLevelSliceSnapshot sliceSnap = LevelSnap.SliceSnap;
                TSstIterator it(&sliceSnap);
                it.SeekToFirst();
                while (it.Valid()) {
                    TLevelSstPtr p = it.Get();
                    if (p.Level > 0) {
                        // TODO: handle zero level segments also
                        Candidate.Add(std::move(p));
                    }

                    it.Next();
                }

                if (Candidate.CompactSstToFreeSpace()) {
                    // free space by compacting this Sst
                    LOG_INFO_S(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP,
                            HullCtx->VCtx->VDiskLogPrefix << " TStrategyFreeSpace decided to compact Ssts " << Task->CompactSsts.ToString()
                            << " because of high garbage/data retio " << Candidate.ToString());
                    action = ActCompactSsts;
                    TUtils::SqueezeOneSst(LevelSnap.SliceSnap, Candidate.LevelSstPtr, Task->CompactSsts);
                }

                return action;
            }
        };

    } // NHullComp
} // NKikimr
