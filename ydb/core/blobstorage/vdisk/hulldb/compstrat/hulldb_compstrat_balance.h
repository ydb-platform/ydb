#pragma once

#include "defs.h"
#include "hulldb_compstrat_defs.h"
#include "hulldb_compstrat_utils.h"
#include <ydb/core/blobstorage/vdisk/common/sublog.h>

namespace NKikimr {
    namespace NHullComp {

        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TBalanceBase
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TLevelSliceSnapshot, class TCompactSsts>
        class TBalanceBase {
        public:
            using TSortedLevel = typename TLevelSliceSnapshot::TSortedLevel;
            using TLevelSegmentPtr = typename TLevelSliceSnapshot::TLevelSegmentPtr;
            using TSegments = typename TLevelSliceSnapshot::TSegments;

            TBalanceBase(
                    TIntrusivePtr<THullCtx> hullCtx,
                    TSublog<> &sublog,
                    const TBoundariesConstPtr &boundaries,
                    const TLevelSliceSnapshot &sliceSnap,
                    TCompactSsts &compactSsts,
                    bool &isFullCompaction)
                : HullCtx(std::move(hullCtx))
                , Sublog(sublog)
                , Boundaries(boundaries)
                , SliceSnap(sliceSnap)
                , CompactSsts(compactSsts)
                , IsFullCompaction(isFullCompaction)
            {}

            struct TLess {
                bool operator () (const TKey &key, const TLevelSegmentPtr &ptr) const {
                    return key < ptr->FirstKey();
                }

                bool operator () (const TLevelSegmentPtr &ptr, const TKey &key) const {
                    return ptr->FirstKey() < key;
                }
            };

            // find the neighborhood of the sst to compact at level
            void FindNeighborhoods(
                    const ui32 level,
                    const TKey &firstKeyToCover,
                    const TKey &lastKeyToCover) {
                ui32 trgtLevelArrIdx = level - 1;
                if (trgtLevelArrIdx < SliceSnap.GetLevelXNumber()) {
                    const TSortedLevel &trgtLevel = SliceSnap.GetLevelXRef(trgtLevelArrIdx);
                    const TSegments &trgtSegs = trgtLevel.Segs->Segments;

                    if (!trgtSegs.empty()) {
                        // find the first sst to cover
                        typename TSegments::const_iterator trgtFirstIt =
                        ::LowerBound(trgtSegs.begin(), trgtSegs.end(), firstKeyToCover, TLess());
                        if (trgtFirstIt != trgtSegs.begin()) {
                            --trgtFirstIt;
                            if ((*trgtFirstIt)->LastKey() < firstKeyToCover)
                                ++trgtFirstIt;
                        }

                        // find the next after last sst to cover
                        typename TSegments::const_iterator trgtEndIt =
                        ::UpperBound(trgtSegs.begin(), trgtSegs.end(), lastKeyToCover, TLess());
                        // FIXME: seems to be good just to call UpperBound
                        //if (trgtEndIt != trgtSegs.begin()) {
                        //    --trgtEndIt;
                        //    if (trgtEndIt->LastKey() < lastKeyToCover)
                        //        ++trgtEndIt;
                        //}

                        // put all found ssts into Vec
                        CompactSsts.PushSstFromLevelX(level, trgtFirstIt, trgtEndIt);
                        if (HullCtx->VCtx->ActorSystem) {
                            LOG_INFO_S(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP,
                                HullCtx->VCtx->VDiskLogPrefix << " TBalanceBase::FindNeighborhoods decided to compact to level# " << level
                                << " Task# " << CompactSsts.ToString());
                        }
                    } else {
                        // we don't have any ssts at level, it's fine
                    }
                } else {
                    // we don't have level, it's fine
                }
            }

        protected:
            TIntrusivePtr<THullCtx> HullCtx;
            TSublog<> &Sublog;
            TBoundariesConstPtr Boundaries;
            const TLevelSliceSnapshot &SliceSnap;
            TCompactSsts &CompactSsts;
            bool &IsFullCompaction;
        };

        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TBalanceLevel0
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TLevelSliceSnapshot, class TCompactSsts>
        class TBalanceLevel0 : public TBalanceBase<TKey, TLevelSliceSnapshot, TCompactSsts> {
        public:
            using TBase = TBalanceBase<TKey, TLevelSliceSnapshot, TCompactSsts>;
            using TSortedLevel = typename TLevelSliceSnapshot::TSortedLevel;
            using TLevelSegmentPtr = typename TLevelSliceSnapshot::TLevelSegmentPtr;
            using TSegments = typename TLevelSliceSnapshot::TSegments;

            TBalanceLevel0(
                    TIntrusivePtr<THullCtx> hullCtx,
                    TSublog<> &sublog,
                    const TBoundariesConstPtr &boundaries,
                    const TLevelSliceSnapshot &sliceSnap,
                    TCompactSsts &compactSsts,
                    bool &isFullCompaction)
                : TBase(std::move(hullCtx), sublog, boundaries, sliceSnap, compactSsts, isFullCompaction)
            {}

            double CalculateRank() const {
                const ui32 virtualLevel = 0;
                double rank = Boundaries->GetRate(virtualLevel, SliceSnap.GetLevel0ChunksNum());
                return rank;
            }

            // find empty level to put compaction result to
            ui32 FindTargetLevel() const {
                ui32 otherLevelsNum = SliceSnap.GetLevelXNumber();
                for (ui32 i = 0; i < Boundaries->SortedParts * 2; i++) {
                    if (i < otherLevelsNum) {
                        const TSortedLevel &sl = SliceSnap.GetLevelXRef(i);
                        if (sl.Empty()) {
                            return i + 1;
                        }
                    } else {
                        return i + 1;
                    }
                }
                Y_ABORT("free level not found");
                return -1;
            }

            void Compact() const {
                CompactSsts.TargetLevel = FindTargetLevel();

                // add tables for compaction
                ui32 added = 0;
                auto it = SliceSnap.GetLevel0SstIterator();
                it.SeekToFirst();
                Y_DEBUG_ABORT_UNLESS(it.Valid());
                while (it.Valid() && added < Boundaries->Level0MaxSstsAtOnce) {
                    // push to the task
                    TLevelSegmentPtr sst(it.Get());
                    CompactSsts.PushOneSst(0, sst);
                    added++;
                    it.Next();
                }
                Y_ABORT_UNLESS(added > 0);

                if (HullCtx->VCtx->ActorSystem) {
                    LOG_INFO_S(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP,
                            HullCtx->VCtx->VDiskLogPrefix << " TBalanceLevel0 decided to compact, Task# " << CompactSsts.ToString());
                }
            }

            // check and run full compaction if required
            // returns true if compaction started, false otherwise
            bool FullCompact(ui64 lsn) const {
                CompactSsts.TargetLevel = FindTargetLevel();

                // add tables for compaction
                ui32 added = 0;
                auto it = SliceSnap.GetLevel0SstIterator();
                it.SeekToFirst();
                while (it.Valid()) {
                    // push to the task
                    TLevelSegmentPtr sst(it.Get());
                    if (sst->GetLastLsn() <= lsn) {
                        CompactSsts.PushOneSst(0, sst);
                        added++;
                        if (added == Boundaries->Level0MaxSstsAtOnce) {
                            // don't compact too many ssts at once
                            break;
                        }
                    }
                    it.Next();
                }

                if (added > 0) {
                    Sublog.Log() << "TBalanceLevel0::FullCompact: added# "
                        << added << " targetLevel# " << CompactSsts.TargetLevel <<  "\n";
                    IsFullCompaction = true;
                }

                return added > 0;
            }

        private:
            using TBase::HullCtx;
            using TBase::Sublog;
            using TBase::Boundaries;
            using TBase::SliceSnap;
            using TBase::CompactSsts;
            using TBase::IsFullCompaction;
        };


        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TBalancePartiallySortedLevels
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TLevelSliceSnapshot, class TCompactSsts>
        class TBalancePartiallySortedLevels : public TBalanceBase<TKey, TLevelSliceSnapshot, TCompactSsts> {
        public:
            using TBase = TBalanceBase<TKey, TLevelSliceSnapshot, TCompactSsts>;
            using TSortedLevel = typename TLevelSliceSnapshot::TSortedLevel;
            using TLevelSegment = typename TLevelSliceSnapshot::TLevelSegment;
            using TLevelSegmentPtr = typename TLevelSliceSnapshot::TLevelSegmentPtr;
            using TSegments = typename TLevelSliceSnapshot::TSegments;

            TBalancePartiallySortedLevels(
                    TIntrusivePtr<THullCtx> hullCtx,
                    TSublog<> &sublog,
                    const TBoundariesConstPtr &boundaries,
                    const TLevelSliceSnapshot &sliceSnap,
                    TCompactSsts &compactSsts,
                    bool &isFullCompaction)
                : TBase(std::move(hullCtx), sublog, boundaries, sliceSnap, compactSsts, isFullCompaction)
            {}

            struct TRank {
                double Rank = 0.0;
                ui32 FreeLevels = 0;
            };

            TRank CalculateRank() const {
                // total partially sorted levels
                const ui32 totalPsl = Boundaries->SortedParts * 2;
                ui32 otherLevelsNum = SliceSnap.GetLevelXNumber();
                ui32 freeLevels = 0;
                for (ui32 i = 0; i < totalPsl; i++) {
                    if (i < otherLevelsNum) {
                        const TSortedLevel &sl = SliceSnap.GetLevelXRef(i);
                        freeLevels += !!sl.Empty();
                    } else {
                        freeLevels++;
                    }
                }

                if (HullCtx->VCtx->ActorSystem) {
                    LOG_DEBUG(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP,
                            VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                                "%s: TBalancePartiallySortedLevels::CalculateRank: %s, "
                                "freeLevels: %" PRIu32 ", totalPsl %" PRIu32,
                                PDiskSignatureForHullDbKey<TKey>().ToString().data(),
                                ToString().data(), freeLevels, totalPsl));
                }

                Y_ABORT_UNLESS(freeLevels <= totalPsl);
                double rank = 0.0;
                if (freeLevels == totalPsl) {
                    rank = 0.0;
                } else if (freeLevels == 0) {
                    rank = 1000000.0;
                } else {
                    double step = 1.0 / totalPsl;
                    rank = step * (totalPsl - freeLevels);
                }

                return {rank, freeLevels};
            }

            // Legend: n - no level, x - no data, D - has data
            TString ToString() const {
                ui32 otherLevelsNum = SliceSnap.GetLevelXNumber();
                ui32 freeLevels = 0;
                TStringStream str;
                for (ui32 i = 0; i < Boundaries->SortedParts * 2; i++) {
                    if (i < otherLevelsNum) {
                        const TSortedLevel &sl = SliceSnap.GetLevelXRef(i);
                        freeLevels += !!sl.Empty();
                        str << (sl.Empty() ? "x " : "D ");
                    } else {
                        freeLevels++;
                        str << "n ";
                    }
                }
                return str.Str();
            }

            void SelectSstsForCompaction(TKey *firstKeyToCover, TKey *lastKeyToCover) {
                // vector of candidates to compact, we select thoses ssts with oldest lsns
                std::vector<typename TLevelSegment::TLevelSstPtr> candidates;

                const ui32 otherLevelsNum = SliceSnap.GetLevelXNumber();
                ui32 pslSize = Min(Boundaries->SortedParts * 2, otherLevelsNum);
                candidates.reserve(pslSize);
                // fill in candidates vector
                for (ui32 i = 0; i < pslSize; ++i) {
                    const TSortedLevel &sl = SliceSnap.GetLevelXRef(i);
                    if (!sl.Empty()) {
                        TLevelSegmentPtr sst = *(sl.Segs->Segments.begin());
                        candidates.emplace_back(i + 1, sst);
                    }
                }

                // sort candidates by lsn
                auto cmp = [] (const auto &left, const auto &right) {
                    return left.SstPtr->GetLastLsn() < right.SstPtr->GetLastLsn();
                };
                std::sort(candidates.begin(), candidates.end(), cmp);

                ui32 levelsToCompact = Min(Boundaries->SortedParts, ui32(candidates.size()));
                ui32 added = 0;
                for (ui32 i = 0; i < levelsToCompact; ++i) {
                    auto &candidate = candidates[i];
                    CompactSsts.PushOneSst(candidate.Level, candidate.SstPtr);
                    if (added == 0) {
                        *firstKeyToCover = candidate.SstPtr->FirstKey();
                        *lastKeyToCover = candidate.SstPtr->LastKey();
                    } else {
                        if (candidate.SstPtr->FirstKey() < *firstKeyToCover)
                            *firstKeyToCover = candidate.SstPtr->FirstKey();
                        if (candidate.SstPtr->LastKey() > *lastKeyToCover)
                            *lastKeyToCover = candidate.SstPtr->LastKey();
                    }
                    added++;
                }
                Y_ABORT_UNLESS(added);

                if (HullCtx->VCtx->ActorSystem) {
                    LOG_INFO_S(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP,
                        HullCtx->VCtx->VDiskLogPrefix << " TBalancePartiallySortedLevels decided to compact, Task# " << CompactSsts.ToString()
                        << " firstKeyToCover# " << (firstKeyToCover ? firstKeyToCover->ToString() : "nullptr")
                        << " lastKeyToCover# " << (lastKeyToCover ? lastKeyToCover->ToString() : "nullptr")
                    );
                }
            }

            void Compact() {
                // select ssts to compact from found partLevel
                TKey firstKeyToCover;
                TKey lastKeyToCover;
                SelectSstsForCompaction(&firstKeyToCover, &lastKeyToCover);

                // find the neighborhood of the sst to compact targetLevel
                ui32 targetLevel = Boundaries->SortedParts * 2 + 1;
                CompactSsts.TargetLevel = targetLevel;
                CompactSsts.LastCompactedKey = TKey::First(); // We don't use it and don't care
                TBase::FindNeighborhoods(targetLevel, firstKeyToCover, lastKeyToCover);
            }

            // check and run full compaction if required
            // returns true if compaction started, false otherwise
            bool FullCompact(ui64 lsn) {
                // find if we have ssts with lsns less than lsn
                const ui32 otherLevelsNum = SliceSnap.GetLevelXNumber();
                ui32 pslSize = Min(Boundaries->SortedParts * 2, otherLevelsNum);
                for (ui32 i = 0; i < pslSize; ++i) {
                    const TSortedLevel &sl = SliceSnap.GetLevelXRef(i);
                    for (auto it = sl.Segs->Segments.begin(); it != sl.Segs->Segments.end(); ++it) {
                        if ((**it).GetLastLsn() <= lsn) {
                            // we have found sst we need to merge with upper level, so just run ordinary
                            // compaction; after (possibly) several iterations we eventually compact all
                            // required ssts
                            Compact();
                            Sublog.Log() << "TBalancePartiallySortedLevels::FullCompact: level# " << (i + 1) <<  "\n";
                            IsFullCompaction = true;
                            return true;
                        }
                    }
                }

                return false;
            }

        private:
            using TBase::HullCtx;
            using TBase::Sublog;
            using TBase::Boundaries;
            using TBase::SliceSnap;
            using TBase::CompactSsts;
            using TBase::IsFullCompaction;
        };


        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TBalanceLevelX
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec, class TLevelSliceSnapshot, class TCompactSsts>
        class TBalanceLevelX : public TBalanceBase<TKey, TLevelSliceSnapshot, TCompactSsts> {
        public:
            using TBase = TBalanceBase<TKey, TLevelSliceSnapshot, TCompactSsts>;
            using TSortedLevel = typename TLevelSliceSnapshot::TSortedLevel;
            using TLevelSegmentPtr = typename TLevelSliceSnapshot::TLevelSegmentPtr;
            using TSegments = typename TLevelSliceSnapshot::TSegments;
            using TUtils = ::NKikimr::NHullComp::TUtils<TKey, TMemRec>;
            using TLevelSstPtr = typename TLevelSegment<TKey, TMemRec>::TLevelSstPtr;

            TBalanceLevelX(
                    TIntrusivePtr<THullCtx> hullCtx,
                    TSublog<> &sublog,
                    const TBoundariesConstPtr &boundaries,
                    const TLevelSliceSnapshot &sliceSnap,
                    TCompactSsts &compactSsts,
                    bool &isFullCompaction)
                : TBase(std::move(hullCtx), sublog, boundaries, sliceSnap, compactSsts, isFullCompaction)
            {}

            void CalculateRanks(std::vector<double> &ranks) {
                Y_DEBUG_ABORT_UNLESS(ranks.size() == 2);
                ui32 otherLevelsNum = SliceSnap.GetLevelXNumber();
                for (ui32 i = Boundaries->SortedParts * 2; i < otherLevelsNum; i++) {
                    ui32 virtualLevel = i - Boundaries->SortedParts * 2 + 2;
                    double rank = Boundaries->GetRate(virtualLevel, SliceSnap.GetLevelXChunksNum(i));
                    ranks.push_back(rank);
                }
            }

            void Compact(const ui32 virtualLevelToCompact) {
                const ui32 srcLevel = virtualLevelToCompact - 2 + Boundaries->SortedParts * 2 + 1;

                Y_DEBUG_ABORT_UNLESS(srcLevel > 0);
                const ui32 srcLevelArrIdx = srcLevel - 1; // srcLevel=1 has index 0

                // find sst to compact
                const TSortedLevel &srcLevelData = SliceSnap.GetLevelXRef(srcLevelArrIdx);
                const TKey &lastCompactedKey = srcLevelData.LastCompactedKey;
                const TSegments &srcSegs = srcLevelData.Segs->Segments;
                Y_DEBUG_ABORT_UNLESS(!srcSegs.empty());
                typename TSegments::const_iterator srcIt = ::LowerBound(srcSegs.begin(),
                                                                        srcSegs.end(),
                                                                        lastCompactedKey,
                                                                        TLess());
                if (srcIt != srcSegs.begin()) {
                    --srcIt;
                    if ((*srcIt)->LastKey() <= lastCompactedKey) {
                        ++srcIt;
                        if (srcIt == srcSegs.end()) {
                            // we compacted the whole level, switch to beginning
                            srcIt = srcSegs.begin();
                        }
                    }
                } else {
                    // srcIt is equal to srcSegs.begin() and it's fine
                }

                Y_ABORT_UNLESS(srcIt != srcSegs.end());
                CompactSst(srcLevel, srcIt);

                if (HullCtx->VCtx->ActorSystem) {
                    LOG_INFO_S(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP,
                        HullCtx->VCtx->VDiskLogPrefix << " TBalanceLevelX decided to compact, Task# " << CompactSsts.ToString()
                        << " lastCompactedKey# " << lastCompactedKey.ToString()
                    );
                }
            }

            void CompactSst(ui32 srcLevel, typename TSegments::const_iterator srcIt) {
                Y_ABORT_UNLESS(srcLevel > 0);
                // srcIt points to the sst to compact
                TKey firstKeyToCover = (*srcIt)->FirstKey();
                TKey lastKeyToCover = (*srcIt)->LastKey();

                if (HullCtx->VCtx->ActorSystem) {
                    LOG_INFO_S(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP,
                        HullCtx->VCtx->VDiskLogPrefix << " TBalanceLevelX: take sst with firstKeyToCover#" << firstKeyToCover.ToString()
                        << " lastKeyToCover# " << lastKeyToCover.ToString()
                    );
                }

                // put this sst to the vector
                CompactSsts.TargetLevel = srcLevel + 1;
                CompactSsts.PushSstFromLevelX(srcLevel, srcIt, srcIt + 1);
                CompactSsts.LastCompactedKey = lastKeyToCover;

                // find the neighborhood of the sst to compact at targetLevel
                TBase::FindNeighborhoods(srcLevel + 1, firstKeyToCover, lastKeyToCover);
            }

            // check and run full compaction if required
            // returns true if compaction started, false otherwise
            bool FullCompact(const TFullCompactionAttrs &attrs) {
                const ui32 otherLevelsNum = SliceSnap.GetLevelXNumber();
                if (otherLevelsNum == 0) {
                    // empty database, we are done
                    return false;
                }

                // find if we have ssts with lsns less than lsn on all levels except the last one
                ui32 i = Boundaries->SortedParts * 2;
                for (; i < otherLevelsNum - 1; ++i) {
                    const ui32 srcLevelArrIdx = i; // level=1 has index 0
                    const ui32 srcLevel = i + 1;
                    const TSortedLevel &srcLevelData = SliceSnap.GetLevelXRef(srcLevelArrIdx);
                    if (!srcLevelData.Empty()) {
                        const TSegments &srcSegs = srcLevelData.Segs->Segments;
                        Y_ABORT_UNLESS(!srcSegs.empty());
                        for (typename TSegments::const_iterator it = srcSegs.begin(); it != srcSegs.end(); ++it) {
                            if ((*it)->GetLastLsn() <= attrs.FullCompactionLsn) {
                                Sublog.Log() << "TBalanceLevelX::FullCompact: srcLevel# " << srcLevel
                                    << " sstsAtThisLevel# " << srcSegs.size()
                                    << " otherLevelsNum# " << otherLevelsNum << "\n";
                                CompactSst(srcLevel, it);
                                IsFullCompaction = true;
                                return true;
                            }
                        }
                    }
                }

                // last level is left
                if (i < otherLevelsNum) {
                    // if the level is present
                    const ui32 srcLevelArrIdx = i; // level=1 has index 0
                    const ui32 srcLevel = i + 1;
                    const TSortedLevel &srcLevelData = SliceSnap.GetLevelXRef(srcLevelArrIdx);
                    if (!srcLevelData.Empty()) {
                        const TSegments &srcSegs = srcLevelData.Segs->Segments;
                        Y_ABORT_UNLESS(!srcSegs.empty());
                        for (typename TSegments::const_iterator it = srcSegs.begin(); it != srcSegs.end(); ++it) {
                            // for the last level we add a condition that sst is subject for compaction if
                            // it was built before full compaction was started
                            if ((*it)->GetLastLsn() <= attrs.FullCompactionLsn
                                && ((*it)->Info.CTime < attrs.CompactionStartTime)) {

                                Sublog.Log() << "TBalanceLevelX::FullCompact: srcLevel# " << srcLevel
                                    << " sstsAtThisLevel# " << srcSegs.size() << "\n";

                                TUtils::SqueezeOneSst(SliceSnap, TLevelSstPtr(srcLevel, *it), CompactSsts);
                                IsFullCompaction = true;
                                return true;
                            }
                        }
                    }
                }

                return false;
            }

        private:
            using TBase::HullCtx;
            using TBase::Sublog;
            using TBase::Boundaries;
            using TBase::SliceSnap;
            using TBase::CompactSsts;
            using TBase::IsFullCompaction;
            using typename TBase::TLess;
        };


        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TStrategyBalance
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec>
        class TStrategyBalance {
        public:
            using TLevelIndexSnapshot = ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
            using TLevelSliceSnapshot = ::NKikimr::TLevelSliceSnapshot<TKey, TMemRec>;
            using TSortedLevel = typename TLevelSliceSnapshot::TSortedLevel;
            using TLevelSegmentPtr = typename TLevelSliceSnapshot::TLevelSegmentPtr;
            using TSegments = typename TLevelSliceSnapshot::TSegments;
            using TTask = ::NKikimr::NHullComp::TTask<TKey, TMemRec>;
            using TBalanceLevel0 = ::NKikimr::NHullComp::TBalanceLevel0<TKey,
                    TLevelSliceSnapshot, typename TTask::TCompactSsts>;
            using TBalancePartiallySortedLevels = ::NKikimr::NHullComp::TBalancePartiallySortedLevels<TKey,
                    TLevelSliceSnapshot, typename TTask::TCompactSsts>;
            using TBalanceLevelX = ::NKikimr::NHullComp::TBalanceLevelX<TKey, TMemRec,
                    TLevelSliceSnapshot, typename TTask::TCompactSsts>;

            TStrategyBalance(
                    TIntrusivePtr<THullCtx> hullCtx,
                    const TSelectorParams &params,
                    const TLevelIndexSnapshot &levelSnap,
                    TTask *task)
                : HullCtx(std::move(hullCtx))
                , LevelSnap(levelSnap)
                , Task(task)
                , RankThreshold(params.RankThreshold)
                , FullCompactionAttrs(params.FullCompactionAttrs)
                , Sublog({})
                , BalanceLevel0(HullCtx, Sublog, params.Boundaries, LevelSnap.SliceSnap, Task->CompactSsts,
                        Task->IsFullCompaction)
                , BalancePartiallySortedLevels(HullCtx, Sublog, params.Boundaries, LevelSnap.SliceSnap,
                        Task->CompactSsts, Task->IsFullCompaction)
                , BalanceLevelX(HullCtx, Sublog, params.Boundaries, LevelSnap.SliceSnap, Task->CompactSsts,
                        Task->IsFullCompaction)
            {}

            EAction Select() {
                TRanks ranks;
                TInstant startTime(TAppData::TimeProvider->Now());
                EAction action = BalanceLevelsTree(ranks);
                if (action != ActNothing) {
                    Task->SetupAction(action);
                }

                TInstant finishTime(TAppData::TimeProvider->Now());
                if (HullCtx->VCtx->ActorSystem) {
                    LOG_LOG(*HullCtx->VCtx->ActorSystem, action == ActNothing ? NLog::PRI_DEBUG : NLog::PRI_INFO,
                            NKikimrServices::BS_HULLCOMP,
                            VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                                "%s: Balance: action# %s timeSpent# %s RankThreshold# %e ranks# %s",
                                PDiskSignatureForHullDbKey<TKey>().ToString().data(),
                                ActionToStr(action), (finishTime - startTime).ToString().data(),
                                RankThreshold, ranks.ToString().data()));
                }

                return action;
            }

        private:
            TIntrusivePtr<THullCtx> HullCtx;
            const TLevelIndexSnapshot &LevelSnap;
            TTask *Task;
            const double RankThreshold;
            const std::optional<TFullCompactionAttrs> FullCompactionAttrs;
            TSublog<> Sublog;
            TBalanceLevel0 BalanceLevel0;
            TBalancePartiallySortedLevels BalancePartiallySortedLevels;
            TBalanceLevelX BalanceLevelX;

            struct TRanks : public std::vector<double> {
                ui32 FreePartiallySortedLevelsNum = 0;
                ui32 VirtualLevelToCompact = 0;

                TRanks() {
                    reserve(8);
                }

                TString ToString() const {
                    TStringStream str;
                    str  << "{VirtualLevelToCompact# " << VirtualLevelToCompact
                    << " FreePartiallySortedLevelsNum# " << FreePartiallySortedLevelsNum
                    << " Ranks# ";
                    for (const auto &x : *this)
                        str << " " << x;
                    str << "}";
                    return str.Str();
                }
            };

            EAction BalanceLevelsTree(TRanks &ranks) {
                // calculate ranks
                ranks.push_back(BalanceLevel0.CalculateRank());
                auto pslRank = BalancePartiallySortedLevels.CalculateRank();
                ranks.push_back(pslRank.Rank);
                ranks.FreePartiallySortedLevelsNum = pslRank.FreeLevels;
                BalanceLevelX.CalculateRanks(ranks);

                // find level to compact
                Y_DEBUG_ABORT_UNLESS(!ranks.empty());
                ranks.VirtualLevelToCompact = 0;
                double maxRank = ranks[0];
                for (ui32 i = 1; i < ranks.size(); i++) {
                    if (ranks[i] > maxRank) {
                        maxRank = ranks[i];
                        ranks.VirtualLevelToCompact = i;
                    }
                }

                // fill in compaction task or do nothing
                if (maxRank < RankThreshold) {
                    if (FullCompactionAttrs) {
                        if (BalanceLevel0.FullCompact(FullCompactionAttrs->FullCompactionLsn)) {
                            if (HullCtx->VCtx->ActorSystem) {
                                LOG_INFO_S(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP,
                                    HullCtx->VCtx->VDiskLogPrefix << " TStrategyBalance decided to full compact compact level 0");
                            }
                            return ActCompactSsts;
                        }

                        if (BalancePartiallySortedLevels.FullCompact(FullCompactionAttrs->FullCompactionLsn)) {
                            if (HullCtx->VCtx->ActorSystem) {
                                LOG_INFO_S(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP,
                                        HullCtx->VCtx->VDiskLogPrefix << " TStrategyBalance decided to full compact compact sorted level");
                            }
                            return ActCompactSsts;
                        }

                        if (BalanceLevelX.FullCompact(*FullCompactionAttrs)) {
                            if (HullCtx->VCtx->ActorSystem) {
                                LOG_INFO_S(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP,
                                        HullCtx->VCtx->VDiskLogPrefix << " TStrategyBalance decided to full compact compact level x");
                            }
                            return ActCompactSsts;
                        }

                        // mark that full compaction has been finished
                        Task->FullCompactionInfo.second = true;
                        return ActNothing;
                    } else {
                        // nothing to merge, try later
                        return ActNothing;
                    }
                } else {
                    if (HullCtx->VCtx->ActorSystem) {
                        LOG_INFO_S(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP,
                            HullCtx->VCtx->VDiskLogPrefix << " TStrategyBalance decided to compact, ranks# " << ranks.ToString());
                    }
                    switch (ranks.VirtualLevelToCompact) {
                        case 0:     BalanceLevel0.Compact(); break;
                        case 1:     BalancePartiallySortedLevels.Compact(); break;
                        default:    BalanceLevelX.Compact(ranks.VirtualLevelToCompact);
                    }
                    return ActCompactSsts;
                }

                Y_ABORT("impossible case");
            }
        };

    } // NHullComp
} // NKikimr
