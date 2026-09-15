#pragma once

#include "defs.h"
#include "hulldb_compstrat_defs.h"
#include "hulldb_compstrat_utils.h"

namespace NKikimr::NHullComp {

    template<typename TKey, typename TMemRec>
    class TStrategyExplicit {
        using TLevelIndexSnapshot = NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
        using TLevelSegment = NKikimr::TLevelSegment<TKey, TMemRec>;
        using TTask = NHullComp::TTask<TKey, TMemRec>;
        using TUtils = NHullComp::TUtils<TKey, TMemRec>;
        using TLevelSstPtr = typename TLevelSegment::TLevelSstPtr;

        TIntrusivePtr<THullCtx> HullCtx;
        const TSelectorParams& Params;
        TLevelIndexSnapshot& LevelSnap;
        TTask* const Task;

    public:
        TStrategyExplicit(TIntrusivePtr<THullCtx> hullCtx, const TSelectorParams& params, TLevelIndexSnapshot& levelSnap,
                TTask *task)
            : HullCtx(std::move(hullCtx))
            , Params(params)
            , LevelSnap(levelSnap)
            , Task(task)
        {}

        EAction Select() {
            auto& [attrs, done] = Task->FullCompactionInfo;
            if (!attrs) {
                return ActNothing;
            }

            auto& sstIds = attrs->TablesToCompact;
            if (sstIds.empty()) {
                return ActNothing;
            }

            // Decide what to take before touching the task: an explicit request may be
            // larger than the output this VDisk is allowed to allocate, and a job that
            // cannot reserve its output is worse than no job at all -- it aborts, gets
            // reselected and gets nowhere.
            std::optional<ui32> levelOfInterest;
            std::vector<TLevelSstPtr> selected;
            std::vector<TLevelSstPtr> pending;
            ui64 keepBytes = 0;
            bool sawRequestedSst = false;
            bool trimmed = false;

            auto& slice = LevelSnap.SliceSnap;
            typename TLevelSliceSnapshot<TKey, TMemRec>::TSstIterator iter(&slice);
            for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
                const ui32 level = iter.Get().Level;

                if (levelOfInterest && *levelOfInterest != level) {
                    break; // going to another level, no need
                } else if (sstIds.contains(iter.Get().SstPtr->AssignedSstId)) {
                    sawRequestedSst = true;
                    if (!levelOfInterest) {
                        levelOfInterest.emplace(level);
                    }

                    // Everything queued since the previous match comes along, so that the
                    // ssts handed to the compaction stay contiguous within the level.
                    pending.push_back(iter.Get());
                    ui64 batchBytes = 0;
                    for (const auto& item : pending) {
                        batchBytes += TUtils::SstKeepBytes(*item.SstPtr);
                    }

                    if (!FitsBudget(keepBytes + batchBytes)) {
                        // Compact what fits and leave the rest in TablesToCompact: the
                        // request is not reported as done, so the next selection picks up
                        // where this one stopped, once there is room for it.
                        trimmed = true;
                        break;
                    }

                    keepBytes += batchBytes;
                    for (auto& item : pending) {
                        selected.push_back(item);
                    }
                    pending.clear();
                } else if (levelOfInterest && *levelOfInterest) {
                    pending.push_back(iter.Get());
                }
            }

            if (!sawRequestedSst) {
                // None of the requested ssts is in the index any more: the work is done.
                done = true;
                return ActNothing;
            }

            if (selected.empty()) {
                // Not even the first sst fits. Yield so that a budgeted emergency
                // compaction can reclaim something first; the request stays pending.
                if (HullCtx->VCtx->ActorSystem) {
                    YDB_LOG_INFO_CTX_COMP(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP,
                        "TStrategyExplicit yields: estimated output exceeds the free-chunk budget",
                        {"VDiskLogPrefix", HullCtx->VCtx->VDiskLogPrefix},
                        {"level", *levelOfInterest},
                        {"freeChunksBudget", Params.FreeChunksBudget});
                }
                return ActNothing;
            }

            Task->SetupAction(ActCompactSsts);
            auto& compact = Task->CompactSsts;
            compact.TargetLevel = *levelOfInterest;
            if (!*levelOfInterest) {
                // try to find new sorted level for these tables
                const size_t numSortedLevels = slice.GetLevelXNumber();
                for (ui32 i = 0, max = Params.Boundaries->SortedParts * 2; i < max; ++i) {
                    if (i == numSortedLevels || slice.GetLevelXRef(i).Empty()) {
                        compact.TargetLevel = i + 1; // found new empty level for the SST
                        break;
                    }
                }
            }

            for (const auto& item : selected) {
                compact.TablesToDelete.PushBack(item); // removing this one table

                if (auto& chains = compact.CompactionChains; chains.empty() || !*levelOfInterest) {
                    chains.push_back(new TOrderedLevelSegments(item.SstPtr));
                } else {
                    Y_DEBUG_ABORT_UNLESS(chains.size() == 1 && *levelOfInterest);
                    chains.back()->Segments.push_back(item.SstPtr);
                }
            }

            Y_DEBUG_ABORT_UNLESS(!compact.CompactionChains.empty());
            Y_DEBUG_ABORT_UNLESS(!compact.TablesToDelete.Empty());

            if (HullCtx->VCtx->ActorSystem) {
                YDB_LOG_INFO_CTX_COMP(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP, "TStrategyExplicit decided to compact level",
                    {"VDiskLogPrefix", HullCtx->VCtx->VDiskLogPrefix},
                    {"levelOfInterest", *levelOfInterest},
                    {"trimmedToBudget", trimmed},
                    {"freeChunksBudget", Params.FreeChunksBudget},
                    {"task", (Task ? Task->ToString() : "nullptr")});
            }
            return ActCompactSsts;
        }

    private:
        // The budget is the chunks PDisk will still let this owner allocate; the default
        // is unbounded, for the case where no space observation has arrived yet.
        bool FitsBudget(ui64 keepBytes) const {
            if (Params.FreeChunksBudget == Max<ui32>()) {
                return true;
            }
            return TUtils::EstimateOutputChunks(keepBytes, HullCtx->ChunkSize) <= Params.FreeChunksBudget;
        }
    };

} // NKikimr::NHullComp
