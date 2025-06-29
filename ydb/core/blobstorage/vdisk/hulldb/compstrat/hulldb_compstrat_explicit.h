#pragma once

#include "defs.h"
#include "hulldb_compstrat_defs.h"

namespace NKikimr::NHullComp {

    template<typename TKey, typename TMemRec>
    class TStrategyExplicit {
        using TLevelIndexSnapshot = NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
        using TLevelSegment = NKikimr::TLevelSegment<TKey, TMemRec>;
        using TTask = NHullComp::TTask<TKey, TMemRec>;

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

            std::optional<ui32> levelOfInterest;
            auto& compact = Task->CompactSsts;
            std::vector<typename TLevelSegment::TLevelSstPtr> pending;

            auto& slice = LevelSnap.SliceSnap;
            typename TLevelSliceSnapshot<TKey, TMemRec>::TSstIterator iter(&slice);
            for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
                const ui32 level = iter.Get().Level;

                if (levelOfInterest && *levelOfInterest != level) {
                    break; // going to another level, no need
                } else if (sstIds.contains(iter.Get().SstPtr->AssignedSstId)) {
                    if (!levelOfInterest) {
                        Task->SetupAction(ActCompactSsts);
                        levelOfInterest.emplace(level);
                        compact.TargetLevel = level;
                        if (!level) {
                            // try to find new sorted level for these tables
                            const size_t numSortedLevels = slice.GetLevelXNumber();
                            for (ui32 i = 0, max = Params.Boundaries->SortedParts * 2; i < max; ++i) {
                                if (i == numSortedLevels || slice.GetLevelXRef(i).Empty()) {
                                    compact.TargetLevel = i + 1; // found new empty level for the SST
                                    break;
                                }
                            }
                        }
                    }

                    pending.push_back(iter.Get());
                    for (const auto& item : pending) {
                        compact.TablesToDelete.PushBack(item); // removing this one table

                        if (auto& chains = compact.CompactionChains; chains.empty() || !*levelOfInterest) {
                            chains.push_back(new TOrderedLevelSegments(item.SstPtr));
                        } else {
                            Y_DEBUG_ABORT_UNLESS(chains.size() == 1 && *levelOfInterest);
                            chains.back()->Segments.push_back(item.SstPtr);
                        }
                    }
                    pending.clear();
                } else if (levelOfInterest && *levelOfInterest) {
                    pending.push_back(iter.Get());
                }
            }

            Y_DEBUG_ABORT_UNLESS(levelOfInterest.has_value() == !compact.CompactionChains.empty());
            Y_DEBUG_ABORT_UNLESS(levelOfInterest.has_value() == !compact.TablesToDelete.Empty());

            if (levelOfInterest) {
                if (HullCtx->VCtx->ActorSystem) {
                    LOG_INFO_S(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP,
                        HullCtx->VCtx->VDiskLogPrefix << "TStrategyExplicit decided to compact level" << *levelOfInterest
                        << " task# " << (Task ? Task->ToString() : "nullptr"));
                }
                return ActCompactSsts;
            } else {
                done = true;
                return ActNothing;
            }
        }
    };

} // NKikimr::NHullComp
