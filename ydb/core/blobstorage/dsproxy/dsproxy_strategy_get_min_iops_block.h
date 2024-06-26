#pragma once

#include "dsproxy_strategy_base.h"
#include "dsproxy_blackboard.h"

namespace NKikimr {

class TMinIopsBlockStrategy : public TStrategyBase {
public:
    std::optional<EStrategyOutcome> RestoreWholeFromDataParts(TLogContext& /*logCtx*/, TBlobState &state,
            const TBlobStorageGroupInfo &info) {
        for (auto [wbegin, wend] : state.Whole.NotHere()) {
            TBlockSplitRange r;
            info.Type.BlockSplitRange((TErasureType::ECrcMode)state.Id.CrcMode(), state.Id.BlobSize(), wbegin, wend, &r);
            for (ui32 i = r.BeginPartIdx; i < r.EndPartIdx; ++i) {
                Y_ABORT_UNLESS(i < info.Type.DataParts());
                TPartOffsetRange& pr = r.PartRanges[i];
                if (pr.Begin == pr.End) {
                    continue; // this part has no data
                }
                Y_DEBUG_ABORT_UNLESS(pr.AlignedBegin <= pr.Begin && pr.End <= pr.AlignedEnd);
                const TIntervalSet<i32> partRange(pr.Begin, pr.End);
                state.Whole.Data.CopyFrom(state.Parts[i].Data, state.Parts[i].Here() & partRange, pr.WholeBegin - pr.Begin);
            }
        }

        if (state.Whole.NotHere()) {
            return {};
        }

        state.WholeSituation = TBlobState::ESituation::Present;
        return EStrategyOutcome::DONE;
    }

    void RestoreWhateverDataPartsPossible(TBlobState& state, const TBlobStorageGroupInfo& info) {
        const ui32 totalPartCount = info.Type.TotalPartCount();

        struct THeapItem {
            ui32 PartIdx;
            TIntervalSet<i32> Interval;

            std::tuple<ui32, ui32> GetRange() const {
                Y_DEBUG_ABORT_UNLESS(Interval);
                return *Interval.begin();
            }

            bool operator <(const THeapItem& x) const { return x.GetRange() < GetRange(); }
        };

        TStackVec<THeapItem, TypicalPartsInBlob> heap;
        for (ui32 i = 0; i < totalPartCount; ++i) {
            if (TIntervalSet<i32> here = state.Parts[i].Here()) {
                heap.push_back(THeapItem{i, std::move(here)});
            }
        }
        std::make_heap(heap.begin(), heap.end());
        while (heap) {
            ui32 alignedBegin = 0;
            ui32 alignedEnd = 0;

            // collect all intervals with this common beginning aligned offset
            auto iter = heap.end();
            while (iter != heap.begin()) {
                auto [iterBegin, iterEnd] = heap.front().GetRange();
                const ui32 minBlockSize = info.Type.MinimalBlockSize() / info.Type.DataParts();
                // align beginning offset up and ending offset down
                iterBegin += minBlockSize - 1;
                iterBegin -= iterBegin % minBlockSize;
                iterEnd -= iterEnd % minBlockSize;
                if (iter != heap.end() && alignedBegin != iterBegin) {
                    alignedEnd = Min(alignedEnd, iterBegin);
                    break;
                }
                std::tie(alignedBegin, alignedEnd) = std::make_tuple(iterBegin,
                    Min(iter != heap.end() ? alignedEnd : Max<ui32>(), iterEnd));
                std::pop_heap(heap.begin(), iter--);
            }

            // [iter, heap.end()) range now contains all parts that we may possibly include in restoration; do it
            if (heap.end() - iter >= info.Type.MinimalRestorablePartCount() && alignedBegin != alignedEnd) {
                ui32 restoreMask = (1 << info.Type.DataParts()) - 1;

                // fetch some data parts
                const TIntervalSet<i32> alignedInterval(alignedBegin, alignedEnd);
                TStackVec<TRope, TypicalPartsInBlob> parts(totalPartCount);
                for (auto i = iter; i != heap.end(); ++i) {
                    Y_ABORT_UNLESS(alignedInterval.IsSubsetOf(state.Parts[i->PartIdx].Here()));
                    parts[i->PartIdx] = state.Parts[i->PartIdx].Data.Read(alignedBegin, alignedEnd - alignedBegin);
                    restoreMask &= ~(1 << i->PartIdx); // don't restore part we already have
                }

                if (restoreMask) {
                    // restore missing data parts
                    ErasureRestore((TErasureType::ECrcMode)state.Id.CrcMode(), info.Type, state.Id.BlobSize(),
                        nullptr, parts, restoreMask, alignedBegin, true);

                    // put them back
                    for (ui32 i = 0; i < info.Type.DataParts(); ++i) {
                        if (restoreMask >> i & 1) {
                            Y_VERIFY_DEBUG_S(alignedBegin + parts[i].size() == alignedEnd, "alignedBegin# " << alignedBegin
                                    << " alignedEnd# " << alignedEnd << " partSize# " << parts[i].size());
                            state.Parts[i].Data.Write(alignedBegin, std::move(parts[i]));
                        }
                    }
                }
            }

            // return items back to heap
            TIntervalSet<i32> interval(0, alignedEnd);
            while (iter != heap.end()) {
                iter->Interval -= interval;
                if (iter->Interval) {
                    std::push_heap(heap.begin(), ++iter);
                } else {
                    std::swap(*iter, heap.back());
                    heap.pop_back();
                }
            }
        }
    }

    std::optional<EStrategyOutcome> RestoreWholeWithErasure(TLogContext& logCtx, TBlobState &state,
            const TBlobStorageGroupInfo &info) {
        RestoreWhateverDataPartsPossible(state, info);
        return RestoreWholeFromDataParts(logCtx, state, info);
    }

    void IssueGetRequestsForMinimal(TLogContext &logCtx, TBlobState &state, const TBlobStorageGroupInfo &info,
            bool considerSlowAsError, TGroupDiskRequests &groupDiskRequests) {
        const ui32 totalPartCount = info.Type.TotalPartCount();
        const i32 handoff = info.Type.Handoff();
        for (auto [begin, end] : state.Whole.NotHere()) {
            TBlockSplitRange range;
            info.Type.BlockSplitRange((TErasureType::ECrcMode)state.Id.CrcMode(), state.Id.BlobSize(),
                    begin, end, &range);
            for (ui32 partIdx = range.BeginPartIdx; partIdx < range.EndPartIdx; ++partIdx) {
                TPartOffsetRange &partRange = range.PartRanges[partIdx];
                if (partRange.Begin != partRange.End) {
                    TIntervalSet<i32> partInterval(partRange.AlignedBegin, partRange.AlignedEnd);
                    partInterval.Subtract(state.Parts[partIdx].Here());
                    if (!partInterval.IsEmpty()) {
                        for (i32 niche = -1; niche < handoff; ++niche) {
                            ui32 diskIdx = (niche < 0 ? partIdx : totalPartCount + niche);
                            TBlobState::TDisk &disk = state.Disks[diskIdx];
                            if (!considerSlowAsError || !disk.IsSlow) {
                                TBlobState::ESituation partSituation = disk.DiskParts[partIdx].Situation;
                                if (partSituation == TBlobState::ESituation::Unknown ||
                                        partSituation == TBlobState::ESituation::Present) {
                                    TIntervalSet<i32> unrequestedInterval(partInterval);
                                    unrequestedInterval.Subtract(disk.DiskParts[partIdx].Requested);
                                    if (!unrequestedInterval.IsEmpty()) {
                                        AddGetRequest(logCtx, groupDiskRequests, state.Id, partIdx, disk,
                                                unrequestedInterval, "BPG46");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    bool IsMinimalPossible(TBlobState &state, const TBlobStorageGroupInfo &info, bool considerSlowAsError) {
        const ui32 totalPartCount = info.Type.TotalPartCount();
        const i32 handoff = info.Type.Handoff();
        bool isMinimalPossible = true;
        for (auto [begin, end] : state.Whole.NotHere()) {
            TBlockSplitRange range;
            info.Type.BlockSplitRange((TErasureType::ECrcMode)state.Id.CrcMode(), state.Id.BlobSize(), begin,
                    end, &range);
            for (ui32 partIdx = range.BeginPartIdx; partIdx < range.EndPartIdx; ++partIdx) {
                TPartOffsetRange &partRange = range.PartRanges[partIdx];
                if (partRange.Begin != partRange.End) {
                    TIntervalVec<i32> partInterval(partRange.AlignedBegin, partRange.AlignedEnd);
                    if (!partInterval.IsSubsetOf(state.Parts[partIdx].Here())) {
                        bool isThereAGoodPart = false;
                        for (i32 niche = -1; niche < handoff; ++niche) {
                            ui32 diskIdx = (niche < 0 ? partIdx : totalPartCount + niche);
                            TBlobState::TDisk &disk = state.Disks[diskIdx];
                            if (!considerSlowAsError || !disk.IsSlow) {
                                TBlobState::ESituation partSituation = disk.DiskParts[partIdx].Situation;
                                if (partSituation == TBlobState::ESituation::Unknown ||
                                        partSituation == TBlobState::ESituation::Present) {
                                    isThereAGoodPart = true;
                                }
                            }
                        }
                        if (!isThereAGoodPart) {
                            isMinimalPossible = false;
                        }
                    }
                }
            }
        }
        return isMinimalPossible;
    }

    void FillIntervalsToRestoreRequestedFullData(TBlobState &state, const TBlobStorageGroupInfo &info,
            TIntervalSet<i32> &outToRestore) {
        // get intervals needed to restore the requested full data
        for (auto [begin, end] : state.Whole.NotHere()) {
            TBlockSplitRange range;
            info.Type.BlockSplitRange((TErasureType::ECrcMode)state.Id.CrcMode(), state.Id.BlobSize(),
                    begin, end, &range);
            for (ui32 partIdx = range.BeginPartIdx; partIdx < range.EndPartIdx; ++partIdx) {
                TPartOffsetRange &partRange = range.PartRanges[partIdx];
                if (partRange.Begin != partRange.End) {
                    outToRestore.Add(partRange.AlignedBegin, partRange.AlignedEnd);
                }
            }
        }
    }

    ui32 CountPartsMissing(TBlobState &state, const TBlobStorageGroupInfo &info, bool considerSlowAsError) {
        const ui32 totalPartCount = info.Type.TotalPartCount();
        const i32 handoff = info.Type.Handoff();
        ui32 partsMissing = 0;
        for (ui32 partIdx = 0; partIdx < totalPartCount; ++partIdx) {
            bool isMissing = true;
            for (i32 niche = -1; niche < handoff; ++niche) {
                ui32 diskIdx = (niche < 0 ? partIdx : totalPartCount + niche);
                TBlobState::TDisk &disk = state.Disks[diskIdx];
                if (!considerSlowAsError || !disk.IsSlow) {
                    TBlobState::ESituation partSituation = disk.DiskParts[partIdx].Situation;
                    if (partSituation != TBlobState::ESituation::Error &&
                            partSituation != TBlobState::ESituation::Absent &&
                            partSituation != TBlobState::ESituation::Lost) {
                        isMissing = false;
                    }
                }
            }
            if (isMissing) {
                partsMissing++;
            }
        }
        return partsMissing;
    }

    void IssueGetRequestsForRestoration(TLogContext &logCtx, TBlobState &state, const TBlobStorageGroupInfo &info,
            bool considerSlowAsError, TGroupDiskRequests &groupDiskRequests) {
        TIntervalSet<i32> toRestore;
        FillIntervalsToRestoreRequestedFullData(state, info, toRestore);

        ui32 partsMissing = CountPartsMissing(state, info, considerSlowAsError);
        Y_ABORT_UNLESS(partsMissing > 0);
        const ui32 totalPartCount = info.Type.TotalPartCount();
        const ui32 dataParts = info.Type.DataParts();
        //(idx < partsMissing);
        for (ui32 diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
            bool isHandoff = (diskIdx >= totalPartCount);
            ui32 beginPartIdx = (isHandoff ? 0 : diskIdx);
            ui32 endPartIdx = (isHandoff ? totalPartCount : (diskIdx + 1));
            endPartIdx = Min(endPartIdx, dataParts + partsMissing);
            for (ui32 partIdx = beginPartIdx; partIdx < endPartIdx; ++partIdx) {
                TBlobState::TDisk &disk = state.Disks[diskIdx];
                if (!considerSlowAsError || !disk.IsSlow) {
                    TBlobState::ESituation partSituation = disk.DiskParts[partIdx].Situation;
                    if (partSituation == TBlobState::ESituation::Unknown ||
                            partSituation == TBlobState::ESituation::Present) {
                        TIntervalSet<i32> partIntervals(toRestore);
                        partIntervals.Subtract(state.Parts[partIdx].Here());
                        partIntervals.Subtract(disk.DiskParts[partIdx].Requested);
                        if (!partIntervals.IsEmpty()) {
                            // TODO(cthulhu): consider the case when we just need to know that there is a copy
                            //                and make an index request to avoid the data transfer
                            //
                            //                actually consider that we dont need to prove anything if we can
                            //                read the data
                            AddGetRequest(logCtx, groupDiskRequests, state.Id, partIdx, disk,
                                    partIntervals, "BPG47");
                        }
                    }
                }
            }
        }
    }

    void IssueGetRequests(TLogContext &logCtx, TBlobState &state, const TBlobStorageGroupInfo &info,
            bool considerSlowAsError, TGroupDiskRequests &groupDiskRequests) {
        // Request minimal parts for each requested range of each blob
        bool isMinimalPossible = IsMinimalPossible(state, info, considerSlowAsError);
        if (isMinimalPossible) {
            IssueGetRequestsForMinimal(logCtx, state, info, considerSlowAsError, groupDiskRequests);
        } else {
            IssueGetRequestsForRestoration(logCtx, state, info, considerSlowAsError, groupDiskRequests);
        }
    }

    EStrategyOutcome Process(TLogContext &logCtx, TBlobState &state, const TBlobStorageGroupInfo &info,
            TBlackboard& blackboard, TGroupDiskRequests &groupDiskRequests, float slowDiskThreshold) override {
        if (auto res = RestoreWholeFromDataParts(logCtx, state, info)) {
            return *res;
        } else if (auto res = RestoreWholeWithErasure(logCtx, state, info)) {
            return *res;
        }
        // Look at the current layout and set the status if possible
        TBlobStorageGroupInfo::EBlobState pessimisticState = TBlobStorageGroupInfo::EBS_DISINTEGRATED;
        TBlobStorageGroupInfo::EBlobState optimisticState = TBlobStorageGroupInfo::EBS_DISINTEGRATED;
        TBlobStorageGroupInfo::EBlobState altruisticState = TBlobStorageGroupInfo::EBS_DISINTEGRATED;
        EvaluateCurrentLayout(logCtx, state, info, &pessimisticState, &optimisticState, &altruisticState, false);

        if (auto res = SetAbsentForUnrecoverableAltruistic(altruisticState, state)) {
            return *res;
        } else if (auto res = ProcessOptimistic(altruisticState, optimisticState, false, state)) {
            return *res;
        } else if (auto res = ProcessPessimistic(info, pessimisticState, false, state)) {
            return *res;
        }

        // Try excluding the slow disk
        bool isDone = false;
        // TODO: Mark disk that does not answer when accelerating requests
        ui32 slowDiskSubgroupMask = MakeSlowSubgroupDiskMask(state, info, blackboard, false, slowDiskThreshold);
        if (slowDiskSubgroupMask >= 0) {
            TBlobStorageGroupInfo::EBlobState fastPessimisticState = TBlobStorageGroupInfo::EBS_DISINTEGRATED;
            TBlobStorageGroupInfo::EBlobState fastOptimisticState = TBlobStorageGroupInfo::EBS_DISINTEGRATED;
            TBlobStorageGroupInfo::EBlobState fastAltruisticState = TBlobStorageGroupInfo::EBS_DISINTEGRATED;
            EvaluateCurrentLayout(logCtx, state, info, &fastPessimisticState, &fastOptimisticState,
                    &fastAltruisticState, true);
            if (!IsUnrecoverableAltruistic(fastAltruisticState)
                    && !ProcessOptimistic(fastAltruisticState, fastOptimisticState, true, state)) {
                IssueGetRequests(logCtx, state, info, true, groupDiskRequests);
                isDone = true;
            }
        }
        if (!isDone) {
            IssueGetRequests(logCtx, state, info, false, groupDiskRequests);
        }

        return EStrategyOutcome::IN_PROGRESS;
    }
};

}//NKikimr
