#pragma once

#include "dsproxy_strategy_base.h"
#include "dsproxy_blackboard.h"

namespace NKikimr {

class TMinIopsBlockStrategy : public TStrategyBase {
public:
    std::optional<EStrategyOutcome> RestoreWholeFromDataParts(TLogContext& /*logCtx*/, TBlobState &state,
            const TBlobStorageGroupInfo &info) {
        TIntervalSet<i32> missing(state.Whole.NotHere);
        TString tmp;
        for (auto [begin, end] : missing) {
            TBlockSplitRange range;
            info.Type.BlockSplitRange((TErasureType::ECrcMode)state.Id.CrcMode(), state.Id.BlobSize(), begin,
                    end, &range);
            for (ui32 partIdx = range.BeginPartIdx; partIdx < range.EndPartIdx; ++partIdx) {
                TPartOffsetRange &partRange = range.PartRanges[partIdx];
                if (partRange.Begin != partRange.End && partRange.WholeBegin < partRange.WholeEnd) {
                    if (!state.Parts[partIdx].Here.IsEmpty()) {
                        TIntervalVec<i32> partInterval(partRange.Begin, partRange.End);
                        if (partInterval.IsSubsetOf(state.Parts[partIdx].Here)) {
                            i64 sizeToCopy = partRange.End - partRange.Begin;
                            if (tmp.size() < (ui64)sizeToCopy) {
                                tmp = TString::Uninitialized(sizeToCopy);
                            }
                            state.Parts[partIdx].Data.Read(partRange.Begin, const_cast<char*>(tmp.data()), sizeToCopy);
                            Y_VERIFY(partRange.WholeEnd - partRange.WholeBegin == (ui64)sizeToCopy);
                            state.Whole.Data.Write(partRange.WholeBegin, tmp.data(), sizeToCopy);
                            state.Whole.Here.Add(partRange.WholeBegin, partRange.WholeEnd);
                            state.Whole.NotHere.Subtract(partRange.WholeBegin, partRange.WholeEnd);
                        }
                    }
                } else {
                    // This is actually possible, for example for 33 byte blob we get:
                    // part 0: xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
                    // part 1: 00000000 00000000 00000000 00000000
                    // part 2: 00000000 00000000 00000000 00000000
                    // part 3: x0000000 00000000 00000000 00000000
                }
            }
        }
        if (state.Whole.NotHere.IsEmpty()) {
            state.WholeSituation = TBlobState::ESituation::Present;
            return EStrategyOutcome::DONE;
        }
        return std::nullopt;
    }

    std::optional<EStrategyOutcome> RestoreWholeWithErasure(TLogContext& /*logCtx*/, TBlobState &state,
            const TBlobStorageGroupInfo &info) {
        const ui32 totalPartCount = info.Type.TotalPartCount();
        const i32 handoff = info.Type.Handoff();
        ui32 partsMissing = 0;
        ui32 responsesPending = 0;
        for (ui32 partIdx = 0; partIdx < totalPartCount; ++partIdx) {
            bool isMissing = true;
            for (i32 niche = -1; niche < handoff; ++niche) {
                ui32 diskIdx = (niche < 0 ? partIdx : totalPartCount + niche);
                TBlobState::TDisk &disk = state.Disks[diskIdx];
                TBlobState::ESituation partSituation = disk.DiskParts[partIdx].Situation;
                TIntervalSet<i32> &requested = disk.DiskParts[partIdx].Requested;
                if (partSituation == TBlobState::ESituation::Present) {
                    isMissing = false;
                }
                if (!requested.IsEmpty()) {
                    responsesPending++;
                }
            }
            if (isMissing) {
                partsMissing++;
            }
        }

        if (partsMissing > info.Type.ParityParts() && responsesPending > 0) {
            return std::nullopt;
        }

        // get intervals needed to restore the requested full data
        TIntervalSet<i32> toRestore;
        for (auto it = state.Whole.NotHere.begin(); it != state.Whole.NotHere.end(); ++it) {
            auto [begin, end] = *it;
            TBlockSplitRange range;
            info.Type.BlockSplitRange((TErasureType::ECrcMode)state.Id.CrcMode(), state.Id.BlobSize(), begin, end, &range);
            for (ui32 partIdx = range.BeginPartIdx; partIdx < range.EndPartIdx; ++partIdx) {
                TPartOffsetRange &partRange = range.PartRanges[partIdx];
                if (partRange.Begin != partRange.End) {
                    toRestore.Add(partRange.AlignedBegin, partRange.AlignedEnd);
                }
            }
        }

        ui32 partsWithEnoughData = 0;
        for (ui32 partIdx = 0; partIdx < totalPartCount; ++partIdx) {
            if (toRestore.IsSubsetOf(state.Parts[partIdx].Here)) {
                partsWithEnoughData++;
            }
        }
        if (partsWithEnoughData < info.Type.MinimalRestorablePartCount()) {
            return std::nullopt;
        }

        // We have enough parts for each interval needed and we can restore all the missing whole intervals

        const ui32 partSize = info.Type.PartSize(state.Id);

        // Gather part ranges that need to be restored
        TIntervalSet<i32> partIntervals;
        for (auto it = state.Whole.NotHere.begin(); it != state.Whole.NotHere.end(); ++it) {
            auto [begin, end] = *it; // missing
            TBlockSplitRange range;
            info.Type.BlockSplitRange((TErasureType::ECrcMode)state.Id.CrcMode(), state.Id.BlobSize(), begin, end, &range);
            for (ui32 partIdx = range.BeginPartIdx; partIdx < range.EndPartIdx; ++partIdx) {
                TPartOffsetRange &partRange = range.PartRanges[partIdx];
                if (partRange.Begin != partRange.End) {
                    partIntervals.Add(partRange.AlignedBegin, partRange.AlignedEnd);
                }
            }
        }
        //Cerr << "partIntervals intersected# " << partIntervals.ToString() << Endl;

        // Obtain part offsets
        TBlockSplitRange wholeRange;
        // Cerr << "Missing begin# " << missingBegin << " end# " << missingEnd << Endl;
        info.Type.BlockSplitRange((TErasureType::ECrcMode)state.Id.CrcMode(), state.Id.BlobSize(),
                0, state.Id.BlobSize(), &wholeRange);

        // Loop through intervals needed to restore the requested full data
        bool isFallback = false;
        {
            TDataPartSet partSet;
            partSet.Parts.resize(totalPartCount);
            i64 maxSize = 0;
            for (auto it = partIntervals.begin(); it != partIntervals.end(); ++it) {
                auto [begin, end] = *it;
                i64 size = end - begin;
                maxSize = Max(size, maxSize);
            }
            for (ui32 i = 0; i < totalPartCount; ++i) {
                TString tmp = TString::Uninitialized(maxSize);
                partSet.Parts[i].ReferenceTo(tmp, 0, maxSize, partSize);
            }
            partSet.FullDataSize = state.Id.BlobSize();
            partSet.IsFragment = true;
            for (auto it = partIntervals.begin(); it != partIntervals.end(); ++it) {
                auto [alignedBegin, alignedEnd] = *it;
                TIntervalVec<i32> needRange(alignedBegin, alignedEnd);
                i32 needSize = alignedEnd - alignedBegin;
                if (info.Type.ErasureFamily() == TErasureType::ErasureParityBlock) {
                    partSet.PartsMask = 0;
                    for (ui32 i = 0; i < totalPartCount; ++i) {
                        partSet.Parts[i].ReferenceTo(partSet.Parts[i].OwnedString, alignedBegin, needSize, partSize);
                        if (needRange.IsSubsetOf(state.Parts[i].Here)) {
                            partSet.PartsMask |= (1 << i);
                            // Read part pieces to those strings
                            state.Parts[i].Data.Read(alignedBegin, partSet.Parts[i].GetDataAt(alignedBegin), needSize);
                        }
                    }
                    // Cerr << Endl << "Restore block data parts" << Endl;
                    // Restore missing part piece
                    info.Type.RestoreData((TErasureType::ECrcMode)state.Id.CrcMode(), partSet, true, false, false);
                    // Write it back to the present set

                    for (ui32 partIdx = wholeRange.BeginPartIdx; partIdx < wholeRange.EndPartIdx; ++partIdx) {
                        TPartOffsetRange &partRange = wholeRange.PartRanges[partIdx];
                        i32 begin = Max<i32>(partRange.Begin, alignedBegin);
                        i32 end = Min<i32>(partRange.End, alignedEnd);
                        if (begin < end) {
                            i32 offset = partRange.WholeBegin + begin - partRange.Begin;
                            TIntervalVec<i32> x(offset, offset + end - begin);
                            if (!x.IsSubsetOf(state.Whole.Here)) {
                                // Cerr << "copy from# " << partRange.WholeBegin << " to# " << partRange.WholeEnd << Endl;
                                state.Whole.Data.Write(offset, partSet.Parts[partIdx].GetDataAt(begin), end - begin);
                                state.Whole.Here.Add(offset, offset + end - begin);
                                state.Whole.NotHere.Subtract(offset, offset + end - begin);
                            }
                        }
                    }
                } else {
                    /*
                    for (ui32 i = 0; i < totalPartCount; ++i) {
                        if (needRange.IsSubsetOf(state.Parts[i].Here)) {
                            partSet.PartsMask |= (1 << i);
                            TString tmp = TString::Uninitialized(needSize);
                            // Read part pieces to those strings
                            state.Parts[i].Data.Read(partRange.AlignedBegin, const_cast<char*>(tmp.Data()), needSize);
                            partSet.Parts[i].ReferenceTo(tmp, partRange.AlignedBegin, needSize, partSize);
                        }
                    }
                    partSet.FullDataSize = state.Id.BlobSize();
                    ui64 wholePieceAlignedSize = partRange.WholeEnd - partRange.AlignedWholeBegin;
                    TString wholePiece = TString::Uninitialized(wholePieceAlignedSize);
                    partSet.FullDataFragment.ReferenceTo(wholePiece, partRange.AlignedWholeBegin,
                    wholePieceAlignedSize, state.Id.BlobSize());
                    */
                    // Restore whole blob piece
                    // Write it back to the present set
                    //state.Whole.Data.Write(partRange.AlignedWholeBegin, wholePiece.Data(), wholePieceAlignedSize);
                    //state.Whole.Here.Add(partRange.AlignedWholeBegin, partRange.WholeEnd);
                    //state.Whole.NotHere.Subtract(partRange.AlignedWholeBegin, partRange.WholeEnd);
                    isFallback = true;
                }
            }
        }

        if (isFallback) {
            // Cerr << "Fallback" << Endl;
            TDataPartSet partSet;
            partSet.Parts.resize(totalPartCount);
            for (ui32 i = 0; i < totalPartCount; ++i) {
                if (toRestore.IsSubsetOf(state.Parts[i].Here)) {
                    partSet.PartsMask |= (1 << i);
                    TString tmp = TString::Uninitialized(partSize);
                    for (auto it = toRestore.begin(); it != toRestore.end(); ++it) {
                        auto [begin, end] = *it;
                        i32 offset = begin;
                        i32 size = end - begin;
                        // Cerr << "part# " << i << " partSize# " << partSize << " offset# " << offset << " size# " << size << Endl;
                        state.Parts[i].Data.Read(offset, const_cast<char*>(tmp.data()) + offset, size);
                    }
                    partSet.Parts[i].ReferenceTo(tmp);
                }
            }
            partSet.FullDataSize = state.Id.BlobSize();
            TRope whole;
            info.Type.RestoreData((TErasureType::ECrcMode)state.Id.CrcMode(), partSet, whole, false, true, false);

            TIntervalSet<i32> missing(state.Whole.NotHere);
            for (auto it = missing.begin(); it != missing.end(); ++it) {
                auto [begin, end] = *it;
                i32 offset = begin;
                i32 size = end - begin;
                const char *source = whole.GetContiguousSpan().data() + offset;
                // Cerr << "LINE " << __LINE__ << " copy whole [" << offset << ", " << (offset + size) << ")" << Endl;
                state.Whole.Data.Write(offset, source, size);
                state.Whole.Here.Add(offset, offset + size);
                state.Whole.NotHere.Subtract(offset, offset + size);
            }
        }
        Y_VERIFY(state.Whole.NotHere.IsEmpty());
        state.WholeSituation = TBlobState::ESituation::Present;
        return EStrategyOutcome::DONE;
    }

    void IssueGetRequestsForMinimal(TLogContext &logCtx, TBlobState &state, const TBlobStorageGroupInfo &info,
            bool considerSlowAsError, TGroupDiskRequests &groupDiskRequests) {
        const ui32 totalPartCount = info.Type.TotalPartCount();
        const i32 handoff = info.Type.Handoff();
        for (auto it = state.Whole.NotHere.begin(); it != state.Whole.NotHere.end(); ++it) {
            auto [begin, end] = *it;
            TBlockSplitRange range;
            info.Type.BlockSplitRange((TErasureType::ECrcMode)state.Id.CrcMode(), state.Id.BlobSize(),
                    begin, end, &range);
            for (ui32 partIdx = range.BeginPartIdx; partIdx < range.EndPartIdx; ++partIdx) {
                TPartOffsetRange &partRange = range.PartRanges[partIdx];
                if (partRange.Begin != partRange.End) {
                    TIntervalSet<i32> partInterval(partRange.AlignedBegin, partRange.AlignedEnd);
                    partInterval.Subtract(state.Parts[partIdx].Here);
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
        for (auto it = state.Whole.NotHere.begin(); it != state.Whole.NotHere.end(); ++it) {
            auto [begin, end] = *it;
            TBlockSplitRange range;
            info.Type.BlockSplitRange((TErasureType::ECrcMode)state.Id.CrcMode(), state.Id.BlobSize(), begin,
                    end, &range);
            for (ui32 partIdx = range.BeginPartIdx; partIdx < range.EndPartIdx; ++partIdx) {
                TPartOffsetRange &partRange = range.PartRanges[partIdx];
                if (partRange.Begin != partRange.End) {
                    TIntervalVec<i32> partInterval(partRange.AlignedBegin, partRange.AlignedEnd);
                    if (!partInterval.IsSubsetOf(state.Parts[partIdx].Here)) {
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
        for (auto it = state.Whole.NotHere.begin(); it != state.Whole.NotHere.end(); ++it) {
            auto [begin, end] = *it;
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
        Y_VERIFY(partsMissing > 0);
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
                        partIntervals.Subtract(state.Parts[partIdx].Here);
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
            TBlackboard& blackboard, TGroupDiskRequests &groupDiskRequests) override {
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
        i32 slowDiskSubgroupIdx = MarkSlowSubgroupDisk(state, info, blackboard, false);
        if (slowDiskSubgroupIdx >= 0) {
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
