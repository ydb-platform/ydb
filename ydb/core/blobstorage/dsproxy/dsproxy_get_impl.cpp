#include "dsproxy_get_impl.h"
#include "dsproxy_put_impl.h"

#include "dsproxy_strategy_base.h"
#include "dsproxy_blackboard.h"

#include "dsproxy_strategy_get_m3dc_basic.h"
#include "dsproxy_strategy_get_m3dc_check.h"
#include "dsproxy_strategy_get_m3dc_restore.h"
#include "dsproxy_strategy_get_m3of4.h"
#include "dsproxy_strategy_restore.h"
#include "dsproxy_strategy_get_bold.h"
#include "dsproxy_strategy_get_min_iops_block.h"
#include "dsproxy_strategy_get_min_iops_mirror.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

namespace NKikimr {

void TGetImpl::PrepareReply(NKikimrProto::EReplyStatus status, TString errorReason, TLogContext &logCtx,
        TAutoPtr<TEvBlobStorage::TEvGetResult> &outGetResult) {
    outGetResult.Reset(new TEvBlobStorage::TEvGetResult(status, QuerySize, Info->GroupID));
    ReplyBytes = 0;
    outGetResult->BlockedGeneration = BlockedGeneration;
    outGetResult->ErrorReason = errorReason;

    if (status != NKikimrProto::OK) {
        Y_ABORT_UNLESS(status != NKikimrProto::NODATA);
        for (ui32 i = 0, e = QuerySize; i != e; ++i) {
            const TEvBlobStorage::TEvGet::TQuery &query = Queries[i];
            TEvBlobStorage::TEvGetResult::TResponse &outResponse = outGetResult->Responses[i];
            outResponse.Status = status;
            outResponse.Id = query.Id;
            outResponse.Shift = query.Shift;
            outResponse.RequestedSize = query.Size;
            outResponse.LooksLikePhantom = PhantomCheck
                ? std::make_optional(false)
                : std::nullopt;

            if (IntegrityCheck) {
                const TBlobState &blobState = Blackboard.GetState(query.Id);
                outResponse.CompletenessFailed = !blobState.HasWrittenQuorum(*Info, nullptr);

                std::unordered_set<ui32> partIndexes;
                ui32 partCount = 0;
                for (const auto &item : blobState.PartMap) {
                    if (!item.Data.IsEmpty()) {
                        partIndexes.insert(item.PartIdRequested);
                        ++partCount;
                    }
                }
                TRope data;
                if (Info->Type.GetErasure() == TErasureType::Erasure4Plus2Block && partIndexes.size() > 3 || 
                    Info->Type.GetErasure() != TErasureType::Erasure4Plus2Block && partCount > 1) {
                    ui32 shift = Min(query.Shift, query.Id.BlobSize());
                    ui32 size = query.Size ? Min(query.Size, query.Id.BlobSize() - shift) : query.Id.BlobSize() - shift;
                    data = blobState.Whole.Data.Read(shift, size);
                } else {
                    continue;
                }

                if (CheckDataInconsistency(blobState, data)) {
                    outResponse.IntegrityCheckFailed = true;
                    ui32 corruptedPartIdx = 0;

                    switch (Info->Type.GetErasure()) {
                        case TBlobStorageGroupType::ErasureMirror3dc:
                        case TBlobStorageGroupType::ErasureMirror3of4:
                            outResponse.CorruptedPartFound = FindCorruptedPartMirror(blobState, corruptedPartIdx);
                            outResponse.CorruptedPartIndex = corruptedPartIdx;
                            break;
                        default: {
                            outResponse.CorruptedPartFound = (outResponse.CompletenessFailed) ? false : FindCorruptedPart42(blobState, data, corruptedPartIdx);
                            outResponse.CorruptedPartIndex = corruptedPartIdx;
                            break;
                        }
                    }
                }
            }
        }
    } else {
        for (ui32 i = 0, e = QuerySize; i != e; ++i) {
            const TEvBlobStorage::TEvGet::TQuery &query = Queries[i];
            TEvBlobStorage::TEvGetResult::TResponse &outResponse = outGetResult->Responses[i];

            const TBlobState &blobState = Blackboard.GetState(query.Id);
            outResponse.Id = query.Id;
            outResponse.PartMap = blobState.PartMap;
            outResponse.LooksLikePhantom = PhantomCheck
                ? std::make_optional(blobState.WholeSituation == TBlobState::ESituation::Absent)
                : std::nullopt;

            // fill in keep/doNotKeep flags
            const auto it = BlobFlags.find(query.Id);
            std::tie(outResponse.Keep, outResponse.DoNotKeep) = it != BlobFlags.end() ? it->second : std::make_tuple(false, false);

            if (blobState.WholeSituation == TBlobState::ESituation::Absent) {
                bool okay = true;

                // extra validation code for phantom logic
                if (PhantomCheck) {
                    TSubgroupPartLayout possiblyPresent;

                    for (ui32 idxInSubgroup = 0; idxInSubgroup < blobState.Disks.size(); ++idxInSubgroup) {
                        const auto& disk = blobState.Disks[idxInSubgroup];
                        for (ui32 partIdx = 0; partIdx < disk.DiskParts.size(); ++partIdx) {
                            bool possible;
                            switch (Info->Type.GetErasure()) {
                                case TBlobStorageGroupType::ErasureMirror3dc:
                                    possible = partIdx == idxInSubgroup % 3;
                                    break;

                                case TBlobStorageGroupType::ErasureMirror3of4:
                                    possible = idxInSubgroup >= 4 || partIdx == (idxInSubgroup & 1) || partIdx == 2;
                                    break;

                                default:
                                    possible = idxInSubgroup == partIdx || idxInSubgroup >= Info->Type.TotalPartCount();
                                    break;
                            }
                            if (!possible) {
                                continue;
                            }
                            switch (disk.DiskParts[partIdx].Situation) {
                                case TBlobState::ESituation::Unknown:
                                    Y_DEBUG_ABORT_S("proxy didn't probe some valid parts of the blob while returning NODATA"
                                        << " State# " << blobState.ToString());
                                    [[fallthrough]];
                                case TBlobState::ESituation::Error:
                                case TBlobState::ESituation::Present:
                                case TBlobState::ESituation::Sent:
                                    possiblyPresent.AddItem(idxInSubgroup, partIdx, Info->Type);
                                    break;

                                case TBlobState::ESituation::Absent:
                                case TBlobState::ESituation::Lost:
                                    // sure we don't have this part
                                    break;
                            }
                        }
                    }

                    const TBlobStorageGroupInfo::TSubgroupVDisks zero(&Info->GetTopology());
                    const auto& checker = Info->GetQuorumChecker();
                    const bool canBeRestored = checker.GetBlobState(possiblyPresent, zero) != TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY;
                    if (canBeRestored) {
                        okay = false; // there is a slight chance that we can restore that blob
                    }
                }

                outResponse.Status = okay ? NKikimrProto::NODATA : NKikimrProto::ERROR;
                IsNoData = true;
            } else if (blobState.WholeSituation == TBlobState::ESituation::Present) {
                outResponse.Status = NKikimrProto::OK;
                outResponse.Shift = query.Shift;
                outResponse.RequestedSize = query.Size;

                if (PhantomCheck) {
                    continue;
                }

                ui32 shift = Min(query.Shift, query.Id.BlobSize());
                ui32 size = query.Size ? Min(query.Size, query.Id.BlobSize() - shift) : query.Id.BlobSize() - shift;
                TRope data = blobState.Whole.Data.Read(shift, size);

                if (IntegrityCheck) {
                    outResponse.CompletenessFailed = !blobState.HasWrittenQuorum(*Info, nullptr);
                    if (CheckDataInconsistency(blobState, data)) {
                        outResponse.Status = NKikimrProto::ERROR;
                        outResponse.IntegrityCheckFailed = true;
                        ui32 corruptedPartIdx = 0;

                        switch (Info->Type.GetErasure()) {
                            case TBlobStorageGroupType::ErasureMirror3dc:
                            case TBlobStorageGroupType::ErasureMirror3of4:
                                outResponse.CorruptedPartFound = FindCorruptedPartMirror(blobState, corruptedPartIdx);
                                outResponse.CorruptedPartIndex = corruptedPartIdx;
                                break;
                            default: {
                                outResponse.CorruptedPartFound = (outResponse.CompletenessFailed) ? false : FindCorruptedPart42(blobState, data, corruptedPartIdx);
                                outResponse.CorruptedPartIndex = corruptedPartIdx;
                                break;
                            }
                        }
                    }
                }
                
                DecryptInplace(data, 0, shift, size, query.Id, *Info);
                outResponse.Buffer = std::move(data);
                Y_ABORT_UNLESS(outResponse.Buffer, "%s empty response buffer", RequestPrefix.data());
                ReplyBytes += outResponse.Buffer.size();
            } else if (blobState.WholeSituation == TBlobState::ESituation::Error) {
                outResponse.Status = NKikimrProto::ERROR;
            } else {
                Y_ABORT_UNLESS(false, "Id# %s BlobState# %s", query.Id.ToString().c_str(), blobState.ToString().data());
            }
        }
    }
    NActors::NLog::EPriority priority = PriorityForStatusOutbound(status);
    DSP_LOG_LOG_SX(logCtx, priority, "BPG29", "Response# " << outGetResult->Print(false));
    if (CollectDebugInfo || (IsVerboseNoDataEnabled && IsNoData)) {
        TStringStream str;
        logCtx.LogAcc.Output(str);
        outGetResult->DebugInfo = str.Str();
    }
    IsReplied = true;
}


ui64 TGetImpl::GetTimeToAccelerateNs(TLogContext &logCtx, NKikimrBlobStorage::EVDiskQueueId queueId) {
    Y_UNUSED(logCtx);
    // Find the slowest disk
    TDiskDelayPredictions worstDisks;
    if (Blackboard.BlobStates.size() == 1) {
        Blackboard.BlobStates.begin()->second.GetWorstPredictedDelaysNs(
                *Info, *Blackboard.GroupQueues, queueId, &worstDisks,
                AccelerationParams.PredictedDelayMultiplier);
    } else {
        Blackboard.GetWorstPredictedDelaysNs(
                *Info, *Blackboard.GroupQueues, queueId, &worstDisks,
                AccelerationParams.PredictedDelayMultiplier);
    }
    return worstDisks[std::min(3u, (ui32)worstDisks.size() - 1)].PredictedNs;
}

ui64 TGetImpl::GetTimeToAccelerateGetNs(TLogContext &logCtx) {
    return GetTimeToAccelerateNs(logCtx, HandleClassToQueueId(Blackboard.GetHandleClass));
}

ui64 TGetImpl::GetTimeToAcceleratePutNs(TLogContext &logCtx) {
    return GetTimeToAccelerateNs(logCtx, HandleClassToQueueId(Blackboard.PutHandleClass));
}

TString TGetImpl::DumpFullState() const {
    TStringStream str;

    str << "{Deadline# " << Deadline;
    str << Endl;
    str << " Info# " << Info->ToString();
    str << Endl;
    // ...
    str << " QuerySize# " << QuerySize;
    str << Endl;
    str << " IsInternal# " << IsInternal;
    str << Endl;
    str << " IsVerboseNoDataEnabled# " << IsVerboseNoDataEnabled;
    str << Endl;
    str << " CollectDebugInfo# " << CollectDebugInfo;
    str << Endl;
    str << " MustRestoreFirst# " << MustRestoreFirst;
    str << Endl;
    str << " ReportDetailedPartMap# " << ReportDetailedPartMap;
    str << Endl;
    if (ForceBlockTabletData) {
        str << " ForceBlockTabletId# " << ForceBlockTabletData->Id;
        str << Endl;
        str << " ForceBlockTabletGeneration# " << ForceBlockTabletData->Generation;
        str << Endl;
    }

    str << " ReplyBytes# " << ReplyBytes;
    str << Endl;
    str << " BytesToReport# " << BytesToReport;
    str << Endl;
    str << " TabletId# " << TabletId;
    str << Endl;

    str << " BlockedGeneration# " << BlockedGeneration;
    str << Endl;
    str << " VPutRequests# " << VPutRequests;
    str << Endl;
    str << " VPutResponses# " << VPutResponses;
    str << Endl;

    str << " IsNoData# " << IsNoData;
    str << Endl;
    str << " IsReplied# " << IsReplied;
    str << Endl;
    str << " AcquireBlockedGeneration# " << AcquireBlockedGeneration;
    str << Endl;

    str << " Blackboard# " << Blackboard.ToString();
    str << Endl;

    str << " RequestIndex# " << RequestIndex;
    str << Endl;
    str << " ResponseIndex# " << ResponseIndex;
    str << Endl;
    str << "}";

    return str.Str();
}

void TGetImpl::GenerateInitialRequests(TLogContext &logCtx, TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &outVGets) {
    Y_ABORT_UNLESS(QuerySize != 0, "internal consistency error");

    // TODO(cthulhu): query politics
    // TODO(cthulhu): adaptive request from nearest domain, and from different DC only when local DC failed
    //                (if at all - may be controlled by query settings)


    for (ui32 queryIdx = 0; queryIdx < QuerySize; ++queryIdx) {
        const TEvBlobStorage::TEvGet::TQuery &query = Queries[queryIdx];
        DSP_LOG_DEBUG_SX(logCtx, "BPG56", "query.Id# " << query.Id.ToString()
            << " shift# " << query.Shift
            << " size# " << query.Size);
        Blackboard.AddNeeded(query.Id, query.Shift, query.Size);
    }

    TAutoPtr<TEvBlobStorage::TEvGetResult> getResult;
    TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> outVPuts;
    bool workDone = Step(logCtx, outVGets, outVPuts, getResult);
    Y_ABORT_UNLESS(outVPuts.empty());
    Y_ABORT_UNLESS(getResult.Get() == nullptr);
    Y_ABORT_UNLESS(workDone);
}

void TGetImpl::PrepareRequests(TLogContext &logCtx, TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &outVGets) {
    TStackVec<std::unique_ptr<TEvBlobStorage::TEvVGet>, TypicalDisksInGroup> gets(Info->GetTotalVDisksNum());

    for (auto& get : Blackboard.GroupDiskRequests.GetsPending) {
        auto& vget = gets[get.OrderNumber];
        if (!vget) {
            const TVDiskID vdiskId = Info->GetVDiskId(get.OrderNumber);
            vget = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdiskId, Deadline, Blackboard.GetHandleClass,
                TEvBlobStorage::TEvVGet::EFlags::None, {}, {}, ForceBlockTabletData);
        }
        std::optional<ui64> cookie;
        if (ReportDetailedPartMap || IntegrityCheck) {
            cookie = Blackboard.AddPartMap(get.Id, get.OrderNumber, RequestIndex);
        }
        vget->AddExtremeQuery(get.Id, get.Shift, get.Size, cookie ? &cookie.value() : nullptr);
        vget->Record.SetSuppressBarrierCheck(IsInternal);
        vget->Record.SetTabletId(TabletId);
        vget->Record.SetAcquireBlockedGeneration(AcquireBlockedGeneration);
        if (ReaderTabletData) {
            auto msg = vget->Record.MutableReaderTabletData();
            msg->SetId(ReaderTabletData->Id);
            msg->SetGeneration(ReaderTabletData->Generation);
        }
    }

    for (auto& vget : gets) {
        if (vget) {
            ui32 orderNumber = Info->GetTopology().GetOrderNumber(VDiskIDFromVDiskID(vget->Record.GetVDiskID()));
            DSP_LOG_DEBUG_SX(logCtx, "BPG14", "Send get to orderNumber# " << orderNumber << " vget# " << vget->ToString());
            if (vget->Record.ExtremeQueriesSize() > 0) {
                TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(vget->Record.GetExtremeQueries(0).GetId());
                History.AddVGetToWaitingList(blobId.PartId(), vget->Record.ExtremeQueriesSize(), orderNumber);
            } else {
                History.AddVGetToWaitingList(THistory::InvalidPartId, 0, orderNumber);
            }
            outVGets.push_back(std::move(vget));
            ++RequestIndex;
        }
    }
    
    Blackboard.GroupDiskRequests.GetsPending.clear();
}

void TGetImpl::PrepareVPuts(TLogContext &logCtx, TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> &outVPuts) {
    for (auto& put : Blackboard.GroupDiskRequests.PutsPending) {
        const TVDiskID vdiskId = Info->GetVDiskId(put.OrderNumber);
        Y_DEBUG_ABORT_UNLESS(Info->Type.GetErasure() != TBlobStorageGroupType::ErasureMirror3of4 ||
            put.Id.PartId() != 3 || put.Buffer.IsEmpty());
        auto vput = std::make_unique<TEvBlobStorage::TEvVPut>(put.Id, put.Buffer, vdiskId, true, nullptr, Deadline,
            Blackboard.PutHandleClass);
        DSP_LOG_DEBUG_SX(logCtx, "BPG15", "Send put to orderNumber# " << put.OrderNumber << " vput# " << vput->ToString());
        History.AddVPutToWaitingList(put.Id.PartId(), 1, put.OrderNumber);
        outVPuts.push_back(std::move(vput));
        ++VPutRequests;
    }
    Blackboard.GroupDiskRequests.PutsPending.clear();
}

EStrategyOutcome TGetImpl::RunBoldStrategy(TLogContext &logCtx) {
    TStackVec<IStrategy*, 1> strategies;
    TBoldStrategy s1(PhantomCheck || IntegrityCheck);
    strategies.push_back(&s1);
    TRestoreStrategy s2;
    if (MustRestoreFirst) {
        strategies.push_back(&s2);
    }
    return Blackboard.RunStrategies(logCtx, strategies, AccelerationParams);
}

EStrategyOutcome TGetImpl::RunMirror3dcStrategy(TLogContext &logCtx) {
    if (IntegrityCheck) {
        return Blackboard.RunStrategy(logCtx, TMirror3dcCheckGetStrategy(), AccelerationParams);
    }
    return MustRestoreFirst
        ? Blackboard.RunStrategy(logCtx, TMirror3dcGetWithRestoreStrategy(), AccelerationParams)
        : Blackboard.RunStrategy(logCtx, TMirror3dcBasicGetStrategy(NodeLayout, PhantomCheck), AccelerationParams);
}

EStrategyOutcome TGetImpl::RunMirror3of4Strategy(TLogContext &logCtx) {
    TStackVec<IStrategy*, 1> strategies;
    TMirror3of4GetStrategy s1(IntegrityCheck);
    strategies.push_back(&s1);
    TPut3of4Strategy s2(TEvBlobStorage::TEvPut::TacticMaxThroughput);
    if (MustRestoreFirst) {
        strategies.push_back(&s2);
    }
    return Blackboard.RunStrategies(logCtx, strategies, AccelerationParams);
}

EStrategyOutcome TGetImpl::RunStrategies(TLogContext &logCtx) {
    auto erasureType = Info->Type.GetErasure();
    auto erasureFamily = Info->Type.ErasureFamily();

    if (erasureType == TErasureType::ErasureMirror3dc) {
        return RunMirror3dcStrategy(logCtx);
    }
    if (erasureType == TErasureType::ErasureMirror3of4) {
        return RunMirror3of4Strategy(logCtx);
    }
    if (MustRestoreFirst || PhantomCheck || IntegrityCheck) {
        return RunBoldStrategy(logCtx);
    }
    if (erasureFamily == TErasureType::ErasureParityBlock) {
        return Blackboard.RunStrategy(logCtx, TMinIopsBlockStrategy(), AccelerationParams);
    }
    if (erasureFamily == TErasureType::ErasureMirror) {
        return Blackboard.RunStrategy(logCtx, TMinIopsMirrorStrategy(), AccelerationParams);
    }
    return RunBoldStrategy(logCtx);
}

void TGetImpl::OnVPutResult(TLogContext &logCtx, TEvBlobStorage::TEvVPutResult &ev,
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &outVGets, TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> &outVPuts,
        TAutoPtr<TEvBlobStorage::TEvGetResult> &outGetResult) {
    const NKikimrBlobStorage::TEvVPutResult &record = ev.Record;
    Y_ABORT_UNLESS(record.HasVDiskID());
    TVDiskID vdisk = VDiskIDFromVDiskID(record.GetVDiskID());
    TVDiskIdShort shortId(vdisk);
    ui32 orderNumber = Info->GetOrderNumber(shortId);
    const TLogoBlobID blob = LogoBlobIDFromLogoBlobID(record.GetBlobID());

    const NKikimrProto::EReplyStatus status = record.GetStatus();
    ++VPutResponses;
    switch (status) {
        case NKikimrProto::ERROR:
        case NKikimrProto::VDISK_ERROR_STATE:
        case NKikimrProto::OUT_OF_SPACE:
            Blackboard.AddErrorResponse(blob, orderNumber);
            break;
        case NKikimrProto::OK:
        case NKikimrProto::ALREADY:
            Blackboard.AddPutOkResponse(blob, orderNumber);
            break;
        default:
        Y_ABORT("Unexpected status# %s", NKikimrProto::EReplyStatus_Name(status).data());
    }
    Step(logCtx, outVGets, outVPuts, outGetResult);
    History.AddVPutResult(orderNumber, status, record.GetErrorReason());
}

bool TGetImpl::CheckDataInconsistency(const TBlobState &blobState, const TRope &data) {
    TStackVec<TRope, TypicalPartsInBlob> partData(Info->Type.TotalPartCount());
    const bool isBlock42 = (Info->Type.GetErasure() == TBlobStorageGroupType::Erasure4Plus2Block);
    if (isBlock42) {
        ErasureSplit((TErasureType::ECrcMode)blobState.Id.CrcMode(), Info->Type, data, partData);
    }
    for (const auto &item : blobState.PartMap) {
        if (item.Data.IsEmpty()) {
            continue;
        }
        const TRope &partToCheck = isBlock42 ? partData[item.PartIdRequested - 1] : data;
        if (item.Data != partToCheck) {
            return true;
        }
    }
    return false;
}

bool TGetImpl::FindCorruptedPart42(const TBlobState &blobState, const TRope &data, ui32 &outPartIndex) {
    const auto &parts = blobState.PartMap;
    auto crcMode = static_cast<TErasureType::ECrcMode>(blobState.Id.CrcMode());
    TStackVec<TRope, TypicalPartsInBlob> selectedParts(Info->Type.TotalPartCount());
    TStackVec<TRope, TypicalPartsInBlob> restoredParts(Info->Type.TotalPartCount());
    TRope restoredBlob;

    for (ui32 excludedPart = 0; excludedPart < parts.size(); ++excludedPart) {
        if (parts[excludedPart].Data.IsEmpty()) {
            continue;
        }
        std::fill(selectedParts.begin(), selectedParts.end(), TRope());
        std::fill(restoredParts.begin(), restoredParts.end(), TRope());
        ui32 restoreMask = 0;
        ui32 selectedCount = 0;
        for (ui32 i = 0; i < parts.size(); ++i) {
            if (i == excludedPart || parts[i].Data.IsEmpty()) {
                continue;
            }
            ui32 partId = parts[i].PartIdRequested - 1;
            if (selectedParts[partId].IsEmpty()) {
                selectedParts[partId] = parts[i].Data;
                restoreMask |= (1 << partId);
                ++selectedCount;
            }
            if (selectedCount == 4) {
                break;
            }
        }
        if (selectedCount != 4) {
            continue;
        }
        
        restoredBlob.clear();
        ErasureRestore(crcMode, Info->Type, data.size(), &restoredBlob, selectedParts, restoreMask);
        ErasureSplit(crcMode, Info->Type, restoredBlob, restoredParts);
        
        ui32 mismatch = 0;
        for (ui32 i = 0; i < parts.size(); ++i) {
            if (i == excludedPart || parts[i].Data.IsEmpty()) {
                continue;
            }
            ui32 partId = parts[i].PartIdRequested - 1;
            if (parts[i].Data != restoredParts[partId]) {
                ++mismatch;
            }
        }
        if (mismatch == 0) {
            outPartIndex = excludedPart;
            return true;
        }
    }
    return false;
}

bool TGetImpl::FindCorruptedPartMirror(const TBlobState &blobState, ui32 &outPartIndex) {
    const auto &parts = blobState.PartMap;
    ui32 corruptedParts = 0;

    for (ui32 checkedPart = 0; checkedPart < parts.size(); ++checkedPart) {
        if (parts[checkedPart].Data.IsEmpty()) {
            continue;
        }
        ui32 mismatch = 0;
        for (ui32 i = 0; i < parts.size(); ++i) {
            if (i == checkedPart || parts[i].Data.IsEmpty()) {
                continue;
            }
            if (parts[checkedPart].Data != parts[i].Data) {
                ++mismatch;
            }
        }
        if (mismatch > 1) {
            outPartIndex = checkedPart;
            ++corruptedParts;
        }
    }
    return corruptedParts == 1;
}

}//NKikimr

