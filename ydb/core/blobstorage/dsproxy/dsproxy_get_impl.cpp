#include "dsproxy_get_impl.h"
#include "dsproxy_put_impl.h"

#include "dsproxy_strategy_base.h"
#include "dsproxy_blackboard.h"

#include "dsproxy_strategy_get_m3dc_basic.h"
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
        Y_VERIFY(status != NKikimrProto::NODATA);
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
        }
    } else {
        for (ui32 i = 0, e = QuerySize; i != e; ++i) {
            const TEvBlobStorage::TEvGet::TQuery &query = Queries[i];
            TEvBlobStorage::TEvGetResult::TResponse &outResponse = outGetResult->Responses[i];

            const TBlobState &blobState = Blackboard.GetState(query.Id);
            outResponse.Id = query.Id;
            outResponse.PartMap = blobState.PartMap;
            outResponse.Keep = blobState.Keep;
            outResponse.DoNotKeep = blobState.DoNotKeep;
            outResponse.LooksLikePhantom = PhantomCheck
                ? std::make_optional(blobState.WholeSituation == TBlobState::ESituation::Absent)
                : std::nullopt;

            if (blobState.WholeSituation == TBlobState::ESituation::Absent) {
                bool okay = true;

                // extra validation code for phantom logic
                if (PhantomCheck) {
                    TSubgroupPartLayout possiblyWritten;

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
                                case TBlobState::ESituation::Error:
                                case TBlobState::ESituation::Present:
                                case TBlobState::ESituation::Sent:
                                    possiblyWritten.AddItem(idxInSubgroup, partIdx, Info->Type);
                                    break;

                                case TBlobState::ESituation::Absent:
                                case TBlobState::ESituation::Lost:
                                    // sure we don't have this part
                                    break;
                            }
                        }
                    }

                    switch (Info->Type.GetErasure()) {
                        case TBlobStorageGroupType::ErasureMirror3dc:
                            if (possiblyWritten.GetDisksWithPart(0) || possiblyWritten.GetDisksWithPart(1) ||
                                    possiblyWritten.GetDisksWithPart(2)) {
                                okay = false;
                            }
                            break;

                        case TBlobStorageGroupType::ErasureMirror3of4:
                            if (possiblyWritten.GetDisksWithPart(0) || possiblyWritten.GetDisksWithPart(1)) {
                                okay = false;
                            }
                            break;

                        default: {
                            ui32 numDistinctParts = 0;
                            for (ui32 partIdx = 0; partIdx < Info->Type.TotalPartCount(); ++partIdx) {
                                if (possiblyWritten.GetDisksWithPart(partIdx)) {
                                    ++numDistinctParts;
                                }
                            }
                            if (numDistinctParts >= Info->Type.MinimalRestorablePartCount()) {
                                okay = false;
                            }
                            break;
                        }
                    }
                }

                outResponse.Status = okay ? NKikimrProto::NODATA : NKikimrProto::ERROR;
                IsNoData = true;
            } else if (blobState.WholeSituation == TBlobState::ESituation::Present) {
                outResponse.Status = NKikimrProto::OK;
                outResponse.Shift = query.Shift;
                outResponse.RequestedSize = query.Size;

                ui32 shift = Min(query.Shift, query.Id.BlobSize());
                ui32 size = query.Size ? Min(query.Size, query.Id.BlobSize() - shift) : query.Id.BlobSize() - shift;

                if (!PhantomCheck) {
                    TString data = TString::Uninitialized(size);
                    char *dataBuffer = data.Detach();
                    blobState.Whole.Data.Read(shift, dataBuffer, size);
                    Decrypt(dataBuffer, dataBuffer, shift, size, query.Id, *Info);
                    outResponse.Buffer = std::move(data);
                    Y_VERIFY(outResponse.Buffer, "%s empty response buffer", RequestPrefix.data());
                    ReplyBytes += outResponse.Buffer.size();
                }
            } else if (blobState.WholeSituation == TBlobState::ESituation::Error) {
                outResponse.Status = NKikimrProto::ERROR;
            } else {
                Y_VERIFY(false, "Id# %s BlobState# %s", query.Id.ToString().c_str(), blobState.ToString().data());
            }
        }
    }
    NActors::NLog::EPriority priority = PriorityForStatusOutbound(status);
    A_LOG_LOG_SX(logCtx, priority != NActors::NLog::PRI_DEBUG, priority, "BPG29", "Response# " << outGetResult->Print(false));
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
    ui64 worstPredictedNs = 0;
    ui64 nextToWorstPredictedNs = 0;
    if (Blackboard.BlobStates.size() == 1) {
        i32 worstSubgroupIdx = -1;
        Blackboard.BlobStates.begin()->second.GetWorstPredictedDelaysNs(
                *Info, *Blackboard.GroupQueues, queueId,
                &worstPredictedNs, &nextToWorstPredictedNs, &worstSubgroupIdx);
    } else {
        i32 worstOrderNumber = -1;
        Blackboard.GetWorstPredictedDelaysNs(
                *Info, *Blackboard.GroupQueues, queueId,
                &worstPredictedNs, &nextToWorstPredictedNs, &worstOrderNumber);
    }
    return nextToWorstPredictedNs * 1;
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
    str << " ForceBlockTabletId# " << ForceBlockTabletData->Id;
    str << Endl;
    str << " ForceBlockTabletGeneration# " << ForceBlockTabletData->Generation;
    str << Endl;

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
    str << " VMultiPutRequests# " << VMultiPutRequests;
    str << Endl;
    str << " VMultiPutResponses# " << VMultiPutResponses;
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
    Y_VERIFY(QuerySize != 0, "internal consistency error");

    // TODO(cthulhu): query politics
    // TODO(cthulhu): adaptive request from nearest domain, and from different DC only when local DC failed
    //                (if at all - may be controlled by query settings)


    for (ui32 queryIdx = 0; queryIdx < QuerySize; ++queryIdx) {
        const TEvBlobStorage::TEvGet::TQuery &query = Queries[queryIdx];
        R_LOG_DEBUG_SX(logCtx, "BPG56", "query.Id# " << query.Id.ToString()
            << " shift# " << query.Shift
            << " size# " << query.Size);
        Blackboard.AddNeeded(query.Id, query.Shift, query.Size);
    }

    TAutoPtr<TEvBlobStorage::TEvGetResult> getResult;
    TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> outVPuts;
    bool workDone = Step(logCtx, outVGets, outVPuts, getResult);
    Y_VERIFY(outVPuts.empty());
    Y_VERIFY(getResult.Get() == nullptr);
    Y_VERIFY(workDone);
}

void TGetImpl::PrepareRequests(TLogContext &logCtx, TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &outVGets) {
    for (ui32 diskOrderNumber = 0; diskOrderNumber < Blackboard.GroupDiskRequests.DiskRequestsForOrderNumber.size();
            ++diskOrderNumber) {
        TDiskRequests &requests = Blackboard.GroupDiskRequests.DiskRequestsForOrderNumber[diskOrderNumber];
        ui32 endIdx = requests.GetsToSend.size();
        ui32 beginIdx = requests.FirstUnsentRequestIdx;

        if (beginIdx < endIdx) {
            TVDiskID vDiskId = Info->GetVDiskId(diskOrderNumber);
            auto vGet = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vDiskId, Deadline, Blackboard.GetHandleClass,
                    TEvBlobStorage::TEvVGet::EFlags::None, TVGetCookie(beginIdx, endIdx), {}, ForceBlockTabletData);
            for (ui32 idx = beginIdx; idx < endIdx; ++idx) {
                TDiskGetRequest &get = requests.GetsToSend[idx];
                ui64 cookie = idx;
                vGet->AddExtremeQuery(get.Id, get.Shift, get.Size, &cookie);
                if (ReportDetailedPartMap) {
                    get.PartMapIndex = Blackboard.AddPartMap(get.Id, diskOrderNumber, RequestIndex);
                }
            }
            vGet->Record.SetSuppressBarrierCheck(IsInternal);
            vGet->Record.SetTabletId(TabletId);
            vGet->Record.SetAcquireBlockedGeneration(AcquireBlockedGeneration);

            if (ReaderTabletData) {
                auto msg = vGet->Record.MutableReaderTabletData();
                msg->SetId(ReaderTabletData->Id);
                msg->SetGeneration(ReaderTabletData->Generation);
            }

            R_LOG_DEBUG_SX(logCtx, "BPG14", "Send get to orderNumber# " << diskOrderNumber
                << " beginIdx# " << beginIdx
                << " endIdx# " << endIdx
                << " vGet# " << vGet->ToString());
            outVGets.push_back(std::move(vGet));
            ++RequestIndex;
            Blackboard.GroupDiskRequests.DiskRequestsForOrderNumber[diskOrderNumber].FirstUnsentRequestIdx = endIdx;
        }
    }
}

void TGetImpl::PrepareVPuts(TLogContext &logCtx,
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> &outVPuts) {
    for (ui32 diskOrderNumber = 0; diskOrderNumber < Blackboard.GroupDiskRequests.DiskRequestsForOrderNumber.size();
            ++diskOrderNumber) {
        const TDiskRequests &requests = Blackboard.GroupDiskRequests.DiskRequestsForOrderNumber[diskOrderNumber];
        ui32 endIdx = requests.PutsToSend.size();
        ui32 beginIdx = requests.FirstUnsentPutIdx;

        if (beginIdx < endIdx) {
            TVDiskID vDiskId = Info->GetVDiskId(diskOrderNumber);
            for (ui32 idx = beginIdx; idx < endIdx; ++idx) {
                const TDiskPutRequest &put = requests.PutsToSend[idx];
                ui64 cookie = TBlobCookie(diskOrderNumber, put.BlobIdx, put.Id.PartId(),
                        VPutRequests);
                auto vPut = std::make_unique<TEvBlobStorage::TEvVPut>(put.Id, put.Buffer, vDiskId, true, &cookie,
                    Deadline, Blackboard.PutHandleClass);
                R_LOG_DEBUG_SX(logCtx, "BPG15", "Send put to orderNumber# " << diskOrderNumber << " idx# " << idx
                        << " vPut# " << vPut->ToString());
                outVPuts.push_back(std::move(vPut));
                ++VPutRequests;
                ReceivedVPutResponses.push_back(false);
            }
            Blackboard.GroupDiskRequests.DiskRequestsForOrderNumber[diskOrderNumber].FirstUnsentPutIdx = endIdx;
        }
    }
}

void TGetImpl::PrepareVPuts(TLogContext &logCtx,
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVMultiPut>> &outVMultiPuts) {
    for (ui32 diskOrderNumber = 0; diskOrderNumber < Blackboard.GroupDiskRequests.DiskRequestsForOrderNumber.size();
            ++diskOrderNumber) {
        const TDiskRequests &requests = Blackboard.GroupDiskRequests.DiskRequestsForOrderNumber[diskOrderNumber];
        ui32 endIdx = requests.PutsToSend.size();
        ui32 beginIdx = requests.FirstUnsentPutIdx;
        if (beginIdx < endIdx) {
            TVDiskID vDiskId = Info->GetVDiskId(diskOrderNumber);
            // set cookie after adding items
            auto vMultiPut = std::make_unique<TEvBlobStorage::TEvVMultiPut>(vDiskId, Deadline, Blackboard.PutHandleClass,
                true, nullptr);
            ui64 bytes = 0;
            ui64 lastItemCount = 0;
            for (ui32 idx = beginIdx; idx < endIdx; ++idx) {
                const TDiskPutRequest &put = requests.PutsToSend[idx];
                ui64 cookie = TBlobCookie(diskOrderNumber, put.BlobIdx, put.Id.PartId(), VMultiPutRequests);
                ui64 itemSize = vMultiPut->Record.ItemsSize();
                if (itemSize == MaxBatchedPutRequests || bytes + put.Buffer.size() > MaxBatchedPutSize) {
                    vMultiPut->Record.SetCookie(TVMultiPutCookie(diskOrderNumber, lastItemCount, VMultiPutRequests));
                    ++VMultiPutRequests;
                    ReceivedVMultiPutResponses.push_back(false);
                    R_LOG_DEBUG_SX(logCtx, "BPG16", "Send multiPut to orderNumber# " << diskOrderNumber << " count# "
                            << vMultiPut->Record.ItemsSize() << " vMultiPut# " << vMultiPut->ToString());
                    outVMultiPuts.push_back(std::move(vMultiPut));
                    // set cookie after adding items
                    vMultiPut = std::make_unique<TEvBlobStorage::TEvVMultiPut>(vDiskId, Deadline,
                        Blackboard.PutHandleClass, true, nullptr);
                    bytes = 0;
                    lastItemCount = 0;
                }
                bytes += put.Buffer.size();
                lastItemCount++;
                vMultiPut->AddVPut(put.Id, TRcBuf(TRope(put.Buffer)), &cookie, put.ExtraBlockChecks, NWilson::TTraceId());
            }
            vMultiPut->Record.SetCookie(TVMultiPutCookie(diskOrderNumber, lastItemCount, VMultiPutRequests));
            ++VMultiPutRequests;
            ReceivedVMultiPutResponses.push_back(false);
            R_LOG_DEBUG_SX(logCtx, "BPG17", "Send multiPut to orderNumber# " << diskOrderNumber << " count# "
                    << vMultiPut->Record.ItemsSize() << " vMultiPut# " << vMultiPut->ToString());
            outVMultiPuts.push_back(std::move(vMultiPut));
            Blackboard.GroupDiskRequests.DiskRequestsForOrderNumber[diskOrderNumber].FirstUnsentPutIdx = endIdx;
        }
    }
}

EStrategyOutcome TGetImpl::RunBoldStrategy(TLogContext &logCtx) {
    EStrategyOutcome outcome = Blackboard.RunStrategy(logCtx, TBoldStrategy(PhantomCheck));
    if (outcome == EStrategyOutcome::DONE && MustRestoreFirst) {
        Blackboard.ChangeAll();
        outcome = Blackboard.RunStrategy(logCtx, TRestoreStrategy());
    }
    return outcome;
}

EStrategyOutcome TGetImpl::RunMirror3dcStrategy(TLogContext &logCtx) {
    return MustRestoreFirst
        ? Blackboard.RunStrategy(logCtx, TMirror3dcGetWithRestoreStrategy())
        : Blackboard.RunStrategy(logCtx, TMirror3dcBasicGetStrategy(NodeLayout, PhantomCheck));
}

EStrategyOutcome TGetImpl::RunMirror3of4Strategy(TLogContext &logCtx) {
    // run basic get strategy and, if blob restoration is required and we have successful get, restore the blob to full amount of parts
    EStrategyOutcome outcome = Blackboard.RunStrategy(logCtx, TMirror3of4GetStrategy());
    if (outcome == EStrategyOutcome::DONE && MustRestoreFirst) {
        Blackboard.ChangeAll();
        outcome = Blackboard.RunStrategy(logCtx, TPut3of4Strategy(TEvBlobStorage::TEvPut::TacticMaxThroughput));
    }
    return outcome;
}

EStrategyOutcome TGetImpl::RunStrategies(TLogContext &logCtx) {
    if (Info->Type.GetErasure() == TErasureType::ErasureMirror3dc) {
        return RunMirror3dcStrategy(logCtx);
    } else if (Info->Type.GetErasure() == TErasureType::ErasureMirror3of4) {
        return RunMirror3of4Strategy(logCtx);
    } else if (MustRestoreFirst || PhantomCheck) {
        return RunBoldStrategy(logCtx);
    } else if (Info->Type.ErasureFamily() == TErasureType::ErasureParityBlock) {
        return Blackboard.RunStrategy(logCtx, TMinIopsBlockStrategy());
    } else if (Info->Type.ErasureFamily() == TErasureType::ErasureMirror) {
        return Blackboard.RunStrategy(logCtx, TMinIopsMirrorStrategy());
    } else {
        return RunBoldStrategy(logCtx);
    }
}

void TGetImpl::OnVPutResult(TLogContext &logCtx, TEvBlobStorage::TEvVPutResult &ev,
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &outVGets, TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> &outVPuts,
        TAutoPtr<TEvBlobStorage::TEvGetResult> &outGetResult) {
    const NKikimrBlobStorage::TEvVPutResult &record = ev.Record;
    Y_VERIFY(record.HasVDiskID());
    TVDiskID vdisk = VDiskIDFromVDiskID(record.GetVDiskID());
    TVDiskIdShort shortId(vdisk);
    ui32 orderNumber = Info->GetOrderNumber(shortId);
    const TLogoBlobID blob = LogoBlobIDFromLogoBlobID(record.GetBlobID());

    Y_VERIFY(record.HasCookie());
    TBlobCookie cookie(record.GetCookie());
    Y_VERIFY(cookie.GetVDiskOrderNumber() == orderNumber);
    Y_VERIFY(cookie.GetPartId() == blob.PartId());

    ui64 requestIdx = cookie.GetRequestIdx();
    Y_VERIFY_S(!ReceivedVPutResponses[requestIdx], "the response is received twice"
            << " Event# " << ev.ToString()
            << " State# " << DumpFullState());
    ReceivedVPutResponses[requestIdx] = true;

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
        Y_FAIL("Unexpected status# %s", NKikimrProto::EReplyStatus_Name(status).data());
    }
    Step(logCtx, outVGets, outVPuts, outGetResult);
}

void TGetImpl::OnVPutResult(TLogContext &logCtx, TEvBlobStorage::TEvVMultiPutResult &ev,
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &outVGets,
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVMultiPut>> &outVMultiPuts,
        TAutoPtr<TEvBlobStorage::TEvGetResult> &outGetResult) {
    const NKikimrBlobStorage::TEvVMultiPutResult &record = ev.Record;
    Y_VERIFY(record.HasVDiskID());
    TVDiskID vdisk = VDiskIDFromVDiskID(record.GetVDiskID());
    TVDiskIdShort shortId(vdisk);
    ui32 orderNumber = Info->GetOrderNumber(shortId);

    Y_VERIFY(record.HasCookie());
    TVMultiPutCookie cookie(record.GetCookie());
    Y_VERIFY(cookie.GetVDiskOrderNumber() == orderNumber);
    Y_VERIFY(cookie.GetItemCount() == record.ItemsSize());

    ui64 requestIdx = cookie.GetRequestIdx();
    Y_VERIFY_S(!ReceivedVMultiPutResponses[requestIdx], "the response is received twice"
            << " Event# " << ev.ToString()
            << " State# " << DumpFullState());
    ReceivedVMultiPutResponses[requestIdx] = true;

    ++VMultiPutResponses;
    for (auto &item : record.GetItems()) {
        const NKikimrProto::EReplyStatus status = item.GetStatus();
        const TLogoBlobID blob = LogoBlobIDFromLogoBlobID(item.GetBlobID());
        Y_VERIFY(item.HasCookie());
        TBlobCookie itemCookie(item.GetCookie());
        Y_VERIFY(itemCookie.GetVDiskOrderNumber() == orderNumber);
        Y_VERIFY(itemCookie.GetPartId() == blob.PartId());
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
            Y_FAIL("Unexpected status# %s", NKikimrProto::EReplyStatus_Name(status).data());
        }
    }
    Step(logCtx, outVGets, outVMultiPuts, outGetResult);
}

}//NKikimr
