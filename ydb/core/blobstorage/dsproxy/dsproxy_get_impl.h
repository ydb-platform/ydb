#pragma once

#include "dsproxy.h"
#include "dsproxy_blackboard.h"
#include "dsproxy_cookies.h"
#include "dsproxy_mon.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <util/generic/set.h>

namespace NKikimr {

class TStrategyBase;

class TGetImpl {
    const TInstant Deadline;
    const TIntrusivePtr<TBlobStorageGroupInfo> Info;
    TArrayHolder<TEvBlobStorage::TEvGet::TQuery> Queries;
    const TNodeLayoutInfoPtr NodeLayout;
    const ui32 QuerySize;
    const bool IsInternal;
    const bool IsVerboseNoDataEnabled;
    const bool CollectDebugInfo;
    const bool MustRestoreFirst;
    const bool ReportDetailedPartMap;
    std::optional<TEvBlobStorage::TEvGet::TForceBlockTabletData> ForceBlockTabletData;

    ui64 ReplyBytes = 0;
    i64 BytesToReport = 0;
    ui64 TabletId = 0;

    ui32 BlockedGeneration = 0;
    ui32 VPutRequests = 0;
    ui32 VPutResponses = 0;
    ui32 VMultiPutRequests = 0;
    ui32 VMultiPutResponses = 0;

    bool IsNoData = false;
    bool IsReplied = false;
    bool AcquireBlockedGeneration = false;

    TBlackboard Blackboard;

    ui32 RequestIndex = 0;
    ui32 ResponseIndex = 0;

    TStackVec<bool, MaxBatchedPutRequests * TypicalDisksInSubring> ReceivedVPutResponses;
    TStackVec<bool, MaxBatchedPutRequests * TypicalDisksInSubring> ReceivedVMultiPutResponses;

    const TString RequestPrefix;

    const bool PhantomCheck;
    const bool Decommission;

    std::optional<TEvBlobStorage::TEvGet::TReaderTabletData> ReaderTabletData;

public:
    TGetImpl(const TIntrusivePtr<TBlobStorageGroupInfo> &info, const TIntrusivePtr<TGroupQueues> &groupQueues,
            TEvBlobStorage::TEvGet *ev, TNodeLayoutInfoPtr&& nodeLayout, const TString& requestPrefix = {})
        : Deadline(ev->Deadline)
        , Info(info)
        , Queries(ev->Queries.Release())
        , NodeLayout(std::move(nodeLayout))
        , QuerySize(ev->QuerySize)
        , IsInternal(ev->IsInternal)
        , IsVerboseNoDataEnabled(ev->IsVerboseNoDataEnabled)
        , CollectDebugInfo(ev->CollectDebugInfo)
        , MustRestoreFirst(ev->MustRestoreFirst)
        , ReportDetailedPartMap(ev->ReportDetailedPartMap)
        , ForceBlockTabletData(ev->ForceBlockTabletData)
        , Blackboard(info, groupQueues, NKikimrBlobStorage::AsyncBlob, ev->GetHandleClass)
        , RequestPrefix(requestPrefix)
        , PhantomCheck(ev->PhantomCheck)
        , Decommission(ev->Decommission)
        , ReaderTabletData(ev->ReaderTabletData)
    {
        Y_VERIFY(QuerySize > 0);
    }

    std::unique_ptr<IEventBase> RestartQuery(ui32 counter) {
        auto ev = std::make_unique<TEvBlobStorage::TEvGet>(Queries, QuerySize, Deadline, GetHandleClass(), MustRestoreFirst,
            false /*isIndexOnly*/, ForceBlockTabletData, IsInternal, IsVerboseNoDataEnabled, CollectDebugInfo,
            ReportDetailedPartMap);
        ev->RestartCounter = counter;
        ev->PhantomCheck = PhantomCheck;
        ev->Decommission = Decommission;
        ev->ReaderTabletData = ReaderTabletData;
        return ev;
    }

    ui32 CountRequestBytes() const {
        ui32 res = 0;
        for (ui32 k = 0; k < QuerySize; ++k) {
            const auto& query = Queries[k];
            ui32 size = query.Size;
            if (!size) {
                size = Max<i32>(0, query.Id.BlobSize() - query.Shift);
            }
            res += size;
        }
        return res;
    }

    ui64 GetReplyBytes() {
        return ReplyBytes;
    }

    NKikimrBlobStorage::EGetHandleClass GetHandleClass() const {
        return Blackboard.GetHandleClass;
    }

    NKikimrBlobStorage::EPutHandleClass GetPutHandleClass() const {
        return Blackboard.PutHandleClass;
    }

    void ReportBytes(i64 bytes) {
        BytesToReport += bytes;
    }

    i64 GrabBytesToReport() {
        i64 bytesToReport = BytesToReport;
        BytesToReport = 0;
        return bytesToReport;
    }

    TString DumpQuery() const {
        TStringStream str;
        str << "{";
        for (ui32 i = 0; i < QuerySize; ++i) {
            str << (i ? " " : "")
                << Queries[i].Id
                << "@" << Queries[i].Shift
                << ":" << Queries[i].Size;
        }
        str << "}";
        return str.Str();
    }

    ui64 GetVPutRequests() const {
        return VPutRequests;
    }

    ui64 GetVPutResponses() const {
        return VPutResponses;
    }

    ui64 GetVMultiPutRequests() const {
        return VMultiPutRequests;
    }

    ui64 GetVMultiPutResponses() const {
        return VMultiPutResponses;
    }

    ui64 GetRequestIndex() const {
        return RequestIndex;
    }

    ui64 GetResponseIndex() const {
        return ResponseIndex;
    }


    void GenerateInitialRequests(TLogContext &logCtx, TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &outVGets);

    template <typename TVPutEvent>
    void OnVGetResult(TLogContext &logCtx, TEvBlobStorage::TEvVGetResult &ev,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &outVGets, TDeque<std::unique_ptr<TVPutEvent>> &outVPuts,
            TAutoPtr<TEvBlobStorage::TEvGetResult> &outGetResult) {
        const NKikimrBlobStorage::TEvVGetResult &record = ev.Record;
        Y_VERIFY(record.HasStatus());
        const NKikimrProto::EReplyStatus status = record.GetStatus();
        Y_VERIFY(status != NKikimrProto::RACE && status != NKikimrProto::BLOCKED && status != NKikimrProto::DEADLINE);
        R_LOG_DEBUG_SX(logCtx, "BPG57", "handle result# " << ev.ToString());

        Y_VERIFY(record.HasVDiskID());
        TVDiskID vdisk = VDiskIDFromVDiskID(record.GetVDiskID());
        TVDiskIdShort shortId(vdisk);
        ui32 orderNumber = Info->GetOrderNumber(shortId);
        {
            NActors::NLog::EPriority priority = PriorityForStatusInbound(record.GetStatus());
            A_LOG_LOG_SX(logCtx, priority != NActors::NLog::PRI_DEBUG, priority, "BPG12", "Handle TEvVGetResult"
                << " status# " << NKikimrProto::EReplyStatus_Name(record.GetStatus()).data()
                << " From# " << vdisk.ToString()
                << " orderNumber# " << orderNumber
                << " ev " << ev.ToString());
        }

        BlockedGeneration = Max(BlockedGeneration, record.GetBlockedGeneration());

        Y_VERIFY(record.ResultSize() > 0, "ev# %s vdisk# %s", ev.ToString().data(), vdisk.ToString().data());
        std::cout << "[OnVGetResult] result size: " << (ui32)record.ResultSize() << "\n";
        for (ui32 i = 0, e = (ui32)record.ResultSize(); i != e; ++i) {
            const NKikimrBlobStorage::TQueryResult &result = record.GetResult(i);
            std::cout << "[OnVGetResult] result[" << i << "]: " << result.ShortDebugString().data() << "\n";
            Y_VERIFY(result.HasStatus());
            const NKikimrProto::EReplyStatus replyStatus = result.GetStatus();
            Y_VERIFY(result.HasCookie());
            const ui64 cookie = result.GetCookie();
            Y_VERIFY(result.HasBlobID());
            const TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(result.GetBlobID());

            if (ReportDetailedPartMap) {
                Blackboard.ReportPartMapStatus(blobId,
                    Blackboard.GroupDiskRequests.DiskRequestsForOrderNumber[orderNumber].GetsToSend[cookie].PartMapIndex,
                    ResponseIndex,
                    replyStatus);
            }

            TString resultBuffer = result.HasBuffer() ? result.GetBuffer() : TString();
            ui32 resultShift = result.HasShift() ? result.GetShift() : 0;

            // Currently CRC can be checked only if blob part is fully read
            if (resultShift == 0 && resultBuffer.size() == Info->Type.PartSize(blobId)) {
                bool isCrcOk = CheckCrcAtTheEnd((TErasureType::ECrcMode)blobId.CrcMode(), resultBuffer);
                if (!isCrcOk) {
                    R_LOG_ERROR_SX(logCtx, "BPG66", "Error in CheckCrcAtTheEnd on TEvVGetResult, blobId# " << blobId
                            << " resultShift# " << resultShift << " resultBuffer.Size()# " << resultBuffer.size());
                    NKikimrBlobStorage::TQueryResult *mutableResult = ev.Record.MutableResult(i);
                    mutableResult->SetStatus(NKikimrProto::ERROR);
                }
            }

            if (replyStatus == NKikimrProto::OK) {
                // TODO(cthulhu): Verify shift and response size, and cookie
                R_LOG_DEBUG_SX(logCtx, "BPG58", "Got# OK orderNumber# " << orderNumber << " vDiskId# " << vdisk.ToString());
                Blackboard.AddResponseData(blobId, orderNumber, resultShift, resultBuffer, result.GetKeep(), result.GetDoNotKeep());
            } else if (replyStatus == NKikimrProto::NODATA) {
                R_LOG_DEBUG_SX(logCtx, "BPG59", "Got# NODATA orderNumber# " << orderNumber
                        << " vDiskId# " << vdisk.ToString());
                Blackboard.AddNoDataResponse(blobId, orderNumber);
            } else if (replyStatus == NKikimrProto::ERROR
                    || replyStatus == NKikimrProto::VDISK_ERROR_STATE
                    || replyStatus == NKikimrProto::CORRUPTED) {
                R_LOG_DEBUG_SX(logCtx, "BPG60", "Got# " << NKikimrProto::EReplyStatus_Name(replyStatus).data()
                    << " orderNumber# " << orderNumber << " vDiskId# " << vdisk.ToString());
                Blackboard.AddErrorResponse(blobId, orderNumber);
            } else if (replyStatus == NKikimrProto::NOT_YET) {
                R_LOG_DEBUG_SX(logCtx, "BPG67", "Got# NOT_YET orderNumber# " << orderNumber
                        << " vDiskId# " << vdisk.ToString());
                Blackboard.AddNotYetResponse(blobId, orderNumber, result.GetKeep(), result.GetDoNotKeep());
            } else {
                Y_VERIFY(false, "Unexpected reply status# %s", NKikimrProto::EReplyStatus_Name(replyStatus).data());
            }
        }

        ++ResponseIndex;

        Step(logCtx, outVGets, outVPuts, outGetResult);
    }

    void OnVPutResult(TLogContext &logCtx, TEvBlobStorage::TEvVPutResult &ev,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &outVGets,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> &outVPuts,
            TAutoPtr<TEvBlobStorage::TEvGetResult> &outGetResult);

    void OnVPutResult(TLogContext &logCtx, TEvBlobStorage::TEvVMultiPutResult &ev,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &outVGets,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVMultiPut>> &outVMultiPuts,
            TAutoPtr<TEvBlobStorage::TEvGetResult> &outGetResult);

    void PrepareReply(NKikimrProto::EReplyStatus status, TString errorReason, TLogContext &logCtx,
            TAutoPtr<TEvBlobStorage::TEvGetResult> &outGetResult);

    template <typename TVPutEvent>
    void AccelerateGet(TLogContext &logCtx, i32 slowDiskOrderNumber,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &outVGets, TDeque<std::unique_ptr<TVPutEvent>> &outVPuts) {
        TAutoPtr<TEvBlobStorage::TEvGetResult> outGetResult;
        TBlackboard::EAccelerationMode prevMode = Blackboard.AccelerationMode;
        Blackboard.AccelerationMode = TBlackboard::AccelerationModeSkipMarked;
        for (auto it = Blackboard.BlobStates.begin(); it != Blackboard.BlobStates.end(); ++it) {
            TStackVec<TBlobState::TDisk, TypicalDisksInSubring> &disks = it->second.Disks;
            for (ui32 i = 0; i < disks.size(); ++i) {
                TBlobState::TDisk &disk = disks[i];
                disk.IsSlow = ((i32)disk.OrderNumber == slowDiskOrderNumber);
            }
        }
        Blackboard.ChangeAll();
        Step(logCtx, outVGets, outVPuts, outGetResult);
        Blackboard.AccelerationMode = prevMode;
        Y_VERIFY(!outGetResult, "%s Unexpected get result in AccelerateGet, outGetResult# %s, DumpFullState# %s",
            RequestPrefix.data(), outGetResult->Print(false).c_str(), DumpFullState().c_str());
    }

    template <typename TVPutEvent>
    void AcceleratePut(TLogContext &logCtx, i32 slowDiskOrderNumber,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &outVGets, TDeque<std::unique_ptr<TVPutEvent>> &outVPuts) {
        AccelerateGet(logCtx, slowDiskOrderNumber, outVGets, outVPuts);
    }

    ui64 GetTimeToAccelerateGetNs(TLogContext &logCtx);
    ui64 GetTimeToAcceleratePutNs(TLogContext &logCtx);

    TString DumpFullState() const;

protected:
    EStrategyOutcome RunBoldStrategy(TLogContext &logCtx);
    EStrategyOutcome RunMirror3dcStrategy(TLogContext &logCtx);
    EStrategyOutcome RunMirror3of4Strategy(TLogContext &logCtx);
    EStrategyOutcome RunStrategies(TLogContext &logCtx);

    // Returns true if there are additional requests to send
    template <typename TVPutEvent>
    bool Step(TLogContext &logCtx, TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &outVGets,
            TDeque<std::unique_ptr<TVPutEvent>> &outVPuts, TAutoPtr<TEvBlobStorage::TEvGetResult> &outGetResult) {
        switch (auto outcome = RunStrategies(logCtx)) {
            case EStrategyOutcome::IN_PROGRESS: {
                const ui32 numRequests = outVGets.size() + outVPuts.size();
                PrepareRequests(logCtx, outVGets);
                PrepareVPuts(logCtx, outVPuts);
                return outVGets.size() + outVPuts.size() > numRequests;
            }

            case EStrategyOutcome::ERROR:
                PrepareReply(NKikimrProto::ERROR, outcome.ErrorReason, logCtx, outGetResult);
                return false;

            case EStrategyOutcome::DONE:
                PrepareReply(NKikimrProto::OK, "", logCtx, outGetResult);
                return false;
        }
    }

    void PrepareRequests(TLogContext &logCtx,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &outVGets);
    void PrepareVPuts(TLogContext &logCtx,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> &outVPuts);
    void PrepareVPuts(TLogContext &logCtx,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVMultiPut>> &outVMultiPuts);

    ui64 GetTimeToAccelerateNs(TLogContext &logCtx, NKikimrBlobStorage::EVDiskQueueId queueId);
}; //TGetImpl

}//NKikimr
