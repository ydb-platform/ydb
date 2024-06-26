#pragma once

#include "dsproxy.h"
#include "dsproxy_blackboard.h"
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

    bool IsNoData = false;
    bool IsReplied = false;
    bool AcquireBlockedGeneration = false;

    TBlackboard Blackboard;

    ui32 RequestIndex = 0;
    ui32 ResponseIndex = 0;

    const TString RequestPrefix;

    const bool PhantomCheck;
    const bool Decommission;

    std::optional<TEvBlobStorage::TEvGet::TReaderTabletData> ReaderTabletData;

    std::unordered_map<TLogoBlobID, std::tuple<bool, bool>> BlobFlags; // keep, doNotKeep per blob

    const float SlowDiskThreshold = 2;

public:
    TGetImpl(const TIntrusivePtr<TBlobStorageGroupInfo> &info, const TIntrusivePtr<TGroupQueues> &groupQueues,
            TEvBlobStorage::TEvGet *ev, TNodeLayoutInfoPtr&& nodeLayout, float slowDiskThreshold,
            const TString& requestPrefix = {})
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
        , SlowDiskThreshold(slowDiskThreshold)
    {
        Y_ABORT_UNLESS(QuerySize > 0);
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
        str << '{';
        str << "MustRestoreFirst# " << MustRestoreFirst;
        for (ui32 i = 0; i < QuerySize; ++i) {
            str << ' '
                << Queries[i].Id
                << '@' << Queries[i].Shift
                << ':' << Queries[i].Size;
        }
        str << '}';
        return str.Str();
    }

    ui64 GetVPutRequests() const {
        return VPutRequests;
    }

    ui64 GetVPutResponses() const {
        return VPutResponses;
    }

    ui64 GetRequestIndex() const {
        return RequestIndex;
    }

    ui64 GetResponseIndex() const {
        return ResponseIndex;
    }

    void GenerateInitialRequests(TLogContext &logCtx, TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &outVGets);

    void OnVGetResult(TLogContext &logCtx, TEvBlobStorage::TEvVGetResult &ev,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &outVGets,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> &outVPuts,
            TAutoPtr<TEvBlobStorage::TEvGetResult> &outGetResult) {
        const NKikimrBlobStorage::TEvVGetResult &record = ev.Record;
        Y_ABORT_UNLESS(record.HasStatus());
        const NKikimrProto::EReplyStatus status = record.GetStatus();
        Y_ABORT_UNLESS(status != NKikimrProto::RACE && status != NKikimrProto::BLOCKED && status != NKikimrProto::DEADLINE);
        R_LOG_DEBUG_SX(logCtx, "BPG57", "handle result# " << ev.ToString());

        Y_ABORT_UNLESS(record.HasVDiskID());
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

        Y_ABORT_UNLESS(record.ResultSize() > 0, "ev# %s vdisk# %s", ev.ToString().data(), vdisk.ToString().data());
        for (ui32 i = 0, e = (ui32)record.ResultSize(); i != e; ++i) {
            const NKikimrBlobStorage::TQueryResult &result = record.GetResult(i);
            Y_ABORT_UNLESS(result.HasStatus());
            const NKikimrProto::EReplyStatus replyStatus = result.GetStatus();
            Y_ABORT_UNLESS(result.HasBlobID());
            const TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(result.GetBlobID());

            if (ReportDetailedPartMap) {
                Blackboard.ReportPartMapStatus(blobId, result.GetCookie(), ResponseIndex, replyStatus);
            }

            TRope resultBuffer = ev.GetBlobData(result);
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

            if (result.HasKeep() || result.HasDoNotKeep()) {
                auto& [a, b] = BlobFlags[blobId];
                a |= result.GetKeep();
                b |= result.GetDoNotKeep();
            }

            if (replyStatus == NKikimrProto::OK) {
                // TODO(cthulhu): Verify shift and response size, and cookie
                R_LOG_DEBUG_SX(logCtx, "BPG58", "Got# OK orderNumber# " << orderNumber << " vDiskId# " << vdisk.ToString());
                resultBuffer.Compact();
                if (resultBuffer.GetOccupiedMemorySize() > resultBuffer.size() * 2) {
                    auto temp = TRcBuf::Uninitialized(resultBuffer.size());
                    resultBuffer.ExtractFrontPlain(temp.GetDataMut(), temp.size());
                    resultBuffer.Insert(resultBuffer.End(), std::move(temp));
                }
                Blackboard.AddResponseData(blobId, orderNumber, resultShift, std::move(resultBuffer));
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
                Blackboard.AddNotYetResponse(blobId, orderNumber);
            } else {
                Y_ABORT_UNLESS(false, "Unexpected reply status# %s", NKikimrProto::EReplyStatus_Name(replyStatus).data());
            }
        }

        ++ResponseIndex;

        Step(logCtx, outVGets, outVPuts, outGetResult);
    }

    void OnVPutResult(TLogContext &logCtx, TEvBlobStorage::TEvVPutResult &ev,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &outVGets,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> &outVPuts,
            TAutoPtr<TEvBlobStorage::TEvGetResult> &outGetResult);

    void PrepareReply(NKikimrProto::EReplyStatus status, TString errorReason, TLogContext &logCtx,
            TAutoPtr<TEvBlobStorage::TEvGetResult> &outGetResult);

    void AccelerateGet(TLogContext &logCtx, ui32 slowDisksMask,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &outVGets,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> &outVPuts) {
        TAutoPtr<TEvBlobStorage::TEvGetResult> outGetResult;
        TBlackboard::EAccelerationMode prevMode = Blackboard.AccelerationMode;
        Blackboard.AccelerationMode = TBlackboard::AccelerationModeSkipMarked;
        for (auto it = Blackboard.BlobStates.begin(); it != Blackboard.BlobStates.end(); ++it) {
            TStackVec<TBlobState::TDisk, TypicalDisksInSubring> &disks = it->second.Disks;
            for (ui32 i = 0; i < disks.size(); ++i) {
                TBlobState::TDisk &disk = disks[i];
                disk.IsSlow = slowDisksMask & (1 << disk.OrderNumber);
            }
        }
        Blackboard.ChangeAll();
        Step(logCtx, outVGets, outVPuts, outGetResult);
        Blackboard.AccelerationMode = prevMode;
        Y_ABORT_UNLESS(!outGetResult, "%s Unexpected get result in AccelerateGet, outGetResult# %s, DumpFullState# %s",
            RequestPrefix.data(), outGetResult->Print(false).c_str(), DumpFullState().c_str());
    }

    void AcceleratePut(TLogContext &logCtx, ui32 slowDisksMask,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &outVGets,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> &outVPuts) {
        AccelerateGet(logCtx, slowDisksMask, outVGets, outVPuts);
    }

    ui64 GetTimeToAccelerateGetNs(TLogContext &logCtx, ui32 acceleratesSent);
    ui64 GetTimeToAcceleratePutNs(TLogContext &logCtx, ui32 acceleratesSent);

    TString DumpFullState() const;

protected:
    EStrategyOutcome RunBoldStrategy(TLogContext &logCtx);
    EStrategyOutcome RunMirror3dcStrategy(TLogContext &logCtx);
    EStrategyOutcome RunMirror3of4Strategy(TLogContext &logCtx);
    EStrategyOutcome RunStrategies(TLogContext &logCtx);

    // Returns true if there are additional requests to send
    bool Step(TLogContext &logCtx, TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &outVGets,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> &outVPuts,
            TAutoPtr<TEvBlobStorage::TEvGetResult> &outGetResult) {
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

    ui64 GetTimeToAccelerateNs(TLogContext &logCtx, NKikimrBlobStorage::EVDiskQueueId queueId, ui32 nthWorst);
}; //TGetImpl

}//NKikimr
