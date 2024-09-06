#pragma once
#include "defs.h"

#include <ydb/core/blobstorage/base/batched_vec.h>
#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////////////////////
    // ResultingCounterForEvent -- returns dynamic counter *ResMsgs for an event
    ////////////////////////////////////////////////////////////////////////////////////////////
    template <class TEvent>
    inline const ::NMonitoring::TDynamicCounters::TCounterPtr &ResultingCounterForEvent(const TVDiskContextPtr &,
                TEvent &) {
        static ::NMonitoring::TDynamicCounters::TCounterPtr dummy;
        return dummy;
    }

    template <>
    inline const ::NMonitoring::TDynamicCounters::TCounterPtr &ResultingCounterForEvent(const TVDiskContextPtr &vctx,
                TEvBlobStorage::TEvVPut::TPtr &) {
        return vctx->IFaceMonGroup->PutResMsgsPtr();
    }

    template <>
    inline const ::NMonitoring::TDynamicCounters::TCounterPtr &ResultingCounterForEvent(const TVDiskContextPtr &vctx,
                TEvBlobStorage::TEvVMultiPut::TPtr &) {
        return vctx->IFaceMonGroup->MultiPutResMsgsPtr();
    }

    template <>
    inline const ::NMonitoring::TDynamicCounters::TCounterPtr &ResultingCounterForEvent(const TVDiskContextPtr &vctx,
                TEvBlobStorage::TEvVGet::TPtr &) {
        return vctx->IFaceMonGroup->GetResMsgsPtr();
    }

    inline const ::NMonitoring::TDynamicCounters::TCounterPtr &ResultingCounterForEvent(const TVDiskContextPtr &vctx,
                TEvBlobStorage::TEvVBlock::TPtr &) {
        return vctx->IFaceMonGroup->BlockResMsgsPtr();
    }

    inline const ::NMonitoring::TDynamicCounters::TCounterPtr &ResultingCounterForEvent(const TVDiskContextPtr &vctx,
                TEvBlobStorage::TEvVCollectGarbage::TPtr &) {
        return vctx->IFaceMonGroup->GCResMsgsPtr();
    }

    inline const ::NMonitoring::TDynamicCounters::TCounterPtr &ResultingCounterForEvent(const TVDiskContextPtr &vctx,
                TEvBlobStorage::TEvVGetBarrier::TPtr &) {
        return vctx->IFaceMonGroup->GetBarrierResMsgsPtr();
    }

    inline const ::NMonitoring::TDynamicCounters::TCounterPtr &ResultingCounterForEvent(const TVDiskContextPtr &vctx,
                TEvBlobStorage::TEvVMovedPatch::TPtr &) {
        return vctx->IFaceMonGroup->MovedPatchMsgsPtr();
    }

    inline const ::NMonitoring::TDynamicCounters::TCounterPtr &ResultingCounterForEvent(const TVDiskContextPtr &vctx,
                TEvBlobStorage::TEvVPatchFoundParts::TPtr &) {
        return vctx->IFaceMonGroup->PatchFoundPartsMsgsPtr();
    }

    inline const ::NMonitoring::TDynamicCounters::TCounterPtr &ResultingCounterForEvent(const TVDiskContextPtr &vctx,
                TEvBlobStorage::TEvVPatchXorDiffResult::TPtr &) {
        return vctx->IFaceMonGroup->PatchXorDiffResMsgsPtr();
    }

    inline const ::NMonitoring::TDynamicCounters::TCounterPtr &ResultingCounterForEvent(const TVDiskContextPtr &vctx,
                TEvBlobStorage::TEvVPatchResult::TPtr &) {
        return vctx->IFaceMonGroup->PatchResMsgsPtr();
    }

    template<typename TRequest, typename TResponse>
    inline void SetRacingGroupInfo(const TRequest& request, TResponse& response,
            const TIntrusivePtr<TBlobStorageGroupInfo>& groupInfo) {
        if (response.GetStatus() == NKikimrProto::RACE && groupInfo->Group) {
            const TVDiskID& vdiskId = VDiskIDFromVDiskID(request.GetVDiskID());
            if (vdiskId.GroupID == groupInfo->GroupID && vdiskId.GroupGeneration < groupInfo->GroupGeneration) {
                response.MutableRecentGroup()->CopyFrom(*groupInfo->Group);
            }
        }
    }

    inline void SetRacingGroupInfo(const NKikimrBlobStorage::TEvVPatchXorDiff&,
            NKikimrBlobStorage::TEvVPatchXorDiffResult&, const TIntrusivePtr<TBlobStorageGroupInfo>&)
    {}


    ////////////////////////////////////////////////////////////////////////////////////////////
    // CreateResult -- create result from original message and status
    ////////////////////////////////////////////////////////////////////////////////////////////
    static inline std::unique_ptr<TEvBlobStorage::TEvVMovedPatchResult>
    CreateResult(const TVDiskContextPtr &vctx, const NKikimrProto::EReplyStatus status, const TString& errorReason,
            TEvBlobStorage::TEvVMovedPatch::TPtr &ev, const TInstant &now, const TActorIDPtr &skeletonFrontIDPtr,
            const TVDiskID &vdiskID, ui64 vdiskIncarnationGuid) {
        NKikimrBlobStorage::TEvVMovedPatch &record = ev->Get()->Record;
        TLogoBlobID originalId = LogoBlobIDFromLogoBlobID(record.GetOriginalBlobId());
        TLogoBlobID patchedId = LogoBlobIDFromLogoBlobID(record.GetPatchedBlobId());
        TMaybe<ui64> cookie;
        if (record.HasCookie()) {
            cookie = record.GetCookie();
        }
        const auto oosStatus = vctx->GetOutOfSpaceState().GetGlobalStatusFlags();
        const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr = ResultingCounterForEvent(vctx, ev);

        return std::make_unique<TEvBlobStorage::TEvVMovedPatchResult>(status, originalId, patchedId, vdiskID, cookie,
            oosStatus, now, ev->Get()->GetCachedByteSize(), &record, skeletonFrontIDPtr, counterPtr, nullptr,
            vdiskIncarnationGuid, errorReason);
    }

    static inline std::unique_ptr<TEvBlobStorage::TEvVPatchFoundParts>
    CreateResult(const TVDiskContextPtr &vctx, const NKikimrProto::EReplyStatus status, const TString& errorReason,
            TEvBlobStorage::TEvVPatchStart::TPtr &ev, const TInstant &now, const TActorIDPtr &skeletonFrontIDPtr,
            const TVDiskID &vdiskID, ui64 vdiskIncarnationGuid) {
        NKikimrBlobStorage::TEvVPatchStart &record = ev->Get()->Record;
        TLogoBlobID originalId = LogoBlobIDFromLogoBlobID(record.GetOriginalBlobId());
        TLogoBlobID patchedId = LogoBlobIDFromLogoBlobID(record.GetPatchedBlobId());
        TMaybe<ui64> cookie;
        if (record.HasCookie()) {
            cookie = record.GetCookie();
        }
        const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr = ResultingCounterForEvent(vctx, ev);

        return std::make_unique<TEvBlobStorage::TEvVPatchFoundParts>(status, originalId, patchedId, vdiskID, cookie, now,
            errorReason, &record, skeletonFrontIDPtr, counterPtr, nullptr, vdiskIncarnationGuid);
    }

    static inline std::unique_ptr<TEvBlobStorage::TEvVPatchResult>
    CreateResult(const TVDiskContextPtr &vctx, const NKikimrProto::EReplyStatus status, const TString& errorReason,
            TEvBlobStorage::TEvVPatchDiff::TPtr &ev, const TInstant &now, const TActorIDPtr &skeletonFrontIDPtr,
            const TVDiskID &vdiskID, ui64 vdiskIncarnationGuid) {

        auto &record = ev->Get()->Record;
        TLogoBlobID originalId = LogoBlobIDFromLogoBlobID(record.GetOriginalPartBlobId());
        TLogoBlobID patchedId = LogoBlobIDFromLogoBlobID(record.GetPatchedPartBlobId());
        TMaybe<ui64> cookie;
        if (record.HasCookie()) {
            cookie = record.GetCookie();
        }
        const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr = ResultingCounterForEvent(vctx, ev);

        auto res = std::make_unique<TEvBlobStorage::TEvVPatchResult>(status, originalId, patchedId, vdiskID, cookie,
            now, &record, skeletonFrontIDPtr, counterPtr, nullptr, vdiskIncarnationGuid);
        res->SetStatus(status, errorReason);
        return res;
    }

    static inline std::unique_ptr<TEvBlobStorage::TEvVPatchXorDiffResult>
    CreateResult(const TVDiskContextPtr &vctx, const NKikimrProto::EReplyStatus status, const TString& errorReason,
            TEvBlobStorage::TEvVPatchXorDiff::TPtr &ev, const TInstant &now, const TActorIDPtr &skeletonFrontIDPtr,
            const TVDiskID &vdiskID, ui64 vdiskIncarnationGuid) {

        Y_UNUSED(errorReason, vdiskID, vdiskIncarnationGuid);
        auto &record = ev->Get()->Record;
        const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr = ResultingCounterForEvent(vctx, ev);
        return std::make_unique<TEvBlobStorage::TEvVPatchXorDiffResult>(status, now, &record, skeletonFrontIDPtr,
            counterPtr, nullptr);
    }

    static inline std::unique_ptr<TEvBlobStorage::TEvVPutResult>
    CreateResult(const TVDiskContextPtr &vctx, const NKikimrProto::EReplyStatus status, const TString& errorReason,
            TEvBlobStorage::TEvVPut::TPtr &ev, const TInstant &now, const TActorIDPtr &skeletonFrontIDPtr,
            const TVDiskID &vdiskID, ui64 vdiskIncarnationGuid) {

        NKikimrBlobStorage::TEvVPut &record = ev->Get()->Record;
        const TLogoBlobID id = LogoBlobIDFromLogoBlobID(record.GetBlobID());
        const ui64 bufferSizeBytes = ev->Get()->GetBufferBytes();
        const ui64 vcookie = record.GetCookie();
        const ui64 *cookie = record.HasCookie() ? &vcookie : nullptr;
        const auto oosStatus = vctx->GetOutOfSpaceState().GetGlobalStatusFlags();
        const auto handleClass = record.GetHandleClass();
        const NVDiskMon::TLtcHistoPtr &histoPtr = vctx->Histograms.GetHistogram(handleClass);
        const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr = ResultingCounterForEvent(vctx, ev);

        return std::make_unique<TEvBlobStorage::TEvVPutResult>(status, id, vdiskID, cookie, oosStatus, now,
                ev->Get()->GetCachedByteSize(), &record, skeletonFrontIDPtr, counterPtr, histoPtr,
                bufferSizeBytes, vdiskIncarnationGuid, errorReason);
    }

    static inline std::unique_ptr<TEvBlobStorage::TEvVPutResult>
    CreateInternalResult(const TVDiskContextPtr &vctx, const NKikimrProto::EReplyStatus status, const TString& errorReason,
            TEvBlobStorage::TEvVPut::TPtr &ev, const TInstant &now,
            const TVDiskID &vdiskID, ui64 vdiskIncarnationGuid) {

        NKikimrBlobStorage::TEvVPut &record = ev->Get()->Record;
        const TLogoBlobID id = LogoBlobIDFromLogoBlobID(record.GetBlobID());
        const ui64 bufferSizeBytes = ev->Get()->GetBufferBytes();
        const ui64 vcookie = record.GetCookie();
        const ui64 *cookie = record.HasCookie() ? &vcookie : nullptr;
        const auto oosStatus = vctx->GetOutOfSpaceState().GetGlobalStatusFlags();

        return std::make_unique<TEvBlobStorage::TEvVPutResult>(status, id, vdiskID, cookie, oosStatus, now,
                ev->Get()->GetCachedByteSize(), &ev->Get()->Record, nullptr, nullptr, nullptr,
                bufferSizeBytes, vdiskIncarnationGuid, errorReason);
    }

    static inline std::unique_ptr<TEvBlobStorage::TEvVBlockResult>
    CreateResult(const TVDiskContextPtr &vctx, const NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
            const TEvBlobStorage::TEvVBlockResult::TTabletActGen *actual,
            TEvBlobStorage::TEvVBlock::TPtr &ev, const TInstant &now, const TActorIDPtr &skeletonFrontIDPtr,
            const TVDiskID &vdiskID, ui64 vdiskIncarnationGuid)
    {
        const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr = ResultingCounterForEvent(vctx, ev);
        return std::make_unique<TEvBlobStorage::TEvVBlockResult>(status, actual, vdiskID, now,
            ev->Get()->GetCachedByteSize(), &ev->Get()->Record, skeletonFrontIDPtr, counterPtr,
            nullptr, vdiskIncarnationGuid);
    }

    static inline std::unique_ptr<TEvBlobStorage::TEvVCollectGarbageResult>
    CreateResult(const TVDiskContextPtr &vctx, const NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
            TEvBlobStorage::TEvVCollectGarbage::TPtr &ev, const TInstant &now,
            const TActorIDPtr &skeletonFrontIDPtr, const TVDiskID &vdiskID, ui64 vdiskIncarnationGuid)
    {
        NKikimrBlobStorage::TEvVCollectGarbage &record = ev->Get()->Record;
        const auto tabletId = record.GetTabletId();
        const auto gen = record.GetRecordGeneration();
        const auto channel = record.GetChannel();
        const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr = ResultingCounterForEvent(vctx, ev);
        return std::make_unique<TEvBlobStorage::TEvVCollectGarbageResult>(status, tabletId, gen, channel, vdiskID, now,
                ev->Get()->GetCachedByteSize(), &record, skeletonFrontIDPtr, counterPtr, nullptr,
                vdiskIncarnationGuid);
    }

    ////////////////////////////////////////////////////////////////////////////
    // NErrBuilder -- routines for error results
    ////////////////////////////////////////////////////////////////////////////
    namespace NErrBuilder {
        template <typename TEvPtr>
        static inline std::unique_ptr<IEventBase>
        ErroneousResult(const TVDiskContextPtr &vctx, const NKikimrProto::EReplyStatus status, const TString& errorReason,
                        TEvPtr &ev, const TInstant &now, const TActorIDPtr &skeletonFrontIDPtr,
                        const TVDiskID &vdiskID, ui64 vdiskIncarnationGuid,
                        const TIntrusivePtr<TBlobStorageGroupInfo>& groupInfo)
        {
            if constexpr (std::is_same_v<TEvPtr, TEvBlobStorage::TEvVMovedPatch::TPtr>) {
                TLogoBlobID id = LogoBlobIDFromLogoBlobID(ev->Get()->Record.GetOriginalBlobId());
                LWTRACK(VDiskSkeletonFrontVMovedPatchRecieved, ev->Get()->Orbit, vctx->NodeId, vctx->GroupId.GetRawId(),
                    vctx->Top->GetFailDomainOrderNumber(vctx->ShortSelfVDisk), id.TabletID(), id.BlobSize());
            }
            if constexpr (std::is_same_v<TEvPtr, TEvBlobStorage::TEvVPatchStart::TPtr>) {
                TLogoBlobID id = LogoBlobIDFromLogoBlobID(ev->Get()->Record.GetOriginalBlobId());
                LWTRACK(VDiskSkeletonFrontVPatchStartRecieved, ev->Get()->Orbit, vctx->NodeId, vctx->GroupId.GetRawId(),
                    vctx->Top->GetFailDomainOrderNumber(vctx->ShortSelfVDisk), id.TabletID(), id.BlobSize());
            }
            if constexpr (std::is_same_v<TEvPtr, TEvBlobStorage::TEvVPatchDiff::TPtr>) {
                TLogoBlobID id = LogoBlobIDFromLogoBlobID(ev->Get()->Record.GetOriginalPartBlobId());
                LWTRACK(VDiskSkeletonFrontVPatchDiffRecieved, ev->Get()->Orbit, vctx->NodeId, vctx->GroupId.GetRawId(),
                    vctx->Top->GetFailDomainOrderNumber(vctx->ShortSelfVDisk), id.TabletID(), id.BlobSize());
            }
            if constexpr (std::is_same_v<TEvPtr, TEvBlobStorage::TEvVPatchXorDiff::TPtr>) {
                TLogoBlobID id = LogoBlobIDFromLogoBlobID(ev->Get()->Record.GetOriginalPartBlobId());
                LWTRACK(VDiskSkeletonFrontVPatchXorDiffRecieved, ev->Get()->Orbit, vctx->NodeId, vctx->GroupId.GetRawId(),
                    vctx->Top->GetFailDomainOrderNumber(vctx->ShortSelfVDisk), id.TabletID(), id.BlobSize());
            }
            if constexpr (std::is_same_v<TEvPtr, TEvBlobStorage::TEvVPut::TPtr>) {
                TLogoBlobID id = LogoBlobIDFromLogoBlobID(ev->Get()->Record.GetBlobID());
                LWTRACK(VDiskSkeletonFrontVPutRecieved, ev->Get()->Orbit, vctx->NodeId, vctx->GroupId.GetRawId(),
                    vctx->Top->GetFailDomainOrderNumber(vctx->ShortSelfVDisk), id.TabletID(), id.BlobSize());
            }

            auto result = CreateResult(vctx, status, errorReason, ev, now, skeletonFrontIDPtr, vdiskID,
                vdiskIncarnationGuid);
            SetRacingGroupInfo(ev->Get()->Record, result->Record, groupInfo);
            return result;
        }

        static inline std::unique_ptr<IEventBase>
        ErroneousResult(const TVDiskContextPtr &vctx, const NKikimrProto::EReplyStatus status, const TString& errorReason,
                        TEvBlobStorage::TEvVMultiPut::TPtr &ev, const TInstant &now,
                        const TActorIDPtr &skeletonFrontIDPtr, const TVDiskID &vdiskID,
                        const TBatchedVec<NKikimrProto::EReplyStatus> &statuses, ui64 vdiskIncarnationGuid,
                        const TIntrusivePtr<TBlobStorageGroupInfo>& groupInfo)
        {
            NKikimrBlobStorage::TEvVMultiPut &record = ev->Get()->Record;
            LWTRACK(VDiskSkeletonFrontVMultiPutRecieved, ev->Get()->Orbit, vctx->NodeId, vctx->GroupId.GetRawId(),
                   vctx->Top->GetFailDomainOrderNumber(vctx->ShortSelfVDisk), ev->Get()->Record.ItemsSize(),
                   ev->Get()->GetBufferBytes());
            const ui64 vcookie = record.GetCookie();
            const ui64 *cookie = record.HasCookie() ? &vcookie : nullptr;
            const ui64 bufferSizeBytes = ev->Get()->GetBufferBytes();
            const auto handleClass = record.GetHandleClass();
            const NVDiskMon::TLtcHistoPtr &histoPtr = vctx->Histograms.GetHistogram(handleClass);
            const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr = ResultingCounterForEvent(vctx, ev);
            auto result = std::make_unique<TEvBlobStorage::TEvVMultiPutResult>(status, vdiskID, cookie, now,
                ev->Get()->GetCachedByteSize(), &record, skeletonFrontIDPtr, counterPtr, histoPtr, bufferSizeBytes,
                vdiskIncarnationGuid, errorReason);
            Y_ABORT_UNLESS(record.ItemsSize() == statuses.size());
            for (ui64 itemIdx = 0; itemIdx < record.ItemsSize(); ++itemIdx) {
                auto &item = record.GetItems(itemIdx);
                ui64 cookieValue = 0;
                ui64 *cookiePtr = nullptr;
                if (item.HasCookie()) {
                    cookieValue = item.GetCookie();
                    cookiePtr = &cookieValue;
                }
                TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(item.GetBlobID());
                result->AddVPutResult(statuses[itemIdx], errorReason, blobId, cookiePtr);
            }
            SetRacingGroupInfo(ev->Get()->Record, result->Record, groupInfo);
            return result;
        }

        static inline std::unique_ptr<IEventBase>
        ErroneousResult(const TVDiskContextPtr &vctx, const NKikimrProto::EReplyStatus status, const TString& errorReason,
                        TEvBlobStorage::TEvVMultiPut::TPtr &ev, const TInstant &now,
                        const TActorIDPtr &skeletonFrontIDPtr,
                        const TVDiskID &vdiskID, ui64 vdiskIncarnationGuid,
                        const TIntrusivePtr<TBlobStorageGroupInfo>& groupInfo)
        {
            NKikimrBlobStorage::TEvVMultiPut &record = ev->Get()->Record;
            TBatchedVec<NKikimrProto::EReplyStatus> statuses(record.ItemsSize(), status);
            return ErroneousResult(vctx, status, errorReason, ev, now, skeletonFrontIDPtr, vdiskID, statuses,
                vdiskIncarnationGuid, groupInfo);
        }

        static inline std::unique_ptr<IEventBase>
        ErroneousResult(const TVDiskContextPtr &vctx, const NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
                        TEvBlobStorage::TEvVGet::TPtr &ev, const TInstant &now, const TActorIDPtr &skeletonFrontIDPtr,
                        const TVDiskID &vdiskID, ui64 vdiskIncarnationGuid,
                        const TIntrusivePtr<TBlobStorageGroupInfo>& groupInfo)
        {
            NKikimrBlobStorage::TEvVGet &record = ev->Get()->Record;
            TMaybe<ui64> cookie;
            if (record.HasCookie())
                cookie = record.GetCookie();
            const auto handleClass = record.GetHandleClass();
            const NVDiskMon::TLtcHistoPtr &histoPtr = vctx->Histograms.GetHistogram(handleClass);
            const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr = ResultingCounterForEvent(vctx, ev);
            auto result = std::make_unique<TEvBlobStorage::TEvVGetResult>(status, vdiskID, now,
                ev->Get()->GetCachedByteSize(), &record, skeletonFrontIDPtr, counterPtr, histoPtr,
                cookie, ev->GetChannel(), vdiskIncarnationGuid);
            // extreme queries
            for (ui32 i = 0, e = (ui32)record.ExtremeQueriesSize(); i != e; ++i) {
                const NKikimrBlobStorage::TExtremeQuery *q = &record.GetExtremeQueries(i);
                const TLogoBlobID first = LogoBlobIDFromLogoBlobID(q->GetId());
                const ui64 vcookie = q->GetCookie();
                const ui64 *cookie = q->HasCookie() ? &vcookie : nullptr;
                result->AddResult(status, first, cookie);
            }
            SetRacingGroupInfo(ev->Get()->Record, result->Record, groupInfo);
            return result;
        }

        static inline std::unique_ptr<IEventBase>
        ErroneousResult(const TVDiskContextPtr &vctx, const NKikimrProto::EReplyStatus status, const TString& errorReason,
                        TEvBlobStorage::TEvVBlock::TPtr &ev, const TInstant &now, const TActorIDPtr &skeletonFrontIDPtr,
                        const TVDiskID &vdiskID, ui64 vdiskIncarnationGuid,
                        const TIntrusivePtr<TBlobStorageGroupInfo>& groupInfo)
        {
            auto result = CreateResult(vctx, status, errorReason, nullptr, ev, now, skeletonFrontIDPtr, vdiskID,
                vdiskIncarnationGuid);
            SetRacingGroupInfo(ev->Get()->Record, result->Record, groupInfo);
            return result;
        }

        static inline std::unique_ptr<IEventBase>
        ErroneousResult(const TVDiskContextPtr &vctx, const NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
                        TEvBlobStorage::TEvVGetBlock::TPtr &ev, const TInstant &now,
                        const TActorIDPtr &skeletonFrontIDPtr, const TVDiskID &vdiskID, ui64 vdiskIncarnationGuid,
                        const TIntrusivePtr<TBlobStorageGroupInfo>& groupInfo)
        {
            Y_UNUSED(vdiskIncarnationGuid);
            const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr = ResultingCounterForEvent(vctx, ev);
            auto result = std::make_unique<TEvBlobStorage::TEvVGetBlockResult>(status, ev->Get()->Record.GetTabletId(),
                vdiskID, now, ev->Get()->GetCachedByteSize(), &ev->Get()->Record, skeletonFrontIDPtr, counterPtr,
                nullptr);
            SetRacingGroupInfo(ev->Get()->Record, result->Record, groupInfo);
            return result;
        }

        static inline std::unique_ptr<IEventBase>
        ErroneousResult(const TVDiskContextPtr &vctx, const NKikimrProto::EReplyStatus status, const TString& errorReason,
                        TEvBlobStorage::TEvVCollectGarbage::TPtr &ev, const TInstant &now,
                        const TActorIDPtr &skeletonFrontIDPtr, const TVDiskID &vdiskID, ui64 vdiskIncarnationGuid,
                        const TIntrusivePtr<TBlobStorageGroupInfo>& groupInfo)
        {
            auto result = CreateResult(vctx, status, errorReason, ev, now, skeletonFrontIDPtr, vdiskID,
                vdiskIncarnationGuid);
            SetRacingGroupInfo(ev->Get()->Record, result->Record, groupInfo);
            return result;
        }

        static inline std::unique_ptr<IEventBase>
        ErroneousResult(const TVDiskContextPtr &vctx, const NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
                        TEvBlobStorage::TEvVGetBarrier::TPtr &ev, const TInstant &now,
                        const TActorIDPtr &skeletonFrontIDPtr, const TVDiskID &vdiskID, ui64 vdiskIncarnationGuid,
                        const TIntrusivePtr<TBlobStorageGroupInfo>& groupInfo)
        {
            Y_UNUSED(vdiskIncarnationGuid);
            const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr = ResultingCounterForEvent(vctx, ev);
            auto result = std::make_unique<TEvBlobStorage::TEvVGetBarrierResult>(status,vdiskID, now,
                ev->Get()->GetCachedByteSize(), &ev->Get()->Record, skeletonFrontIDPtr, counterPtr, nullptr);
            SetRacingGroupInfo(ev->Get()->Record, result->Record, groupInfo);
            return result;
        }

        static inline std::unique_ptr<IEventBase>
        ErroneousResult(const TVDiskContextPtr &vctx, const NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
                        TEvBlobStorage::TEvVDbStat::TPtr & ev, const TInstant &now,
                        const TActorIDPtr &/*skeletonFrontIDPtr*/, const TVDiskID &vdiskID, ui64 vdiskIncarnationGuid,
                        const TIntrusivePtr<TBlobStorageGroupInfo>& /*groupInfo*/)
        {
            Y_UNUSED(vdiskIncarnationGuid);
            const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr = ResultingCounterForEvent(vctx, ev);
            return std::make_unique<TEvBlobStorage::TEvVDbStatResult>(status, vdiskID, now, counterPtr, nullptr);
        }

        static inline std::unique_ptr<IEventBase>
        ErroneousResult(const TVDiskContextPtr &vctx, const NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
                        TEvBlobStorage::TEvVSync::TPtr & ev, const TInstant &now,
                        const TActorIDPtr &/*skeletonFrontIDPtr*/, const TVDiskID &vdiskID, ui64 vdiskIncarnationGuid,
                        const TIntrusivePtr<TBlobStorageGroupInfo>& /*groupInfo*/)
        {
            Y_UNUSED(vdiskIncarnationGuid);
            const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr = ResultingCounterForEvent(vctx, ev);
            auto flags = vctx->GetOutOfSpaceState().GetLocalStatusFlags();
            return std::make_unique<TEvBlobStorage::TEvVSyncResult>(status, vdiskID, flags, now, counterPtr, nullptr,
                ev->GetChannel());
        }

        static inline std::unique_ptr<IEventBase>
        ErroneousResult(const TVDiskContextPtr &vctx, const NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
                        TEvBlobStorage::TEvVSyncFull::TPtr &ev, const TInstant &now,
                        const TActorIDPtr &/*skeletonFrontIDPtr*/, const TVDiskID &vdiskID, ui64 vdiskIncarnationGuid,
                        const TIntrusivePtr<TBlobStorageGroupInfo>& /*groupInfo*/)
        {
            Y_UNUSED(vdiskIncarnationGuid);
            const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr = ResultingCounterForEvent(vctx, ev);
            ui64 cookie = ev->Get()->Record.GetCookie();
            return std::make_unique<TEvBlobStorage::TEvVSyncFullResult>(status, vdiskID, cookie, now, counterPtr, nullptr,
                ev->GetChannel());
        }

        static inline std::unique_ptr<IEventBase>
        ErroneousResult(const TVDiskContextPtr &vctx, const NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
                        TEvBlobStorage::TEvVSyncGuid::TPtr & ev, const TInstant &now,
                        const TActorIDPtr &/*skeletonFrontIDPtr*/, const TVDiskID &vdiskID, ui64 vdiskIncarnationGuid,
                        const TIntrusivePtr<TBlobStorageGroupInfo>& /*groupInfo*/)
        {
            Y_UNUSED(vdiskIncarnationGuid);
            const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr = ResultingCounterForEvent(vctx, ev);
            return std::make_unique<TEvBlobStorage::TEvVSyncGuidResult>(status, vdiskID, now, counterPtr, nullptr,
                ev->GetChannel());
        }

        static inline std::unique_ptr<IEventBase>
        ErroneousResult(const TVDiskContextPtr& /*vctx*/, const NKikimrProto::EReplyStatus status, const TString& errorReason,
                        TEvBlobStorage::TEvVAssimilate::TPtr& /*ev*/, const TInstant& /*now*/,
                        const TActorIDPtr &/*skeletonFrontIDPtr*/, const TVDiskID &vdiskID, ui64 /*vdiskIncarnationGuid*/,
                        const TIntrusivePtr<TBlobStorageGroupInfo>& /*groupInfo*/)
        {
            return std::make_unique<TEvBlobStorage::TEvVAssimilateResult>(status, errorReason, vdiskID);
        }

        static inline std::unique_ptr<IEventBase>
        ErroneousResult(const TVDiskContextPtr& /*vctx*/, const NKikimrProto::EReplyStatus status, const TString& errorReason,
                        TEvBlobStorage::TEvVTakeSnapshot::TPtr& /*ev*/, const TInstant& /*now*/,
                        const TActorIDPtr &/*skeletonFrontIDPtr*/, const TVDiskID &vdiskID, ui64 /*vdiskIncarnationGuid*/,
                        const TIntrusivePtr<TBlobStorageGroupInfo>& /*groupInfo*/)
        {
            return std::make_unique<TEvBlobStorage::TEvVTakeSnapshotResult>(status, errorReason, vdiskID);
        }

        static inline std::unique_ptr<IEventBase>
        ErroneousResult(const TVDiskContextPtr& /*vctx*/, const NKikimrProto::EReplyStatus status, const TString& errorReason,
                        TEvBlobStorage::TEvVReleaseSnapshot::TPtr& /*ev*/, const TInstant& /*now*/,
                        const TActorIDPtr &/*skeletonFrontIDPtr*/, const TVDiskID &vdiskID, ui64 /*vdiskIncarnationGuid*/,
                        const TIntrusivePtr<TBlobStorageGroupInfo>& /*groupInfo*/)
        {
            return std::make_unique<TEvBlobStorage::TEvVReleaseSnapshotResult>(status, errorReason, vdiskID);
        }

        static inline std::unique_ptr<IEventBase>
        ErroneousResult(const TVDiskContextPtr& /*vctx*/, const NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
                        TEvGetLogoBlobIndexStatRequest::TPtr& /*ev*/, const TInstant& now,
                        const TActorIDPtr &/*skeletonFrontIDPtr*/, const TVDiskID &vdiskID, ui64 /*vdiskIncarnationGuid*/,
                        const TIntrusivePtr<TBlobStorageGroupInfo>& /*groupInfo*/)
        {
            return std::make_unique<TEvGetLogoBlobIndexStatResponse>(status,vdiskID, now, nullptr, nullptr);
        }
    } // NErrBuilder

} // NKikimr
