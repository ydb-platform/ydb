#pragma once

#include "defs.h"
#include "dsproxy_vdisk_mock_ut.h"

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr {

struct TTestState {
    TTestActorRuntime &Runtime;
    TActorId EdgeActor;
    TBlobStorageGroupType Type;
    TGroupMock GroupMock;
    TIntrusivePtr<TBlobStorageGroupInfo> Info;


    TTestState(TTestActorRuntime &runtime, const TBlobStorageGroupType &type,
            TIntrusivePtr<TBlobStorageGroupInfo> &info, ui64 nodeIndex = 0)
        : Runtime(runtime)
        , EdgeActor(runtime.AllocateEdgeActor(nodeIndex))
        , Type(type)
        , GroupMock(0, Type.GetErasure(), Type.BlobSubgroupSize(), 1, info)
        , Info(info)
    {
    }

    TTestState(TTestActorRuntime &runtime, const TBlobStorageGroupType &type, ui64 nodeIndex = 0)
        : Runtime(runtime)
        , EdgeActor(runtime.AllocateEdgeActor(nodeIndex))
        , Type(type)
        , GroupMock(0, Type.GetErasure(), Type.BlobSubgroupSize(), 1)
        , Info(GroupMock.GetInfo())
    {
    }

    TGroupMock& GetGroupMock() {
        return GroupMock;
    }

    ////////////////////////////////////////////////////////////////////////////
    // HELPER FUNCTIONS
    ////////////////////////////////////////////////////////////////////////////
    TPartLocation PrimaryVDiskForBlobPart(TLogoBlobID blobId) {
        Y_ABORT_UNLESS(blobId.PartId());
        TLogoBlobID origBlobId(blobId, 0);
        TVDiskID vDiskId = Info->GetVDiskInSubgroup(blobId.PartId() - 1, origBlobId.Hash());
        return {blobId, vDiskId};
    }

    TPartLocation HandoffVDiskForBlobPart(TLogoBlobID blobId, ui64 handoffIdx) {
        Y_ABORT_UNLESS(blobId.PartId());
        Y_ABORT_UNLESS(handoffIdx < Type.Handoff());
        TLogoBlobID origBlobId(blobId, 0);
        TVDiskID vDiskId = Info->GetVDiskInSubgroup(Type.TotalPartCount() + handoffIdx, origBlobId.Hash());
        return {blobId, vDiskId};
    }

    THashMap<TVDiskID, ui32> MakePredictedDelaysForVDisks(TLogoBlobID blobId) {
        TBlobStorageGroupInfo::TVDiskIds vDiskIds;
        TBlobStorageGroupInfo::TServiceIds serviceIds;
        THashMap<TVDiskID, ui32> result;
        Info->PickSubgroup(blobId.Hash(), &vDiskIds, &serviceIds);
        for (ui32 idx = 0; idx < vDiskIds.size(); ++idx) {
            result.emplace(vDiskIds[idx], 0);
        }
        return result;
    }

    ////////////////////////////////////////////////////////////////////////////
    // PUT BLOBS TO GROUP MOCK
    ////////////////////////////////////////////////////////////////////////////
    TTestState& PutBlobsToGroupMock(const TBlobTestSet &blobSet) {
        GroupMock.PutBlobSet(blobSet);
        return *this;
    }

    template <typename TIter>
    TTestState& PutBlobsToGroupMock(TIter begin, TIter end) {
        TBlobTestSet blobSet;
        blobSet.AddBlobs(begin, end);
        return PutBlobsToGroupMock(blobSet);
    }

    TTestState& PutBlobsToGroupMock(const TVector<TBlobTestSet::TBlob> &blobs) {
        return PutBlobsToGroupMock(blobs.begin(), blobs.end());
    }

    ////////////////////////////////////////////////////////////////////////////
    // CREATE REQUESTS
    ////////////////////////////////////////////////////////////////////////////
    TEvBlobStorage::TEvPut::TPtr CreatePutRequest(const TBlobTestSet::TBlob &blob,
            TEvBlobStorage::TEvPut::ETactic tactic, NKikimrBlobStorage::EPutHandleClass handleClass)
    {
        std::unique_ptr<TEvBlobStorage::TEvPut> put = std::make_unique<TEvBlobStorage::TEvPut>(blob.Id, blob.Data, TInstant::Max(),
                handleClass, tactic);
        return static_cast<TEventHandle<TEvBlobStorage::TEvPut>*>(
                new IEventHandle(EdgeActor, EdgeActor, put.release()));
    }

    template <typename TIter, typename TPutIter>
    void CreatePutRequests(TIter begin, TIter end, TPutIter out,
            TEvBlobStorage::TEvPut::ETactic tactic, NKikimrBlobStorage::EPutHandleClass handleClass)
    {
        for (auto it = begin; it != end; ++it) {
            *out++ = CreatePutRequest(*it, tactic, handleClass);
        }
    }

    template <typename TPutIter>
    void CreatePutRequests(const TVector<TBlobTestSet::TBlob> &blobs, TPutIter out, TEvBlobStorage::TEvPut::ETactic tactic,
            NKikimrBlobStorage::EPutHandleClass handleClass)
    {
        CreatePutRequests(blobs.begin(), blobs.end(), out, tactic, handleClass);
    }

    TEvBlobStorage::TEvGet::TPtr CreateGetRequest(const TVector<TLogoBlobID> &blobs, bool mustRestore)
    {
        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queries(new TEvBlobStorage::TEvGet::TQuery[blobs.size()]);
        for (ui64 queryIdx = 0; queryIdx < blobs.size(); ++queryIdx) {
            TEvBlobStorage::TEvGet::TQuery &q = queries[queryIdx];
            q.Id = blobs[queryIdx];
            q.Shift = 0;
            q.Size = q.Id.BlobSize();
        }
        std::unique_ptr<TEvBlobStorage::TEvGet> get = std::make_unique<TEvBlobStorage::TEvGet>(queries, blobs.size(), TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead, mustRestore, false);
        return static_cast<TEventHandle<TEvBlobStorage::TEvGet>*>(
                new IEventHandle(EdgeActor, EdgeActor, get.release()));
    }

    ////////////////////////////////////////////////////////////////////////////
    // CREATE EVENTS
    ////////////////////////////////////////////////////////////////////////////
    template <typename TEvent>
    typename TEvent::TPtr CreateEventPtr(TActorId recipient, TActorId sender, TEvent *ev, ui64 cookie) {
        TAutoPtr<IEventHandle> handle = new IEventHandle(recipient, sender, ev, 0, cookie);
        return static_cast<TEventHandle<TEvent>*>(handle.Release());
    }

    template <typename TEvent>
    typename TEvent::TPtr CreateEventPtr(TActorId recipient, TActorId sender, std::unique_ptr<TEvent> &ev, ui64 cookie) {
        return CreateEventPtr(recipient, sender, ev.release(), cookie);
    }

    template <typename TEvent>
    typename TEvent::TPtr GrabEventPtr() {
        TAutoPtr<IEventHandle> handle;
        auto ev = Runtime.GrabEdgeEventRethrow<TEvent>(handle);
        UNIT_ASSERT(ev);
        return static_cast<TEventHandle<TEvent>*>(handle.Release());
    }

    template <typename TEventPtr, typename ...TArgs>
    auto CreateEventResultPtr(TEventPtr &ev, TArgs ...args) {
        auto result = CreateEventResult(ev->Get(), args...);
        auto handle = CreateEventPtr(ev->Sender, ev->Recipient, result, ev->Cookie);
        ui64 msgId = ev->Get()->Record.GetMsgQoS().GetMsgId().GetMsgId();
        ui64 sequenceId = ev->Get()->Record.GetMsgQoS().GetMsgId().GetSequenceId();
        handle->Get()->Record.MutableMsgQoS()->MutableMsgId()->SetMsgId(msgId);
        handle->Get()->Record.MutableMsgQoS()->MutableMsgId()->SetSequenceId(sequenceId);
        return handle;
    }

    std::unique_ptr<TEvBlobStorage::TEvVPutResult> CreateEventResult(TEvBlobStorage::TEvVPut *ev,
            NKikimrProto::EReplyStatus status, TVDiskID vDiskId)
    {
        NKikimrBlobStorage::TEvVPut &record = ev->Record;
        TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(record.GetBlobID());
        ui64 cookieValue = record.GetCookie();
        ui64 *cookie = record.HasCookie() ? &cookieValue : nullptr;
        std::unique_ptr<TEvBlobStorage::TEvVPutResult> result(new TEvBlobStorage::TEvVPutResult(status, blobId, vDiskId,
                cookie, TOutOfSpaceStatus(0u, 0.0), TAppData::TimeProvider->Now(),
                0, nullptr, nullptr, nullptr, nullptr, 0, 0, TString()));
        return result;
    }

    std::unique_ptr<TEvBlobStorage::TEvVMultiPutResult> CreateEventResult(TEvBlobStorage::TEvVMultiPut *ev,
            NKikimrProto::EReplyStatus status, TVDiskID vDiskId)
    {
        NKikimrBlobStorage::TEvVMultiPut &record = ev->Record;
        ui64 cookieValue = record.GetCookie();
        ui64 *cookie = record.HasCookie() ? &cookieValue : nullptr;
        std::unique_ptr<TEvBlobStorage::TEvVMultiPutResult> result(
                new TEvBlobStorage::TEvVMultiPutResult(status, vDiskId, cookie, TAppData::TimeProvider->Now(), 0,
                        nullptr, nullptr, nullptr, nullptr, 0, 0, TString()));
        result->Record.SetStatusFlags(TOutOfSpaceStatus(0u, 0.0).Flags);
        return result;
    }

    template <typename TIter>
    std::unique_ptr<TEvBlobStorage::TEvVMultiPutResult> CreateEventResult(TEvBlobStorage::TEvVMultiPut *ev,
            NKikimrProto::EReplyStatus status, TIter begin, TIter end, TVDiskID vDiskId)
    {
        NKikimrBlobStorage::TEvVMultiPut &record = ev->Record;
        std::unique_ptr<TEvBlobStorage::TEvVMultiPutResult> result = CreateEventResult(ev, status, vDiskId);

        ui64 itemCount = record.ItemsSize();
        for (ui64 itemIdx = 0; itemIdx < itemCount; ++itemIdx) {
            UNIT_ASSERT(begin != end);
            auto &item = record.GetItems(itemIdx);
            UNIT_ASSERT(item.HasBlobID());
            TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(item.GetBlobID());
            ui64 cookieValue = item.GetCookie();
            ui64 *cookie = item.HasCookie() ? &cookieValue : nullptr;
            result->AddVPutResult(*begin, TString(), blobId, cookie, TOutOfSpaceStatus(0u, 0.0).Flags);
            begin++;
        }
        UNIT_ASSERT(begin == end);
        return result;
    }

    template <typename TCont>
    std::unique_ptr<TEvBlobStorage::TEvVMultiPutResult> CreateEventResult(TEvBlobStorage::TEvVMultiPut *ev,
            NKikimrProto::EReplyStatus status, const TCont &cont, TVDiskID vDiskId)
    {
        return CreateEventResult(ev, status, cont.cbegin(), cont.cend(), vDiskId);
    }

    std::unique_ptr<TEvBlobStorage::TEvVGetResult> CreateEventResult(TEvBlobStorage::TEvVGet *ev,
            NKikimrProto::EReplyStatus status = NKikimrProto::OK)
    {
        TVDiskID vDiskId = VDiskIDFromVDiskID(ev->Record.GetVDiskID());
        std::unique_ptr<TEvBlobStorage::TEvVGetResult> result(new TEvBlobStorage::TEvVGetResult(
                status, vDiskId, TAppData::TimeProvider->Now(), 0, nullptr,
                nullptr, nullptr, nullptr, {}, 0U, 0U));
        return result;
    }

    ////////////////////////////////////////////////////////////////////////////
    // HANDLE WITH MOCK
    ////////////////////////////////////////////////////////////////////////////
    void HandleVGetsWithMock(ui64 vGetCount) {
        for (ui64 idx = 0; idx < vGetCount; ++idx) {
            TEvBlobStorage::TEvVGet::TPtr ev = GrabEventPtr<TEvBlobStorage::TEvVGet>();
            TEvBlobStorage::TEvVGetResult::TPtr result = CreateEventResultPtr(ev, NKikimrProto::UNKNOWN);
            GroupMock.OnVGet(*ev->Get(), *result->Get());
            Runtime.Send(result.Release());
        }
    }

    void HandleVPutsWithMock(ui64 vPutCount) {
        for (ui64 idx = 0; idx < vPutCount; ++idx) {
            TEvBlobStorage::TEvVPut::TPtr ev = GrabEventPtr<TEvBlobStorage::TEvVPut>();
            TVDiskID vDiskId = GroupMock.GetVDiskID(*ev->Get());
            NKikimrProto::EReplyStatus status = GroupMock.OnVPut(*ev->Get());
            TEvBlobStorage::TEvVPutResult::TPtr result = CreateEventResultPtr(ev, status, vDiskId);
            Runtime.Send(result.Release());
        }
    }

    void HandleVMultiPutsWithMock(ui64 vMultiPutCount) {
        for (ui64 idx = 0; idx < vMultiPutCount; ++idx) {
            TEvBlobStorage::TEvVMultiPut::TPtr ev = GrabEventPtr<TEvBlobStorage::TEvVMultiPut>();
            TVDiskID vDiskId = GroupMock.GetVDiskID(*ev->Get());
            TVector<NKikimrProto::EReplyStatus> statuses = GroupMock.OnVMultiPut(*ev->Get());
            NKikimrProto::EReplyStatus status = NKikimrProto::OK;
            if (std::all_of(statuses.begin(), statuses.end(), [](auto st) { return st == NKikimrProto::RACE; })) {
                status = NKikimrProto::RACE;
            }
            TEvBlobStorage::TEvVMultiPutResult::TPtr result = CreateEventResultPtr(ev, status, statuses, vDiskId);
            Runtime.Send(result.Release());
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // RECEIVE RESPONSES
    ////////////////////////////////////////////////////////////////////////////
    void ReceivePutResults(ui64 count, const TMap<TLogoBlobID, NKikimrProto::EReplyStatus> &specialStatus)
    {
        TSet<TLogoBlobID> seenBlobs;
        for (ui64 idx = 0; idx < count; ++idx) {
            TAutoPtr<IEventHandle> handle;
            TEvBlobStorage::TEvPutResult *putResult = Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvPutResult>(handle);
            Y_ABORT_UNLESS(!seenBlobs.count(putResult->Id));
            seenBlobs.insert(putResult->Id);
            auto it = specialStatus.find(putResult->Id);
            Y_ABORT_UNLESS(it != specialStatus.end());
            NKikimrProto::EReplyStatus expectedStatus = it->second;
            Y_VERIFY_S(putResult->Status == expectedStatus, "expected status "
                    << NKikimrProto::EReplyStatus_Name(expectedStatus)
                    << ", but given "
                    << NKikimrProto::EReplyStatus_Name(putResult->Status));
        }
    }
};

} // namespace NKikimr
