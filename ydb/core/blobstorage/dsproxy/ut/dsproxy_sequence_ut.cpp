#include "defs.h"
#include "dsproxy_env_mock_ut.h"
#include "dsproxy_vdisk_mock_ut.h"
#include "dsproxy_test_state_ut.h"

#include <ydb/core/blobstorage/dsproxy/dsproxy.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_nodemon.h>

#include <ydb/core/blobstorage/base/utility.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/query/query_spacetracker.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NBlobStorageProxySequenceTest {

constexpr ui32 GROUP_ID = 0;
TDSProxyEnv DSProxyEnv;

void Setup(TTestActorRuntime& runtime, TBlobStorageGroupType type) {
    if (IsVerbose) {
        runtime.SetLogPriority(NKikimrServices::BS_PROXY_PUT, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::BS_PROXY_GET, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::BS_PROXY_PATCH, NLog::PRI_DEBUG);
    } else {
        runtime.SetLogPriority(NKikimrServices::BS_PROXY, NLog::PRI_CRIT);
        runtime.SetLogPriority(NKikimrServices::BS_PROXY_PUT, NLog::PRI_CRIT);
        runtime.SetLogPriority(NKikimrServices::BS_PROXY_GET, NLog::PRI_CRIT);
        runtime.SetLogPriority(NKikimrServices::BS_QUEUE, NLog::PRI_CRIT);
    }
    SetupRuntime(runtime);
    DSProxyEnv.Configure(runtime, type, GROUP_ID, 0);
}

struct TGetQuery {
    TLogoBlobID LogoBlobId;
    ui32 Shift;
    ui32 Size;
    ui64 QueryCookie;

    TGetQuery()
        : Shift(0)
        , Size(0)
        , QueryCookie(0)
    {}

    void SetFrom(const TEvBlobStorage::TEvVGet &vget, const ::NKikimrBlobStorage::TExtremeQuery &query) {
        Y_UNUSED(vget);
        LogoBlobId = LogoBlobIDFromLogoBlobID(query.GetId());
        if (query.HasShift()) {
            Shift = (ui32)query.GetShift();
        }
        if (query.HasSize()) {
            Size = (ui32)query.GetSize();
        }
        if (query.HasCookie()) {
            QueryCookie = query.GetCookie();
        }
    }
};

struct TGetRangeQuery {
    TLogoBlobID FromId;
    TLogoBlobID ToId;
    bool IndexOnly;
    ui64 MaxResults;
    ui64 QueryCookie;

    TGetRangeQuery()
        : IndexOnly(false)
        , MaxResults(0)
        , QueryCookie(0)
    {}

    void SetFrom(const TEvBlobStorage::TEvVGet &vget, const ::NKikimrBlobStorage::TRangeQuery &query) {
        FromId = LogoBlobIDFromLogoBlobID(query.GetFrom());
        ToId = LogoBlobIDFromLogoBlobID(query.GetTo());
        IndexOnly = vget.Record.HasIndexOnly() ? vget.Record.GetIndexOnly() : false;
        MaxResults = query.HasMaxResults() ? query.GetMaxResults() : 0;
        QueryCookie = query.HasCookie() ? query.GetCookie() : 0;
    }
};

struct TGetRequest {
    TVector<TGetQuery> Queries;
    TVector<TGetRangeQuery> RangeQueries;
    TActorId Sender;
    TActorId ActorId;
    TVDiskID VDiskId;
    ui64 Cookie;
    ui64 RecordCookie;
    ui64 MsgId;
    ui64 SequenceId;
    bool IsValid;

    TGetRequest()
        : Cookie(0)
        , RecordCookie(0)
        , MsgId(0)
        , SequenceId(0)
        , IsValid(false)
    {}

    void SetFrom(const IEventHandle *handle, const TEvBlobStorage::TEvVGet *vget) {
        Queries.clear();
        Queries.resize(vget->Record.ExtremeQueriesSize());
        TStringStream str;
        for (ui32 queryIdx = 0; queryIdx < vget->Record.ExtremeQueriesSize(); ++queryIdx) {
            Queries[queryIdx].SetFrom(*vget, vget->Record.GetExtremeQueries(queryIdx));
            str << " id# " << LogoBlobIDFromLogoBlobID(vget->Record.GetExtremeQueries(queryIdx).GetId()).ToString();
        }
        RangeQueries.clear();
        RangeQueries.resize(vget->Record.HasRangeQuery() ? 1 : 0);
        if (RangeQueries.size()) {
            str << " rangeQuery";
            RangeQueries[0].SetFrom(*vget, vget->Record.GetRangeQuery());
        }

        Sender = handle->Sender;
        ActorId = handle->Recipient;
        str << " actor# " << ActorId << Endl;
        VDiskId = VDiskIDFromVDiskID(vget->Record.GetVDiskID());
        str << " disk# " << VDiskId.ToString() << Endl;
        Cookie = handle->Cookie;
        RecordCookie = vget->Record.GetCookie();
        MsgId = vget->Record.GetMsgQoS().GetMsgId().GetMsgId();
        SequenceId = vget->Record.GetMsgQoS().GetMsgId().GetSequenceId();

        IsValid = true;
    }
};

struct TVDiskState {
    TActorId Sender;
    TActorId ActorId;
    TLogoBlobID LogoBlobId;
    TVDiskID VDiskId;
    ui64 LastCookie;
    ui64 InnerCookie;
    TString Data;
    TVector<ui64> QueryCookies;
    ui64 MsgId;
    ui64 SequenceId;
    bool IsValid;

    TVDiskState()
        : LastCookie(0)
        , InnerCookie(0)
        , MsgId(0)
        , SequenceId(0)
        , IsValid(false)
    {}

    void SetFrom(const IEventHandle *handle, const TEvBlobStorage::TEvVPut *vput) {
        Sender = handle->Sender;
        ActorId = handle->Recipient;
        LogoBlobId = LogoBlobIDFromLogoBlobID(vput->Record.GetBlobID());
        VDiskId = VDiskIDFromVDiskID(vput->Record.GetVDiskID());
        LastCookie = handle->Cookie;
        InnerCookie = vput->Record.GetCookie();
        Data = vput->Record.GetBuffer();
        if (!vput->Record.HasBuffer()) {
            const TRope& rope = vput->GetPayload(0);
            Data = TString::Uninitialized(rope.GetSize());
            rope.Begin().ExtractPlainDataAndAdvance(Data.Detach(), Data.size());
        }
        MsgId = vput->Record.GetMsgQoS().GetMsgId().GetMsgId();
        SequenceId = vput->Record.GetMsgQoS().GetMsgId().GetSequenceId();
        IsValid = true;
    }

    void SetFrom(const IEventHandle *handle, const TEvBlobStorage::TEvVMultiPut *vput, ui64 itemIdx) {
        Sender = handle->Sender;
        ActorId = handle->Recipient;
        auto &item = vput->Record.GetItems(itemIdx);
        LogoBlobId = LogoBlobIDFromLogoBlobID(item.GetBlobID());
        VDiskId = VDiskIDFromVDiskID(vput->Record.GetVDiskID());
        LastCookie = handle->Cookie;
        InnerCookie = item.GetCookie();
        Data = item.GetBuffer();
        if (!item.HasBuffer()) {
            const TRope& rope = vput->GetPayload(itemIdx);
            Data = TString::Uninitialized(rope.GetSize());
            rope.Begin().ExtractPlainDataAndAdvance(Data.Detach(), Data.size());
        }
        MsgId = vput->Record.GetMsgQoS().GetMsgId().GetMsgId();
        SequenceId = vput->Record.GetMsgQoS().GetMsgId().GetSequenceId();
        IsValid = true;
    }

    void SetCookiesAndSenderFrom(const IEventHandle *handle, const TEvBlobStorage::TEvVGet *vget) {
        Sender = handle->Sender;
        LastCookie = handle->Cookie;
        InnerCookie = vget->Record.GetCookie();
        QueryCookies.clear();
        for (ui32 queryIdx = 0; queryIdx < vget->Record.ExtremeQueriesSize(); ++queryIdx) {
            if (vget->Record.GetExtremeQueries(queryIdx).HasCookie()) {
                QueryCookies.push_back(vget->Record.GetExtremeQueries(queryIdx).GetCookie());
            }
        }
        MsgId = vget->Record.GetMsgQoS().GetMsgId().GetMsgId();
        SequenceId = vget->Record.GetMsgQoS().GetMsgId().GetSequenceId();
        IsValid = true;
    }
};


void SetPredictedDelaysForAllQueues(const THashMap<TVDiskID, ui32> &latencies) {
    UNIT_ASSERT(DSProxyEnv.GroupQueues);
    for (TGroupQueues::TFailDomain &domain : DSProxyEnv.GroupQueues->FailDomains) {
        for (TGroupQueues::TVDisk &vDisk : domain.VDisks) {
            ui32 begin = NKikimrBlobStorage::EVDiskQueueId::Begin;
            ui32 end = NKikimrBlobStorage::EVDiskQueueId::End;
            for (ui32 id = begin; id < end; ++id) {
                NKikimrBlobStorage::EVDiskQueueId vDiskQueueId = static_cast<NKikimrBlobStorage::EVDiskQueueId>(id);
                auto flowRecord = vDisk.Queues.FlowRecordForQueueId(vDiskQueueId);
                if (flowRecord) {
                    flowRecord->SetPredictedDelayNs(0);
                }
            }
        }
    }
    for (auto &[vDiskId, latency] : latencies) {
        TGroupQueues::TVDisk &vDisk = DSProxyEnv.GroupQueues->FailDomains[vDiskId.FailDomain].VDisks[vDiskId.VDisk];
        ui32 begin = NKikimrBlobStorage::EVDiskQueueId::Begin;
        ui32 end = NKikimrBlobStorage::EVDiskQueueId::End;
        for (ui32 id = begin; id < end; ++id) {
            NKikimrBlobStorage::EVDiskQueueId vDiskQueueId = static_cast<NKikimrBlobStorage::EVDiskQueueId>(id);;
            auto flowRecord = vDisk.Queues.FlowRecordForQueueId(vDiskQueueId);
            if (flowRecord) {
                flowRecord->SetPredictedDelayNs(latency);
            }
        }
    }
}

void SendVGetResult(ui32 vDiskIdx, NKikimrProto::EReplyStatus status, ui32 partId,
        TVector<TVDiskState> &subgroup, TTestActorRuntime &runtime) {

    TVDiskState *from = &subgroup[vDiskIdx];

    std::unique_ptr<TEvBlobStorage::TEvVGetResult> result(new TEvBlobStorage::TEvVGetResult(NKikimrProto::OK, from->VDiskId,
        TAppData::TimeProvider->Now(), 0, nullptr, nullptr, nullptr, nullptr, {}, 0U, 0U));

    SetPredictedDelaysForAllQueues({});
    ui64 queryCookie = from->QueryCookies.size() ? from->QueryCookies[0] : 0;
    if (status == NKikimrProto::OK) {
        result->Record.SetCookie(from->InnerCookie);
        TVDiskState *part = nullptr;
        Y_ABORT_UNLESS(subgroup.size() > partId - 1);
        part = &subgroup[partId - 1];

        result->AddResult(status, part->LogoBlobId, 0, TRope(part->Data), &queryCookie);
    } else if (status == NKikimrProto::NODATA) {
        result->Record.SetCookie(from->InnerCookie);
        TLogoBlobID id(from->LogoBlobId, partId);
        result->AddResult(status, id, 0, 0u, &queryCookie);
    } else {
        result->Record.SetCookie(from->InnerCookie);
        TLogoBlobID id(from->LogoBlobId, 0);
        result->AddResult(status, id, &queryCookie);
    }
    result->Record.MutableMsgQoS()->MutableMsgId()->SetMsgId(subgroup[vDiskIdx].MsgId);
    result->Record.MutableMsgQoS()->MutableMsgId()->SetSequenceId(subgroup[vDiskIdx].SequenceId);
    runtime.Send(new IEventHandle(from->Sender, from->ActorId, result.release(), 0, from->LastCookie));
}

void SendVGetResult(ui32 blobIdx, ui32 vDiskIdx, NKikimrProto::EReplyStatus status,
        TVector<TVector<TVDiskState>> &blobSubgroups,
        TMap<TActorId, TGetRequest> &lastRequest, TTestActorRuntime &runtime) {
    TGetRequest &request = lastRequest[blobSubgroups[blobIdx][vDiskIdx].ActorId];
    if (!request.IsValid) {
        return;
    }

    SetPredictedDelaysForAllQueues({});
    if (status == NKikimrProto::ERROR || status == NKikimrProto::TRYLATER
            || status == NKikimrProto::TRYLATER_SIZE || status == NKikimrProto::TRYLATER_TIME
            || status == NKikimrProto::VDISK_ERROR_STATE) {
        std::unique_ptr<TEvBlobStorage::TEvVGetResult> result(new TEvBlobStorage::TEvVGetResult(
            status, request.VDiskId, TAppData::TimeProvider->Now(), 0, nullptr, nullptr, nullptr, nullptr,
            {}, 0U, 0U));
        for (auto it = request.Queries.begin(); it != request.Queries.end(); ++it) {
            result->AddResult(status, it->LogoBlobId, &it->QueryCookie);
        }
        result->Record.MutableMsgQoS()->MutableMsgId()->SetMsgId(request.MsgId);
        result->Record.MutableMsgQoS()->MutableMsgId()->SetSequenceId(request.SequenceId);
        result->Record.SetCookie(request.RecordCookie);
        runtime.Send(new IEventHandle(request.Sender, request.ActorId, result.release(), 0, request.Cookie));
        return;
    } else if (status == NKikimrProto::NODATA) {
        std::unique_ptr<TEvBlobStorage::TEvVGetResult> result(new TEvBlobStorage::TEvVGetResult(
            NKikimrProto::OK, request.VDiskId, TAppData::TimeProvider->Now(), 0, nullptr,
            nullptr, nullptr, nullptr, {}, 0U, 0U));
        for (auto it = request.Queries.begin(); it != request.Queries.end(); ++it) {
            result->AddResult(status, it->LogoBlobId, &it->QueryCookie);
            TLogoBlobID id(it->LogoBlobId);
            result->AddResult(NKikimrProto::NODATA, id, &it->QueryCookie);
        }
        result->Record.MutableMsgQoS()->MutableMsgId()->SetMsgId(request.MsgId);
        result->Record.MutableMsgQoS()->MutableMsgId()->SetSequenceId(request.SequenceId);
        result->Record.SetCookie(request.RecordCookie);
        runtime.Send(new IEventHandle(request.Sender, request.ActorId, result.release(), 0, request.Cookie));
        return;
    } else if (status == NKikimrProto::OK) {
        std::unique_ptr<TEvBlobStorage::TEvVGetResult> result(new TEvBlobStorage::TEvVGetResult(
            NKikimrProto::OK, request.VDiskId, TAppData::TimeProvider->Now(), 0, nullptr,
            nullptr, nullptr, nullptr, {}, 0U, 0U));
        for (auto it = request.Queries.begin(); it != request.Queries.end(); ++it) {
            TString data;
            ui32 partIdx = 0;
            for (ui32 bIdx = 0; bIdx < blobSubgroups.size(); ++bIdx) {
                if (blobSubgroups[bIdx][0].LogoBlobId.IsSameBlob(it->LogoBlobId)) {
                    for (ui32 vIdx = 0; vIdx < blobSubgroups[bIdx].size(); ++vIdx) {
                        TVDiskState &state = blobSubgroups[bIdx][vIdx];
                        if (state.ActorId == request.ActorId) {
                            partIdx = vIdx % 6;
                            ui32 size = state.Data.size() - it->Shift;
                            if (it->Size && it->Size < size) {
                                size = it->Size;
                            }
                            data = state.Data.substr(it->Shift, size);
                        }
                    }
                }
            }
            TLogoBlobID id(it->LogoBlobId, partIdx + 1);
            result->AddResult(status, id, it->Shift, TRope(data), &it->QueryCookie);
        }
        result->Record.MutableMsgQoS()->MutableMsgId()->SetMsgId(request.MsgId);
        result->Record.MutableMsgQoS()->MutableMsgId()->SetSequenceId(request.SequenceId);
        result->Record.SetCookie(request.RecordCookie);
        runtime.Send(new IEventHandle(request.Sender, request.ActorId, result.release(), 0, request.Cookie));
        return;
    } else {
        Y_ABORT();
    }
}


void GrabVPutEvent(TTestActorRuntime &runtime, TVector<TVDiskState> &subgroup, ui32 vDiskIdxShift = 0) {
    TAutoPtr <IEventHandle> handle;
    auto vput = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVPut>(handle);
    UNIT_ASSERT(vput);
    TLogoBlobID id = LogoBlobIDFromLogoBlobID(vput->Record.GetBlobID());
    ui32 idx = id.PartId();
    Y_ABORT_UNLESS(idx);
    idx = idx - 1 + vDiskIdxShift;
    Y_VERIFY_S(idx < subgroup.size(), idx << ' ' << subgroup.size() << ' ' << vDiskIdxShift << ' ' << vput->ToString());
    Y_ABORT_UNLESS(!subgroup[idx].IsValid);
    subgroup[idx].SetFrom(handle.Get(), vput);
}

void SendVPutResultEvent(TTestActorRuntime &runtime, TVDiskState &vdisk, NKikimrProto::EReplyStatus status) {
    Y_ABORT_UNLESS(vdisk.IsValid);
    std::unique_ptr<TEvBlobStorage::TEvVPutResult> vPutResult(new TEvBlobStorage::TEvVPutResult(
        status, vdisk.LogoBlobId, vdisk.VDiskId,
        &vdisk.InnerCookie, TOutOfSpaceStatus(0u, 0.0), TAppData::TimeProvider->Now(),
        0, nullptr, nullptr, nullptr, nullptr, 0, 0, TString()));
    vPutResult->Record.MutableMsgQoS()->MutableMsgId()->SetMsgId(vdisk.MsgId);
    vPutResult->Record.MutableMsgQoS()->MutableMsgId()->SetSequenceId(vdisk.SequenceId);
    SetPredictedDelaysForAllQueues({});
    runtime.Send(new IEventHandle(vdisk.Sender, vdisk.ActorId, vPutResult.release(), 0, vdisk.LastCookie));
}

void PrepareBlobSubgroup(TLogoBlobID logoblobid, TString data, TVector<TVDiskState> &subgroup,
        TTestActorRuntime &runtime, TBlobStorageGroupType type) {
    TActorId proxy = MakeBlobStorageProxyID(GROUP_ID);
    TActorId sender = runtime.AllocateEdgeActor(0);

    runtime.Send(new IEventHandle(proxy, sender, new TEvBlobStorage::TEvPut(logoblobid, data, TInstant::Max())));
    subgroup.resize(type.BlobSubgroupSize());

    ui32 partCount = type.TotalPartCount();
    for (ui32 i = 0; i < partCount; ++i) {
        GrabVPutEvent(runtime, subgroup);
    }
    for (ui32 i = type.Handoff(); i < partCount; ++i) {
        SendVPutResultEvent(runtime, subgroup[i], NKikimrProto::OK);
    }
    for (ui32 i = 0; i < type.Handoff(); ++i) {
        SendVPutResultEvent(runtime, subgroup[i], NKikimrProto::ERROR);
    }
    for (ui32 i = 0; i < type.Handoff(); ++i) {
        GrabVPutEvent(runtime, subgroup, type.TotalPartCount());
    }
    for (ui32 i = 0; i < type.Handoff(); ++i) {
        SendVPutResultEvent(runtime, subgroup[i + type.TotalPartCount()], NKikimrProto::OK);
    }

    TAutoPtr<IEventHandle> handle;
    auto putResult = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvPutResult>(handle);
    UNIT_ASSERT(putResult);
    UNIT_ASSERT(putResult->Status == NKikimrProto::OK);
}



Y_UNIT_TEST_SUITE(TBlobStorageProxySequenceTest) {


struct TGeneralDecorator : public TDecorator {
    using TAction = std::function<bool(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx)>;

    TAction Action;

    TGeneralDecorator(THolder<IActor> &&actor, TAction action)
        : TDecorator(std::move(actor))
        , Action(action)
    {
    }

    bool DoBeforeReceiving(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) override {
        return Action(ev, ctx);
    }
};


Y_UNIT_TEST(TestBlock42PutWithChangingSlowDisk) {
    TBlobStorageGroupType type = {TErasureType::Erasure4Plus2Block};
    TTestBasicRuntime runtime(1, false);
    Setup(runtime, type);
    TTestState testState(runtime, type, DSProxyEnv.Info);

    TLogoBlobID blobId(72075186224047637, 1, 863, 1, 786, 24576);

    TStringBuilder dataBuilder;
    for (size_t i = 0; i < blobId.BlobSize(); ++i) {
        dataBuilder << 'a';
    }
    TBlobTestSet::TBlob blob(blobId, dataBuilder);


    TEvBlobStorage::TEvPut::ETactic tactic = TEvBlobStorage::TEvPut::TacticDefault;
    NKikimrBlobStorage::EPutHandleClass handleClass = NKikimrBlobStorage::TabletLog;

    TBatchedVec<TEvBlobStorage::TEvPut::TPtr> batched;

    TEvBlobStorage::TEvPut::TPtr ev = testState.CreatePutRequest(blob, tactic, handleClass);
    std::unique_ptr<IActor> putActor = DSProxyEnv.CreatePutRequestActor(ev);

    TGroupMock &groupMock = testState.GetGroupMock();
    groupMock.SetError(0, NKikimrProto::ERROR);
    groupMock.SetError(5, NKikimrProto::ERROR);

    THashMap<TVDiskID, ui32> latencies = testState.MakePredictedDelaysForVDisks(blobId);
    for (auto &[vDiskId, latency] : latencies) {
        latency = 1;
    }
    std::optional<TVDiskID> prevVDiskId;
    TSet<TLogoBlobID> okParts;

    auto action = [&](TAutoPtr<IEventHandle>& ev, const TActorContext&) {
        if (ev->Type == TEvents::TSystem::Bootstrap) {
            return true;
        }
        UNIT_ASSERT(ev->Type == TEvBlobStorage::EvVPutResult);
        TEvBlobStorage::TEvVPutResult *putResult = ev->Get<TEvBlobStorage::TEvVPutResult>();
        UNIT_ASSERT(putResult);
        TVDiskID vDiskId = VDiskIDFromVDiskID(putResult->Record.GetVDiskID());
        TLogoBlobID part = LogoBlobIDFromLogoBlobID(putResult->Record.GetBlobID());
        NKikimrProto::EReplyStatus status = putResult->Record.GetStatus();
        CTEST << "Receive LogoBlobId# " << part << " vdisk " << vDiskId << " " << NKikimrProto::EReplyStatus_Name(status) << Endl;
        if (prevVDiskId) {
            latencies[*prevVDiskId] = 1;
        }
        latencies[vDiskId] = 10;
        prevVDiskId = vDiskId;
        SetPredictedDelaysForAllQueues(latencies);
        return true;
    };

    runtime.Register(new TGeneralDecorator(THolder<IActor>(putActor.release()), action));

    for (ui64 idx = 0; idx < 8; ++idx) {
        TEvBlobStorage::TEvVPut::TPtr ev = testState.GrabEventPtr<TEvBlobStorage::TEvVPut>();
        TLogoBlobID part = LogoBlobIDFromLogoBlobID(ev->Get()->Record.GetBlobID());
        TVDiskID vDiskId = VDiskIDFromVDiskID(ev->Get()->Record.GetVDiskID());
        UNIT_ASSERT(okParts.count(part) == 0);
        NKikimrProto::EReplyStatus status = groupMock.OnVPut(*ev->Get());
        if (status == NKikimrProto::OK) {
            okParts.insert(part);
        }
        TEvBlobStorage::TEvVPutResult::TPtr result = testState.CreateEventResultPtr(ev, status, vDiskId);
        runtime.Send(result.Release());
    }


    TMap<TLogoBlobID, NKikimrProto::EReplyStatus> expectedStatus {
        {blobId, NKikimrProto::OK}
    };
    testState.ReceivePutResults(1, expectedStatus);
}

void MakeTestMultiPutItemStatuses(TTestBasicRuntime &runtime, const TBlobStorageGroupType &type,
                                  const TBatchedVec<NKikimrProto::EReplyStatus> &statuses) {
    TString data("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
    TTestState testState(runtime, type, DSProxyEnv.Info);

    TVector<TLogoBlobID> blobIds = {
        TLogoBlobID(72075186224047637, 1, 863, 1, 786, 24576),
        TLogoBlobID(72075186224047637, 1, 2194, 1, 142, 12288)
    };
    TVector<TBlobTestSet::TBlob> blobs;
    for (const auto& id : blobIds) {
        TStringBuilder builder;
        for (size_t i = 0; i < id.BlobSize(); ++i) {
            builder << 'a';
        }
        blobs.emplace_back(id, builder);
    }

    TEvBlobStorage::TEvPut::ETactic tactic = TEvBlobStorage::TEvPut::TacticDefault;
    NKikimrBlobStorage::EPutHandleClass handleClass = NKikimrBlobStorage::TabletLog;

    TBatchedVec<TEvBlobStorage::TEvPut::TPtr> batched;
    testState.CreatePutRequests(blobs, std::back_inserter(batched), tactic, handleClass);
    SetPredictedDelaysForAllQueues({});
    runtime.Register(DSProxyEnv.CreatePutRequestActor(batched, tactic, handleClass).release());

    TMap<TPartLocation, NKikimrProto::EReplyStatus> specialStatuses;
    for (ui64 idx = 0; idx < blobIds.size(); ++idx) {
        for (ui64 part = 1; part <= type.TotalPartCount(); ++part) {
            TLogoBlobID partBlobId(blobIds[idx], part);
            TPartLocation id = testState.PrimaryVDiskForBlobPart(partBlobId);
            specialStatuses[id] = statuses[idx];
        }
    }

    TGroupMock &groupMock = testState.GetGroupMock();
    groupMock.SetSpecialStatuses(specialStatuses);

    testState.HandleVMultiPutsWithMock(type.BlobSubgroupSize());

    TMap<TLogoBlobID, NKikimrProto::EReplyStatus> expectedStatus;
    for (ui64 idx = 0; idx < blobIds.size(); ++idx) {
        expectedStatus[blobIds[idx]] = statuses[idx];
    }
    testState.ReceivePutResults(expectedStatus.size(), expectedStatus);
}

Y_UNIT_TEST(TestGivenBlock42MultiPut2ItemsStatuses) {
    return; // KIKIMR-9016

    TBlobStorageGroupType type = {TErasureType::Erasure4Plus2Block};
    constexpr ui64 statusCount = 3;
    NKikimrProto::EReplyStatus maybeStatuses[statusCount] = {
        NKikimrProto::OK,
        NKikimrProto::BLOCKED,
        NKikimrProto::DEADLINE
    };
    Y_ABORT_UNLESS(maybeStatuses[statusCount - 1] == NKikimrProto::DEADLINE);
    for (ui64 fstIdx = 0; fstIdx < statusCount; ++fstIdx) {
        for (ui64 sndIdx = 0; sndIdx < statusCount; ++sndIdx) {
            TTestBasicRuntime runtime(1, false);
            Setup(runtime, type);
            MakeTestMultiPutItemStatuses(runtime, type, {maybeStatuses[fstIdx], maybeStatuses[sndIdx]});
        }
    }
}

enum {
    Begin = EventSpaceBegin(TEvents::ES_USERSPACE),
    EvRequestEnd
};

struct TEvRequestEnd : TEventLocal<TEvRequestEnd, EvRequestEnd> {
    TEvRequestEnd() = default;
};

struct TDyingDecorator : public TTestDecorator {
    TActorId ParentId;

    TDyingDecorator(THolder<IActor> &&actor, TActorId parentId)
        : TTestDecorator(std::move(actor))
        , ParentId(parentId)
    {
    }

    virtual ~TDyingDecorator() {
        if (NActors::TlsActivationContext) {
            std::unique_ptr<IEventBase> ev = std::make_unique<TEvRequestEnd>();
            std::unique_ptr<IEventHandle> handle = std::make_unique<IEventHandle>(ParentId, ParentId, ev.release());
            TActivationContext::Send(handle.release());
        }
    }
};

Y_UNIT_TEST(TestGivenBlock42GroupGenerationGreaterThanVDiskGenerations) {
    return; // KIKIMR-9016

    TBlobStorageGroupType type = {TErasureType::Erasure4Plus2Block};
    TTestBasicRuntime runtime(1, false);
    Setup(runtime, type);

    TString data("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
    TTestState testState(runtime, type, DSProxyEnv.Info);

    TVector<TLogoBlobID> blobIds = {
        TLogoBlobID(72075186224047637, 1, 863, 1, 786, 24576),
        TLogoBlobID(72075186224047637, 1, 2194, 1, 142, 12288)
    };
    TVector<TBlobTestSet::TBlob> blobs;
    for (const auto& id : blobIds) {
        TStringBuilder builder;
        for (size_t i = 0; i < id.BlobSize(); ++i) {
            builder << 'a';
        }
        blobs.emplace_back(id, builder);
    }

    TEvBlobStorage::TEvPut::ETactic tactic = TEvBlobStorage::TEvPut::TacticDefault;
    NKikimrBlobStorage::EPutHandleClass handleClass = NKikimrBlobStorage::TabletLog;

    TBatchedVec<TEvBlobStorage::TEvPut::TPtr> batched;
    testState.CreatePutRequests(blobs, std::back_inserter(batched), tactic, handleClass);

    DSProxyEnv.SetGroupGeneration(2);

    THolder<IActor> putActor(DSProxyEnv.CreatePutRequestActor(batched, tactic, handleClass).release());
    runtime.Register(new TDyingDecorator(
            std::move(putActor), testState.EdgeActor));

    TMap<TPartLocation, NKikimrProto::EReplyStatus> specialStatuses;
    for (ui64 idx = 0; idx < blobIds.size(); ++idx) {
        for (ui64 part = 1; part <= type.TotalPartCount(); ++part) {
            TLogoBlobID partBlobId(blobIds[idx], part);
            TPartLocation id = testState.PrimaryVDiskForBlobPart(partBlobId);
            specialStatuses[id] = NKikimrProto::RACE;
        }
    }

    TGroupMock &groupMock = testState.GetGroupMock();
    groupMock.SetSpecialStatuses(specialStatuses);

    testState.HandleVMultiPutsWithMock(8);
    testState.GrabEventPtr<TEvRequestEnd>();
}

Y_UNIT_TEST(TestGivenMirror3DCGetWithFirstSlowDisk) {
    TBlobStorageGroupType type = {TErasureType::ErasureMirror3dc};
    TTestBasicRuntime runtime(1, false);
    Setup(runtime, type);

    TLogoBlobID blobId = TLogoBlobID(72075186224047637, 1, 2194, 1, 142, 12288);

    TTestState testState(runtime, type, DSProxyEnv.Info);

    TEvBlobStorage::TEvGet::TPtr ev = testState.CreateGetRequest({blobId}, false);
    TActorId getActorId = runtime.Register(DSProxyEnv.CreateGetRequestActor(ev, NKikimrBlobStorage::TabletLog).release());
    runtime.EnableScheduleForActor(getActorId);

    testState.GrabEventPtr<TEvBlobStorage::TEvVGet>();
    TEvBlobStorage::TEvVGet::TPtr vget = testState.GrabEventPtr<TEvBlobStorage::TEvVGet>();
}

Y_UNIT_TEST(TestGivenBlock42GetThenVGetResponseParts2523Nodata4ThenGetOk) {
    TTestBasicRuntime runtime(1, false);
    TBlobStorageGroupType type = {TErasureType::Erasure4Plus2Block};
    Setup(runtime, type);

    TActorId proxy = MakeBlobStorageProxyID(GROUP_ID);
    TActorId sender = runtime.AllocateEdgeActor(0);

    TString data("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
    TLogoBlobID logoblobid(1, 0, 0, 0, (ui32)data.size(), 0);

    TVector<TVDiskState> subgroup;
    PrepareBlobSubgroup(logoblobid, data, subgroup, runtime, type);

    runtime.Send(new IEventHandle(proxy, sender, new TEvBlobStorage::TEvGet(logoblobid, 0, 0, TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::FastRead)));
    for (ui32 i = 0; i < 6; ++i) {
        TAutoPtr<IEventHandle> handle;
        auto vget = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVGet>(handle);
        UNIT_ASSERT(vget);
        for (size_t idx = 0; idx < subgroup.size(); ++idx) {
            if (subgroup[idx].ActorId == handle->Recipient) {
                subgroup[idx].SetCookiesAndSenderFrom(handle.Get(), vget);
            }
        }
    }

    SendVGetResult(6, NKikimrProto::OK, 2, subgroup, runtime);
    SendVGetResult(4, NKikimrProto::OK, 5, subgroup, runtime);
    SendVGetResult(1, NKikimrProto::OK, 2, subgroup, runtime);
    SendVGetResult(2, NKikimrProto::OK, 3, subgroup, runtime);
    SendVGetResult(7, NKikimrProto::NODATA, 1, subgroup, runtime);
    SendVGetResult(3, NKikimrProto::OK, 4, subgroup, runtime);
    SendVGetResult(5, NKikimrProto::OK, 6, subgroup, runtime);
    SendVGetResult(0, NKikimrProto::OK, 1, subgroup, runtime);

    TAutoPtr<IEventHandle> handle;
    auto getResult = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvGetResult>(handle);
    UNIT_ASSERT(getResult);
    UNIT_ASSERT(getResult->Status == NKikimrProto::OK);
    UNIT_ASSERT(getResult->ResponseSz == 1);
    UNIT_ASSERT(getResult->Responses[0].Status == NKikimrProto::OK);
}

struct TBlobPack {
    ui32 Count;
    ui32 DataLength;

    static TString ToString(const TBlobPack &pack) {
        return TStringBuilder() << "{"
            << "Count# " << pack.Count
            << " DataLength# " << pack.DataLength
            << '}';
    }

    TString ToString() const {
        return ToString(*this);
    }
};

TString VectorToString(const TVector<ui32> &vec) {
    TStringBuilder str;
    str << "{" << Endl;
    str << "Size# " << vec.size() << Endl;
    for (ui64 idx = 0; idx < vec.size(); ++idx) {
        str << " [" << idx << "]# " << ToString(vec[idx]);
    }
    str << "}";
    return str;
}

template <typename Type>
TString VectorToString(const TVector<Type> &vec) {
    TStringBuilder str;
    str << "{" << Endl;
    str << "Size# " << vec.size() << Endl;
    for (ui64 idx = 0; idx < vec.size(); ++idx) {
        str << " [" << idx << "]# " << Type::ToString(vec[idx]);
    }
    str << "}";
    return str;
}

void MakeTestProtobufSizeWithMultiGet(const TVector<TBlobPack> &packs, TVector<ui32> vGetQueryCounts) {
    TTestBasicRuntime runtime(1, false);
    TBlobStorageGroupType type(TErasureType::ErasureNone);
    Setup(runtime, type);

    TActorId proxy = MakeBlobStorageProxyID(GROUP_ID);
    TActorId sender = runtime.AllocateEdgeActor(0);

    TVector<TString> datas(packs.size());
    for (ui32 idx = 0; idx < packs.size(); ++idx) {
        for (ui32 chIdx = 0; chIdx < packs[idx].DataLength; ++chIdx) {
            datas[idx] += 'a' + chIdx % 26;
        }
    }
    ui32 blobCount = 0;
    for (const TBlobPack &pack : packs) {
        blobCount += pack.Count;
    }

    TVector<TVector<TVDiskState>> blobSubgroups(blobCount);
    TVector<TLogoBlobID> blobIds(blobCount);
    for (ui32 packIdx = 0, blobIdx = 0; packIdx < packs.size(); ++packIdx) {
        const TBlobPack &pack = packs[packIdx];
        for (ui32 interIdx = 0; interIdx < pack.Count; ++interIdx, ++blobIdx) {
            blobIds[blobIdx] = TLogoBlobID(1, 0, blobIdx, 0, datas[packIdx].size(), 0);
            PrepareBlobSubgroup(blobIds[blobIdx], datas[packIdx], blobSubgroups[blobIdx], runtime, type);
        }
    }

    TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queries(new TEvBlobStorage::TEvGet::TQuery[blobCount]);
    for (ui32 i = 0; i < blobCount; ++i) {
        auto &q = queries[i];
        q.Id = blobIds[i];
        q.Shift = 0;
        q.Size = q.Id.BlobSize();
    }
    runtime.Send(new IEventHandle(proxy, sender, new TEvBlobStorage::TEvGet(
        queries, blobCount, TInstant::Max(),
        NKikimrBlobStorage::EGetHandleClass::AsyncRead, false)));

    struct TTestVGetInfo {
        ui32 Count;
        ui32 ProtobufSize;

        static TString ToString(const TTestVGetInfo& self) {
            return TStringBuilder() << "{" << "Count# " << self.Count << " ProtobufSize# " << self.ProtobufSize << "}";
        }

        TString ToString() const {
            return ToString(*this);
        }
    };

    TVector<TTestVGetInfo> vGetInformations;
    vGetInformations.push_back({0, 0});
    TQueryResultSizeTracker resultSize;
    resultSize.Init();
    TMap<ui64, ui64> firstQueryStepToVGetIdx = { {0, 0} };
    TVector<ui64> firstSteps(1, 0);
    for (ui32 i = 0; i < blobCount; ++i) {
        ui32 size = blobIds[i].BlobSize();
        resultSize.AddLogoBlobIndex();
        resultSize.AddLogoBlobData(size, 0, 0);
        if (resultSize.IsOverflow()) {
            firstSteps.push_back(blobIds[i].Step());
            firstQueryStepToVGetIdx[firstSteps.back()] = firstSteps.size() - 1;
            resultSize.Init();
            resultSize.AddLogoBlobIndex();
            resultSize.AddLogoBlobData(size, 0, 0);
            vGetInformations.push_back({0, 0});
        }
        vGetInformations.back().Count++;
        vGetInformations.back().ProtobufSize = resultSize.GetSize();
    }

    UNIT_ASSERT_C(vGetInformations.size() == vGetQueryCounts.size(),
            "not equal vGetCount for multiget"
            << " vGetInformations# " << VectorToString(vGetInformations)
            << " vGetQueryCounts# " << VectorToString(vGetQueryCounts));
    for (ui32 vGetIdx = 0; vGetIdx < vGetInformations.size(); ++vGetIdx) {
        UNIT_ASSERT_C(vGetInformations[vGetIdx].Count == vGetQueryCounts[vGetIdx],
                "not equal query count "
                << " vGetIdx# " << vGetIdx
                << " vGetInformations# " << VectorToString(vGetInformations)
                << " vGetQueryCounts# " << VectorToString(vGetQueryCounts));
    }

    for (ui64 idx = 0; idx < vGetInformations.size(); ++idx) {
        TMap<TActorId, TGetRequest> lastRequest;
        TAutoPtr<IEventHandle> handle;
        auto vget = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVGet>(handle);
        UNIT_ASSERT_C(vget->Record.ExtremeQueriesSize(), "vget without queries vGet# " << vget->ToString());
        auto firstQuery = vget->Record.GetExtremeQueries(0);
        TLogoBlobID firstBlobId = LogoBlobIDFromLogoBlobID(firstQuery.GetId());
        ui64 step = firstBlobId.Step();
        UNIT_ASSERT_C(firstQueryStepToVGetIdx.count(step),
                "not expected first blobId step"
                << " idx# " << idx
                << " step# " << step
                << " firstSteps# " << FormatList(firstSteps)
                << " vGet# " << vget->ToString()
                << " vGetInformations# " << VectorToString(vGetInformations)
                << " vGetQueryCounts# " << VectorToString(vGetQueryCounts));
        ui64 vGetIdx = firstQueryStepToVGetIdx[step];
        TTestVGetInfo &vGetInfo = vGetInformations[vGetIdx];
        lastRequest[handle->Recipient].SetFrom(handle.Get(), vget);
        UNIT_ASSERT_C(vget->Record.ExtremeQueriesSize() == vGetInfo.Count,
                "vget query count not equal predicted count"
                << " idx# " << idx
                << " vGetIdx# " << vGetIdx
                << " realQueryCount# " << vget->Record.ExtremeQueriesSize()
                << " predictedQueryCount# " << vGetInfo.Count
                << " vGet# " << vget->ToString()
                << " vGetInformations# " << VectorToString(vGetInformations)
                << " vGetQueryCounts# " << VectorToString(vGetQueryCounts));
        UNIT_ASSERT_C(vget->Record.ByteSizeLong() <= vGetInfo.ProtobufSize,
                "real protobuf size greater than predicted size"
                << " idx# " << idx
                << " vGetIdx# " << vGetIdx
                << " realProtoBufSize# " << vget->Record.ByteSizeLong()
                << " predictedProtoBufSize# " << vGetInfo.ProtobufSize
                << " vGet# " << vget->ToString()
                << " vGetInformations# " << VectorToString(vGetInformations)
                << " vGetQueryCounts# " << VectorToString(vGetQueryCounts));
        SendVGetResult(0, 0, NKikimrProto::OK, blobSubgroups, lastRequest, runtime);
    }

    TAutoPtr<IEventHandle> handle;
    auto getResult = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvGetResult>(handle);
    UNIT_ASSERT(getResult);
    UNIT_ASSERT(getResult->Status == NKikimrProto::OK);
    UNIT_ASSERT(getResult->ResponseSz == blobCount);
    UNIT_ASSERT(getResult->Responses[0].Status == NKikimrProto::OK);
}

Y_UNIT_TEST(TestProtobufSizeWithMultiGet) {
    MakeTestProtobufSizeWithMultiGet({TBlobPack{223, 300'000}, TBlobPack{1, 186'332}}, {224}); // MaxProtobufSize - 1
    MakeTestProtobufSizeWithMultiGet({TBlobPack{223, 300'000}, TBlobPack{1, 186'333}}, {224}); // MaxProtobufSize
    MakeTestProtobufSizeWithMultiGet({TBlobPack{223, 300'000}, TBlobPack{1, 186'334}}, {223, 1}); // MaxProtobufSize + 1

    auto blobPacks = {TBlobPack{223, 300'000}, TBlobPack{1, 186'333}, TBlobPack{223, 300'000}, TBlobPack{1, 186'334}};
    MakeTestProtobufSizeWithMultiGet(blobPacks, {224, 223, 1});
}

Y_UNIT_TEST(TestGivenStripe42GetThenVGetResponsePartsNodata263451ThenGetOk) {
    TTestBasicRuntime runtime(1, false);
    TBlobStorageGroupType type = {TErasureType::Erasure4Plus2Stripe};
    Setup(runtime, type);

    TActorId proxy = MakeBlobStorageProxyID(GROUP_ID);
    TActorId sender = runtime.AllocateEdgeActor(0);

    TString data;
    data.resize(1209816, 'x');
    TLogoBlobID logoblobid(0x10010000001000Bull, 5, 58949, 1, 1209816, 10);

    TVector<TVDiskState> subgroup;
    PrepareBlobSubgroup(logoblobid, data, subgroup, runtime, type);

    runtime.Send(new IEventHandle(proxy, sender, new TEvBlobStorage::TEvGet(logoblobid, 0, 0, TInstant::Max(),
        NKikimrBlobStorage::EGetHandleClass::FastRead)));
    for (ui32 i = 0; i < 8; ++i) {
        TAutoPtr<IEventHandle> handle;
        auto vget = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVGet>(handle);
        UNIT_ASSERT(vget);
        for (size_t idx = 0; idx < subgroup.size(); ++idx) {
            if (subgroup[idx].ActorId == handle->Recipient) {
                subgroup[idx].SetCookiesAndSenderFrom(handle.Get(), vget);
            }
        }
    }

    SendVGetResult(7, NKikimrProto::NODATA, 1, subgroup, runtime);
    SendVGetResult(1, NKikimrProto::OK, 2, subgroup, runtime);
    SendVGetResult(5, NKikimrProto::OK, 6, subgroup, runtime);
    SendVGetResult(2, NKikimrProto::OK, 3, subgroup, runtime);
    SendVGetResult(6, NKikimrProto::OK, 6, subgroup, runtime);
    SendVGetResult(3, NKikimrProto::OK, 4, subgroup, runtime);
    SendVGetResult(4, NKikimrProto::OK, 5, subgroup, runtime);
    SendVGetResult(0, NKikimrProto::OK, 1, subgroup, runtime);

    TAutoPtr<IEventHandle> handle;
    auto getResult = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvGetResult>(handle);
    UNIT_ASSERT(getResult);
    UNIT_ASSERT(getResult->Status == NKikimrProto::OK);
    UNIT_ASSERT(getResult->ResponseSz == 1);
    UNIT_ASSERT(getResult->Responses[0].Status == NKikimrProto::OK);
}

Y_UNIT_TEST(TestGivenStripe42WhenGet2PartsOfBlobThenGetOk) {
    // Arrange
    TTestBasicRuntime runtime(1, false);
    TBlobStorageGroupType type = {TErasureType::Erasure4Plus2Stripe};
    Setup(runtime, type);

    TActorId proxy = MakeBlobStorageProxyID(GROUP_ID);
    TActorId sender = runtime.AllocateEdgeActor(0);

    TString data;
    data.resize(1209816, 'x');
    TVector<TLogoBlobID> logoblobids;
    TVector<ui32> offsets;
    TVector<ui32> sizes;

    logoblobids.push_back(TLogoBlobID(0x10010000001000Bull, 5, 58949, 1, 1209816, 10));
    offsets.push_back(0);
    sizes.push_back(100);

    logoblobids.push_back(TLogoBlobID(0x10010000001000Bull, 5, 58949, 1, 1209816, 10));
    offsets.push_back(1179648);
    sizes.push_back(100);

    TVector<TVector<TVDiskState>> blobSubgroups;
    blobSubgroups.resize(logoblobids.size());
    TMap<TActorId, TGetRequest> lastRequest;

    for (ui32 i = 0; i < logoblobids.size(); ++i) {
        PrepareBlobSubgroup(logoblobids[i], data, blobSubgroups[i], runtime, type);
    }

    // Act
    TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queries(new TEvBlobStorage::TEvGet::TQuery[2]);
    for (ui32 i = 0; i < logoblobids.size(); ++i) {
        auto &q = queries[i];
        q.Id = logoblobids[i];
        q.Shift = offsets[i];
        q.Size = sizes[i];
    }
    runtime.Send(new IEventHandle(proxy, sender, new TEvBlobStorage::TEvGet(
        queries, (ui32)logoblobids.size(), TInstant::Max(),
        NKikimrBlobStorage::EGetHandleClass::AsyncRead, false)));

    for (ui32 i = 0; i < 8; ++i) {
        TAutoPtr<IEventHandle> handle;
        auto vget = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVGet>(handle);
        UNIT_ASSERT(vget);
        lastRequest[handle->Recipient].SetFrom(handle.Get(), vget);
    }
    runtime.EnableScheduleForActor(lastRequest.begin()->second.Sender, true);

    for (ui32 vDiskIdx = 0; vDiskIdx < 8; ++vDiskIdx) {
        SendVGetResult(0, vDiskIdx, NKikimrProto::OK, blobSubgroups, lastRequest, runtime);
    }
    TAutoPtr<IEventHandle> handle;
    auto getResult = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvGetResult>(handle);
    // Assert
    UNIT_ASSERT(getResult);
    UNIT_ASSERT(getResult->Status == NKikimrProto::OK);
    UNIT_ASSERT(getResult->ResponseSz == 2);
    for (ui32 idx = 0; idx < 2; ++idx) {
        UNIT_ASSERT_C(getResult->Responses[idx].Status == NKikimrProto::OK, "Status# " <<
            NKikimrProto::EReplyStatus_Name(getResult->Responses[idx].Status) << " idx# " << idx);
    }
}

Y_UNIT_TEST(TestGivenBlock42IntersectingPutWhenNodataOkThenOk) {
    TTestBasicRuntime runtime(1, false);
    TBlobStorageGroupType type = {TErasureType::Erasure4Plus2Block};
    Setup(runtime, type);

    TActorId proxy = MakeBlobStorageProxyID(GROUP_ID);
    TActorId sender = runtime.AllocateEdgeActor(0);

    TString data;
    data.resize(1000, 'x');
    TVector<TLogoBlobID> logoblobids;
    TVector<ui32> offsets;
    TVector<ui32> sizes;
    for (ui32 i = 0; i < 4; ++i) {
        logoblobids.push_back(TLogoBlobID(1, 1, 0, 0, (ui32)data.size(), 0));
        offsets.push_back(100 + i);
        sizes.push_back(200);
    }
    TVector<TVector<TVDiskState>> blobSubgroups;
    blobSubgroups.resize(logoblobids.size());
    TMap<TActorId, TGetRequest> lastRequest;

    for (ui32 i = 0; i < logoblobids.size(); ++i) {
        PrepareBlobSubgroup(logoblobids[i], data, blobSubgroups[i], runtime, type);
    }

    TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queries(new TEvBlobStorage::TEvGet::TQuery[4]);
    for (ui32 i = 0; i < logoblobids.size(); ++i) {
        auto &q = queries[i];
        q.Id = logoblobids[i];
        q.Shift = offsets[i];
        q.Size = sizes[i];
    }
    runtime.Send(new IEventHandle(proxy, sender, new TEvBlobStorage::TEvGet(
        queries, (ui32)logoblobids.size(), TInstant::Max(),
        NKikimrBlobStorage::EGetHandleClass::AsyncRead, false)));

    for (ui32 i = 0; i < 4; ++i) {
        TAutoPtr<IEventHandle> handle;
        auto vget = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVGet>(handle);
        UNIT_ASSERT(vget);
        lastRequest[handle->Recipient].SetFrom(handle.Get(), vget);
    }
    runtime.EnableScheduleForActor(lastRequest.begin()->second.Sender, true);

    SendVGetResult(0, 0, NKikimrProto::NODATA, blobSubgroups, lastRequest, runtime);
    for (ui32 vDiskIdx = 1; vDiskIdx < 8; ++vDiskIdx) {
        SendVGetResult(0, vDiskIdx, NKikimrProto::OK, blobSubgroups, lastRequest, runtime);
    }
    TAutoPtr<IEventHandle> handle;
    auto getResult = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvGetResult>(handle);
    UNIT_ASSERT(getResult);
    UNIT_ASSERT(getResult->Status == NKikimrProto::OK);
    UNIT_ASSERT(getResult->ResponseSz == 4);
    for (ui32 idx = 0; idx < 4; ++idx) {
        UNIT_ASSERT_C(getResult->Responses[idx].Status == NKikimrProto::OK, "Status# " <<
            NKikimrProto::EReplyStatus_Name(getResult->Responses[idx].Status) << " idx# " << idx);
    }
}

Y_UNIT_TEST(TestGivenBlock42PutWhenPartialGetThenSingleDiskRequestOk) {
    TTestBasicRuntime runtime(1, false);
    TBlobStorageGroupType type = {TErasureType::Erasure4Plus2Block};
    Setup(runtime, type);

    TActorId proxy = MakeBlobStorageProxyID(GROUP_ID);
    TActorId sender = runtime.AllocateEdgeActor(0);

    TString data;
    data.resize(400 << 10);
    for (ui64 i = 0; i < data.size(); ++i) {
        *const_cast<char *>(data.data() + i) = (char)(i % 251);
    }
    TLogoBlobID logoblobid(1, 2, 3, 4, (ui32)data.size(), 5);
    TVector<TVDiskState> blobSubgroup;
    PrepareBlobSubgroup(logoblobid, data, blobSubgroup, runtime, type);

    for (ui32 step = 0; step < 32; ++step) {
        for (ui32 part = 0; part < 4; ++part) {
            for (ui32 disk = 0; disk < 3; ++disk) {
                TMap<TActorId, TGetRequest> lastRequest;
                // Send Get
                ui32 shift = ((25 + part * 100) << 10) + step;
                ui32 size = 50 << 10;
                TGetRequest theRequest;
                TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queries(new TEvBlobStorage::TEvGet::TQuery[1]);
                {
                    auto &q = queries[0];
                    q.Id = logoblobid;
                    q.Shift = shift;
                    q.Size = size;
                }
                SetPredictedDelaysForAllQueues({});
                runtime.Send(
                    new IEventHandle(
                        proxy, sender, new TEvBlobStorage::TEvGet(
                        queries, 1, TInstant::Max(),
                        NKikimrBlobStorage::EGetHandleClass::FastRead, false)));

                // Receive VGet
                TAutoPtr<IEventHandle> handle;
                for (ui32 miss = 0; miss < disk; ++miss) {
                    auto vget = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVGet>(handle);
                    lastRequest[handle->Recipient].SetFrom(handle.Get(), vget);
                }
                auto vget = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVGet>(handle);
                ui64 msgId = vget->Record.GetMsgQoS().GetMsgId().GetMsgId();
                ui64 sequenceId = vget->Record.GetMsgQoS().GetMsgId().GetSequenceId();
                UNIT_ASSERT(vget);
                lastRequest[handle->Recipient].SetFrom(handle.Get(), vget);
                theRequest.SetFrom(handle.Get(), vget);
                for (ui32 miss = disk + 1; miss < 3; ++miss) {
                    TAutoPtr<IEventHandle> handle2;
                    auto vget2 = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVGet>(handle2);
                    lastRequest[handle2->Recipient].SetFrom(handle2.Get(), vget2);
                }
                UNIT_ASSERT(theRequest.Queries.size() == 1);
                TGetQuery &query = theRequest.Queries[0];
                UNIT_ASSERT(query.LogoBlobId.PartId() == part + 1);

                runtime.EnableScheduleForActor(theRequest.Sender, true);

                // Send VGetResult
                TLogoBlobID id(query.LogoBlobId, query.LogoBlobId.PartId());
                TString resultData = blobSubgroup[query.LogoBlobId.PartId() - 1].Data.substr(query.Shift, query.Size);
                std::unique_ptr<TEvBlobStorage::TEvVGetResult> result(
                    new TEvBlobStorage::TEvVGetResult(
                        NKikimrProto::OK, theRequest.VDiskId, TAppData::TimeProvider->Now(), 0, nullptr,
                        nullptr, nullptr, nullptr, {}, 0U, 0U));
                result->AddResult(NKikimrProto::OK, id, query.Shift, TRope(resultData), &query.QueryCookie);
                result->Record.MutableMsgQoS()->MutableMsgId()->SetMsgId(msgId);
                result->Record.MutableMsgQoS()->MutableMsgId()->SetSequenceId(sequenceId);
                result->Record.SetCookie(theRequest.RecordCookie);
                runtime.Send(
                    new IEventHandle(theRequest.Sender, theRequest.ActorId, result.release(), 0, theRequest.Cookie));

                // Receive GetResult
                auto getResult = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvGetResult>(handle);
                UNIT_ASSERT(getResult);
                UNIT_ASSERT(getResult->Status == NKikimrProto::OK);
                UNIT_ASSERT(getResult->ResponseSz == 1);
                UNIT_ASSERT_C(getResult->Responses[0].Status == NKikimrProto::OK, "Status# " <<
                    NKikimrProto::EReplyStatus_Name(getResult->Responses[0].Status));
                TString expectedData = data.substr(shift, size);
                TString actualData = getResult->Responses[0].Buffer.ConvertToString();
                UNIT_ASSERT_STRINGS_EQUAL_C(expectedData, actualData, "ExpectedSize# " << expectedData.size()
                    << " resultSize$ " << actualData.size() << " part# " << part << " disk# " << disk
                    << " expectedFirst# " << (ui32) (ui8) expectedData[0] << " actualFirst# " <<
                    (ui32) (ui8) actualData[0]);

                // Send responses in order for queues to progress
                for (const auto &item: lastRequest) {
                    const auto &request = item.second;
                    std::unique_ptr<TEvBlobStorage::TEvVGetResult> result(new TEvBlobStorage::TEvVGetResult(
                        NKikimrProto::RACE, request.VDiskId, TAppData::TimeProvider->Now(), 0, nullptr,
                        nullptr, nullptr, nullptr, {}, 0U, 0U));
                    result->Record.MutableMsgQoS()->MutableMsgId()->SetMsgId(request.MsgId);
                    result->Record.MutableMsgQoS()->MutableMsgId()->SetSequenceId(request.SequenceId);
                    result->Record.SetCookie(request.RecordCookie);
                    runtime.Send(new IEventHandle(
                        request.Sender, request.ActorId, result.release(), 0, request.Cookie));
                }

            }
        }
    }
}

Y_UNIT_TEST(TestGivenBlock42Put6PartsOnOneVDiskWhenDiscoverThenRecoverFirst) {
    TTestBasicRuntime runtime(1, false);
    TBlobStorageGroupType type = {TErasureType::Erasure4Plus2Block};
    Setup(runtime, type);

    TActorId proxy = MakeBlobStorageProxyID(GROUP_ID);
    TActorId sender = runtime.AllocateEdgeActor(0);

    TString data;
    data.resize(400 << 10);
    for (ui64 i = 0; i < data.size(); ++i) {
        *const_cast<char *>(data.data() + i) = (char)(i / 1024);
    }
    TLogoBlobID logoblobid(1, 2, 3, 4, (ui32)data.size(), 5);
    TVector<TVector<TVDiskState>> blobSubgroups;
    blobSubgroups.resize(1);
    PrepareBlobSubgroup(logoblobid, data, blobSubgroups[0], runtime, type);


    const ui64 tabletId = 1;
    const ui32 minGeneration = 0;
    // Send Discover
    runtime.Send(new IEventHandle(
        proxy, sender, new TEvBlobStorage::TEvDiscover(tabletId, minGeneration, true, false, TInstant::Max(), 0, true)));

    // Receive VGet
    TMap<TActorId, TGetRequest> lastRequest;
    for (ui32 i = 0; i < 8; ++i) {
        TAutoPtr<IEventHandle> handle;
        auto vget = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVGet>(handle);
        UNIT_ASSERT(vget);
        lastRequest[handle->Recipient].SetFrom(handle.Get(), vget);
        TGetRequest &req = lastRequest[handle->Recipient];
        UNIT_ASSERT(req.RangeQueries.size() == 1);
        TGetRangeQuery &query = req.RangeQueries[0];
        UNIT_ASSERT(query.FromId > query.ToId);
    }
    runtime.EnableScheduleForActor(lastRequest.begin()->second.Sender, true);

    TActorId firstHandoffActorId = blobSubgroups[0][6].ActorId;

    {
        // Send 6 part VGetResult from the first handoff vDiskIdx# 6
        TGetRequest &req = lastRequest[firstHandoffActorId];
        TGetRangeQuery &query = req.RangeQueries[0];
        std::unique_ptr<TEvBlobStorage::TEvVGetResult> result(new TEvBlobStorage::TEvVGetResult(
                NKikimrProto::OK, req.VDiskId, TAppData::TimeProvider->Now(), 0, nullptr, nullptr, nullptr,
                nullptr, {}, 0U, 0U));
        TIngress ingress;
        for (ui32 partIdx = 0; partIdx < 6; ++partIdx) {
            TLogoBlobID blobPartId(logoblobid, partIdx + 1);
            TIngress partIngress(*TIngress::CreateIngressWithLocal(&DSProxyEnv.Info->GetTopology(), req.VDiskId, blobPartId));
            ingress.Merge(partIngress);
        }
        const ui64 ingressRaw = ingress.Raw();
        result->AddResult(NKikimrProto::OK, logoblobid, 0, 0u, &query.QueryCookie, &ingressRaw);
        result->Record.MutableMsgQoS()->MutableMsgId()->SetMsgId(req.MsgId);
        result->Record.MutableMsgQoS()->MutableMsgId()->SetSequenceId(req.SequenceId);
        runtime.Send(new IEventHandle(req.Sender, req.ActorId, result.release(), 0, req.Cookie));
    }

    for (auto iter = lastRequest.begin(); iter != lastRequest.end(); ++iter) {
        if (iter->first == firstHandoffActorId) {
            continue;
        }
        // Send Nodata VGetResult
        TGetRequest &req = iter->second;
        //TGetRangeQuery &query = req.RangeQueries[0];
        std::unique_ptr<TEvBlobStorage::TEvVGetResult> result(new TEvBlobStorage::TEvVGetResult(
                NKikimrProto::OK, req.VDiskId, TAppData::TimeProvider->Now(), 0, nullptr, nullptr, nullptr,
                nullptr, {}, 0U, 0U));
        result->Record.MutableMsgQoS()->MutableMsgId()->SetMsgId(req.MsgId);
        result->Record.MutableMsgQoS()->MutableMsgId()->SetSequenceId(req.SequenceId);
        runtime.Send(
            new IEventHandle(req.Sender, req.ActorId, result.release(), 0, req.Cookie));
    }

    lastRequest.clear();

    /*
    // Receive "full" VGet request set
    for (ui32 i = 0; i < 8; ++i) {
        TAutoPtr<IEventHandle> handle;
        auto vget = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVGet>(handle);
        UNIT_ASSERT(vget);
        lastRequest[handle->Recipient].SetFrom(handle.Get(), vget);
    }

    for (auto iter = lastRequest.begin(); iter != lastRequest.end(); ++iter) {
        if (iter->first == firstHandoffActorId) {
            continue;
        }
        // Send Nodata VGetResult
        TGetRequest &req = iter->second;
        TGetQuery &query = req.Queries[0];
        std::unique_ptr<TEvBlobStorage::TEvVGetResult> result(
            new TEvBlobStorage::TEvVGetResult(
                NKikimrProto::OK, req.VDiskId, TAppData::TimeProvider->Now(), 0, nullptr, nullptr, nullptr,
                nullptr, 0));
        result->AddResult(
            NKikimrProto::NODATA, query.LogoBlobId, 0, query.Shift, nullptr,
            0, &query.QueryCookie);
        result->Record.MutableMsgQoS()->MutableMsgId()->SetMsgId(req.MsgId);
        result->Record.MutableMsgQoS()->MutableMsgId()->SetSequenceId(req.SequenceId);
        runtime.Send(
            new IEventHandle(req.Sender, req.ActorId, result.release(), 0, req.Cookie));
    }
    */

    {
        // Receive DiscoverResult
        TAutoPtr<IEventHandle> handle;
        auto discoverResult = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvDiscoverResult>(handle);
        UNIT_ASSERT(discoverResult);
        UNIT_ASSERT(discoverResult->Status == NKikimrProto::NODATA);
    }
}

Y_UNIT_TEST(TestBlock42CheckLwtrack) {
    NLWTrace::TManager mngr(*Singleton<NLWTrace::TProbeRegistry>(), true);
    NLWTrace::TOrbit orbit;
    NLWTrace::TTraceRequest req;
    req.SetIsTraced(true);
    mngr.HandleTraceRequest(req, orbit);


    TTestBasicRuntime runtime(1, false);
    TBlobStorageGroupType type = {TErasureType::Erasure4Plus2Block};
    Setup(runtime, type);
    runtime.SetLogPriority(NKikimrServices::BS_PROXY_GET, NLog::PRI_DEBUG);

    TActorId proxy = MakeBlobStorageProxyID(GROUP_ID);
    TActorId sender = runtime.AllocateEdgeActor(0);

    TString data("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
    TLogoBlobID logoblobid(1, 0, 0, 0, (ui32)data.size(), 0);

    TVector<TVDiskState> subgroup;
    PrepareBlobSubgroup(logoblobid, data, subgroup, runtime, type);

    auto ev = new TEvBlobStorage::TEvGet(logoblobid, 0, 0, TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::FastRead);
    ev->Orbit = std::move(orbit);

    runtime.Send(new IEventHandle(proxy, sender, ev));
    for (ui32 i = 0; i < 6; ++i) {
        TAutoPtr<IEventHandle> handle;
        auto vget = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVGet>(handle);
        UNIT_ASSERT(vget);
        for (size_t idx = 0; idx < subgroup.size(); ++idx) {
            if (subgroup[idx].ActorId == handle->Recipient) {
                subgroup[idx].SetCookiesAndSenderFrom(handle.Get(), vget);
            }
        }
    }

    SendVGetResult(6, NKikimrProto::OK, 2, subgroup, runtime);
    SendVGetResult(4, NKikimrProto::OK, 5, subgroup, runtime);
    SendVGetResult(1, NKikimrProto::OK, 2, subgroup, runtime);
    SendVGetResult(2, NKikimrProto::OK, 3, subgroup, runtime);
    SendVGetResult(7, NKikimrProto::NODATA, 1, subgroup, runtime);
    SendVGetResult(3, NKikimrProto::OK, 4, subgroup, runtime);
    SendVGetResult(5, NKikimrProto::OK, 6, subgroup, runtime);
    SendVGetResult(0, NKikimrProto::OK, 1, subgroup, runtime);

    TAutoPtr<IEventHandle> handle;
    auto getResult = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvGetResult>(handle);
    UNIT_ASSERT(getResult);
    UNIT_ASSERT(getResult->Status == NKikimrProto::OK);
    UNIT_ASSERT(getResult->ResponseSz == 1);
    UNIT_ASSERT(getResult->Responses[0].Status == NKikimrProto::OK);

    NLWTrace::TTraceResponse resp;
    getResult->Orbit.Serialize(0, *resp.MutableTrace());
    auto& r = resp.GetTrace();
    UNIT_ASSERT_VALUES_EQUAL(21, r.EventsSize());

    {
        const auto& p = r.GetEvents(0);
        UNIT_ASSERT_VALUES_EQUAL("BLOBSTORAGE_PROVIDER", p.GetProvider());
        UNIT_ASSERT_VALUES_EQUAL("DSProxyGetHandle", p.GetName());
        UNIT_ASSERT_VALUES_EQUAL(0 , p.ParamsSize());
    }

    {
        const auto& p = r.GetEvents(1);
        UNIT_ASSERT_VALUES_EQUAL("DSProxyGetBootstrap", p.GetName());
        UNIT_ASSERT_VALUES_EQUAL(0 , p.ParamsSize());
    }

    {
        const auto& p = r.GetEvents(2);
        UNIT_ASSERT_VALUES_EQUAL("DSProxyGetRequest", p.GetName());
        // check groupId
        UNIT_ASSERT_VALUES_EQUAL(0, p.GetParams(0).GetUintValue());
        // check deviceType
        UNIT_ASSERT_VALUES_EQUAL("DEVICE_TYPE_UNKNOWN(255)", p.GetParams(1).GetStrValue());
        // check handleClass
        UNIT_ASSERT_VALUES_EQUAL("FastRead", p.GetParams(2).GetStrValue());
    }

    TVector<ui32> vdiskOrderNum = {0, 1, 4, 5, 6, 7};
    for (auto i = 3; i < 9; ++i) {
        const auto& p = r.GetEvents(i);
        UNIT_ASSERT_VALUES_EQUAL("DSProxyVGetSent", p.GetName());
        // check vdiskId
        UNIT_ASSERT_VALUES_EQUAL("[0:_:0:" + ToString(vdiskOrderNum[i-3]) + ":0]", p.GetParams(0).GetStrValue());
        // check vdiskOrderNum
        UNIT_ASSERT_VALUES_EQUAL(vdiskOrderNum[i-3], p.GetParams(1).GetUintValue());
        // check vgets count
        UNIT_ASSERT_VALUES_EQUAL(6, p.GetParams(2).GetUintValue());
    }

    for (auto i = 9; i < 13; ++i) {
        const auto& p = r.GetEvents(i);
        UNIT_ASSERT_VALUES_EQUAL("DSProxyVDiskRequestDuration", p.GetName());
    }

    for (auto i = 13; i < 15; ++i) {
        const auto& p = r.GetEvents(i);
        UNIT_ASSERT_VALUES_EQUAL("DSProxyScheduleAccelerate", p.GetName());
         UNIT_ASSERT_VALUES_EQUAL("Get", p.GetParams(1).GetStrValue());
    }

    for (auto i = 15; i < 17; ++i) {
        const auto& p = r.GetEvents(i);
        UNIT_ASSERT_VALUES_EQUAL("DSProxyVDiskRequestDuration", p.GetName());
    }

    {
        const auto& p = r.GetEvents(17);
        UNIT_ASSERT_VALUES_EQUAL("DSProxyStartTransfer", p.GetName());
        UNIT_ASSERT_VALUES_EQUAL(0 , p.ParamsSize());
    }

    {
        const auto& p = r.GetEvents(18);
        UNIT_ASSERT_VALUES_EQUAL("VDiskStartProcessing", p.GetName());
        UNIT_ASSERT_VALUES_EQUAL(0 , p.ParamsSize());
    }

    {
        const auto& p = r.GetEvents(19);
        UNIT_ASSERT_VALUES_EQUAL("VDiskReply", p.GetName());
        UNIT_ASSERT_VALUES_EQUAL(0 , p.ParamsSize());
    }

    {
        const auto& p = r.GetEvents(20);
        UNIT_ASSERT_VALUES_EQUAL("DSProxyGetReply", p.GetName());
        UNIT_ASSERT_VALUES_EQUAL(0 , p.ParamsSize());
    }
}

} // Y_UNIT_TEST_SUITE TBlobStorageProxySequenceTest
} // namespace NBlobStorageProxySequenceTest
} // namespace NKikimr
