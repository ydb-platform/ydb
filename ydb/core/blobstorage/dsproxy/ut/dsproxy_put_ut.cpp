#include "defs.h"
#include "dsproxy_env_mock_ut.h"
#include "dsproxy_test_state_ut.h"

#include <ydb/core/blobstorage/dsproxy/dsproxy_put_impl.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_request_reporting.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/testlib/actors/block_events.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

namespace NKikimr {

extern const char* GetZeroDataAddrForTestOnly();

namespace NDSProxyPutTest {

Y_UNIT_TEST_SUITE(TDSProxyPutTest) {

TString AlphaData(ui32 size) {
    TString data = TString::Uninitialized(size);
    ui8 *p = (ui8*)(void*)data.Detach();
    for (ui32 offset = 0; offset < size; ++offset) {
        p[offset] = (ui8)offset;
    }
    return data;
}

class TRcBufCustomBackend : public IContiguousChunk {
    TString Buffer;
public:
    TRcBufCustomBackend(size_t sz)
    {
        Buffer.resize(sz);
    }

    TContiguousSpan GetData() const override {
        return {Buffer.data(), Buffer.size()};
    }

    TMutableContiguousSpan UnsafeGetDataMut() override {
        return {const_cast<char*>(Buffer.data()), Buffer.size()};
    }

    size_t GetOccupiedMemorySize() const override {
        return Buffer.capacity();
    }

    IContiguousChunk::TPtr Clone() override {
        return this;
    }

    EInnerType GetInnerType() const noexcept override {
        return RDMA_MEM_REG;
    }
};

class TRcBufTestAllocator final : public IRcBufAllocator {
public:
    TRcBuf AllocRcBuf(size_t size, size_t headRoom, size_t tailRoom) noexcept {
        auto region = MakeIntrusive<TRcBufCustomBackend>(size + headRoom + tailRoom);
        return TRcBuf(IContiguousChunk::TPtr(region));
    }

    TRcBuf AllocPageAlignedRcBuf(size_t size, size_t tailRoom) noexcept {
        return AllocRcBuf(size, 0, tailRoom);
    }
};

void TestPutMaxPartCountOnHandoff(TErasureType::EErasureSpecies erasureSpecies, bool zeroPages) {
    TRcBufTestAllocator rcBufAllocator;

    TActorSystemStub actorSystemStub;
    i32 size = 786;
    TLogoBlobID blobId(72075186224047637, 1, 863, 1, size, 24576);
    TString data = AlphaData(size);

    const ui32 groupId = 0;
    TBlobStorageGroupType groupType(erasureSpecies);
    const ui32 domainCount = groupType.BlobSubgroupSize();;

    TGroupMock group(groupId, erasureSpecies, domainCount, 1, 1);
    TIntrusivePtr<TGroupQueues> groupQueues = group.MakeGroupQueues();

    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters(new ::NMonitoring::TDynamicCounters());
    TIntrusivePtr<TDsProxyNodeMon> nodeMon(new TDsProxyNodeMon(counters, true));
    TIntrusivePtr<TBlobStorageGroupProxyMon> mon(new TBlobStorageGroupProxyMon(counters, counters, counters,
                group.GetInfo(), nodeMon, false));

    TLogContext logCtx(NKikimrServices::BS_PROXY_PUT, false);
    logCtx.LogAcc.IsLogEnabled = false;

    const ui32 hash = blobId.Hash();
    const ui32 totalvd = group.GetInfo()->Type.BlobSubgroupSize();
    const ui32 totalParts = group.GetInfo()->Type.TotalPartCount();
    Y_ABORT_UNLESS(blobId.BlobSize() == data.size());
    Y_ABORT_UNLESS(totalvd >= totalParts);
    TBlobStorageGroupInfo::TServiceIds vDisksSvc;
    TBlobStorageGroupInfo::TVDiskIds vDisksId;
    group.GetInfo()->PickSubgroup(hash, &vDisksId, &vDisksSvc);

    TRcBuf encryptedData = rcBufAllocator.AllocRcBuf(data.size(), 0, 0);
    UNIT_ASSERT_VALUES_EQUAL(encryptedData.GetContiguousSpanMut().size(), data.size());
    UNIT_ASSERT_VALUES_EQUAL(encryptedData.GetContiguousSpanMut().size(), encryptedData.size());

    memcpy(encryptedData.GetContiguousSpanMut().data(), data.data(), data.size());
    char *dataBytes = encryptedData.GetContiguousSpanMut().data();
    Encrypt(dataBytes, dataBytes, 0, encryptedData.size(), blobId, *group.GetInfo());

    TBatchedVec<TStackVec<TRope, TypicalPartsInBlob>> partSetSingleton(1);
    partSetSingleton[0].resize(totalParts);
    ErasureSplit((TErasureType::ECrcMode)blobId.CrcMode(), group.GetInfo()->Type, TRope(encryptedData), partSetSingleton[0], nullptr,
        zeroPages ? GetDefaultRcBufAllocator() : &rcBufAllocator);

    TEvBlobStorage::TEvPut ev(blobId, std::move(data), TInstant::Max(), NKikimrBlobStorage::TabletLog,
            TEvBlobStorage::TEvPut::TacticDefault);

    TPutImpl putImpl(group.GetInfo(), groupQueues, &ev, mon, false, TActorId(), 0, NWilson::TTraceId(), TAccelerationParams{}, false);

    for (ui32 idx = 0; idx < domainCount; ++idx) {
        group.SetPredictedDelayNs(idx, 1);
    }
    group.SetPredictedDelayNs(7, 10);

    TPutImpl::TPutResultVec putResults;

    putImpl.GenerateInitialRequests(logCtx, partSetSingleton);
    putImpl.Step(logCtx, putResults, {&group.GetInfo()->GetTopology()}, false);
    auto vPuts = putImpl.GeneratePutRequests();
    group.SetError(0, NKikimrProto::ERROR);

    TVector<ui32> diskSequence = {0, 7, 7, 7, 7, 6, 3, 4, 5, 1, 2};
    TVector<ui32> slowDiskSequence = {3, 4, 5, 6, 1, 2};
    const char* const zero = GetZeroDataAddrForTestOnly();
    ui32 zeroPageCount = 0;

    for (ui32 vPutIdx = 0; vPutIdx < vPuts.size(); ++vPutIdx) {
        ui32 nextVPut = vPutIdx;
        ui32 diskPos = (ui32)-1;
        for (ui32 i = vPutIdx; i < vPuts.size(); ++i) {
            auto rope = std::get<0>(vPuts[i])->GetBuffer();
            if (!zeroPages) {
                for (auto it = rope.Begin(); it != rope.End(); ++it) {
                    const TRcBuf& chunk = it.GetChunk();
                    UNIT_ASSERT(chunk.data() != zero);
                    std::optional<IContiguousChunk::TPtr> underlying = chunk.ExtractFullUnderlyingContainer<IContiguousChunk::TPtr>();
                    UNIT_ASSERT(underlying);
                    UNIT_ASSERT(*underlying);
                    UNIT_ASSERT(underlying->Get()->GetInnerType() == IContiguousChunk::EInnerType::RDMA_MEM_REG);
                }
            } else {
                for (auto it = rope.Begin(); it != rope.End(); ++it) {
                    const TRcBuf& chunk = it.GetChunk();
                    const char* data = chunk.Data();
                    if (data == zero) {
                        zeroPageCount++;
                    }
                }
            }
            auto& record = std::get<0>(vPuts[i])->Record;
            TVDiskID vDiskId = VDiskIDFromVDiskID(record.GetVDiskID());
            ui32 diskIdx = group.VDiskIdx(vDiskId);
            auto it = Find(diskSequence, diskIdx);
            if (it != diskSequence.end()) {
                ui32 pos = it - diskSequence.begin();
                if (pos < diskPos) {
                    nextVPut = i;
                    diskPos = pos;
                }
            }
        }
        if (zeroPages) {
            UNIT_ASSERT(zeroPageCount > 0);
        }
        CTEST << "vdisk exp# " << (diskSequence.size() ? diskSequence.front() : -1) << " get# " << group.VDiskIdx(VDiskIDFromVDiskID(std::get<0>(vPuts[nextVPut])->Record.GetVDiskID())) << Endl;
        if (diskPos != (ui32)-1) {
            diskSequence.erase(diskSequence.begin() + diskPos);
        }
        std::swap(vPuts[vPutIdx], vPuts[nextVPut]);

        for (ui32 idx = 0; idx < domainCount; ++idx) {
            group.SetPredictedDelayNs(idx, 1);
        }
        if (vPutIdx < slowDiskSequence.size()) {
            group.SetPredictedDelayNs(slowDiskSequence[vPutIdx], 10);
        }

        TEvBlobStorage::TEvVPut& vPut = *std::get<0>(vPuts[vPutIdx]);
        TActorId sender;
        TEvBlobStorage::TEvVPutResult vPutResult;
        NKikimrProto::EReplyStatus status = group.OnVPut(vPut);
        vPutResult.MakeError(status, TString(), vPut.Record);

        putImpl.ProcessResponse(vPutResult);
        putImpl.Step(logCtx, putResults, {&group.GetInfo()->GetTopology()}, false);
        auto nextVPuts = putImpl.GeneratePutRequests();

        if (putResults.size()) {
            break;
        }

        std::move(nextVPuts.begin(), nextVPuts.end(), std::back_inserter(vPuts));
    }
    UNIT_ASSERT(putResults.size() == 1);
    auto& [_, result] = putResults.front();
    UNIT_ASSERT(result->Status == NKikimrProto::OK);
    UNIT_ASSERT(result->Id == blobId);
    UNIT_ASSERT_VALUES_EQUAL(putImpl.GetHandoffPartsSent(), 2);
}

Y_UNIT_TEST(TestBlock42MaxPartCountOnHandoff) {
    TestPutMaxPartCountOnHandoff(TErasureType::Erasure4Plus2Block, false);
}

Y_UNIT_TEST(TestBlock42MaxPartCountOnHandoffWithZeropages) {
    TestPutMaxPartCountOnHandoff(TErasureType::Erasure4Plus2Block, true);
}

enum ETestPutAllOkMode {
    TPAOM_VPUT,
    TPAOM_VMULTIPUT
};

template <TErasureType::EErasureSpecies ErasureSpecies, ETestPutAllOkMode TestMode>
struct TTestPutAllOk {
    static constexpr ui32 GroupId = 0;
    static constexpr i32 DataSize = 100500;
    static constexpr bool IsVPut = TestMode == TPAOM_VPUT;
    static constexpr ui64 BlobCount = IsVPut ? 1 : 2;
    static constexpr ui32 MaxIterations = 10000;

    using TPutResultEvent = std::variant<std::unique_ptr<TEvBlobStorage::TEvVPutResult>,
                                         std::unique_ptr<TEvBlobStorage::TEvVMultiPutResult>>;

    TActorSystemStub ActorSystemStub;
    TBlobStorageGroupType GroupType;
    TGroupMock Group;
    TIntrusivePtr<TGroupQueues> GroupQueues;

    TBatchedVec<TLogoBlobID> BlobIds;
    TString Data;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TIntrusivePtr<TDsProxyNodeMon> NodeMon;
    TIntrusivePtr<TBlobStorageGroupProxyMon> Mon;

    TLogContext LogCtx;

    TBatchedVec<TStackVec<TRope, TypicalPartsInBlob>> PartSets;

    TStackVec<ui32, 16> CheckStack;

    TTestPutAllOk()
        : GroupType(ErasureSpecies)
        , Group(GroupId, ErasureSpecies, 1, GroupType.BlobSubgroupSize(), 1)
        , GroupQueues(Group.MakeGroupQueues())
        , BlobIds({TLogoBlobID(743284823, 10, 12345, 0, DataSize, 0), TLogoBlobID(743284823, 9, 12346, 0, DataSize, 0)})
        , Data(AlphaData(DataSize))
        , Counters(new ::NMonitoring::TDynamicCounters())
        , NodeMon(new TDsProxyNodeMon(Counters, true))
        , Mon(new TBlobStorageGroupProxyMon(Counters, Counters, Counters, Group.GetInfo(), NodeMon, false))
        , LogCtx(NKikimrServices::BS_PROXY_PUT, false)
        , PartSets(BlobCount)
    {
        LogCtx.LogAcc.IsLogEnabled = false;

        const ui32 totalvd = Group.GetInfo()->Type.BlobSubgroupSize();
        const ui32 totalParts = Group.GetInfo()->Type.TotalPartCount();
        Y_ABORT_UNLESS(totalvd >= totalParts);

        for (ui64 blobIdx = 0; blobIdx < BlobCount; ++blobIdx) {
            TLogoBlobID blobId = BlobIds[blobIdx];
            Y_ABORT_UNLESS(blobId.BlobSize() == Data.size());
            TBlobStorageGroupInfo::TServiceIds vDisksSvc;
            TBlobStorageGroupInfo::TVDiskIds vDisksId;
            const ui32 hash = blobId.Hash();
            Group.GetInfo()->PickSubgroup(hash, &vDisksId, &vDisksSvc);

            TString encryptedData = Data;
            char *dataBytes = encryptedData.Detach();
            Encrypt(dataBytes, dataBytes, 0, encryptedData.size(), blobId, *Group.GetInfo());

            PartSets[blobIdx].resize(totalParts);
            ErasureSplit((TErasureType::ECrcMode)blobId.CrcMode(), Group.GetInfo()->Type, TRope(encryptedData), PartSets[blobIdx],
                nullptr, GetDefaultRcBufAllocator());
        }
    }

    std::unique_ptr<TEvBlobStorage::TEvVPutResult> InitResult(TEvBlobStorage::TEvVPut& ev) {
        NKikimrProto::EReplyStatus status = Group.OnVPut(ev);
        UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::OK);
        auto vPutResult = std::make_unique<TEvBlobStorage::TEvVPutResult>();
        vPutResult->MakeError(status, TString(), ev.Record);
        return vPutResult;
    }

    std::unique_ptr<TEvBlobStorage::TEvVMultiPutResult> InitResult(TEvBlobStorage::TEvVMultiPut& ev) {
        TVector<NKikimrProto::EReplyStatus> statuses = Group.OnVMultiPut(ev);
        for (auto status : statuses) {
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::OK);
        }
        auto vMultiPutResult = std::make_unique<TEvBlobStorage::TEvVMultiPutResult>();
        Y_ABORT_UNLESS(ev.Record.ItemsSize() == statuses.size());
        vMultiPutResult->MakeError(NKikimrProto::OK, TString(), ev.Record);
        for (ui64 itemIdx = 0; itemIdx < statuses.size(); ++itemIdx) {
            NKikimrBlobStorage::TVMultiPutResultItem &item = *vMultiPutResult->Record.MutableItems(itemIdx);
            NKikimrProto::EReplyStatus status = statuses[itemIdx];
            item.SetStatus(status);
        }
        Y_ABORT_UNLESS(vMultiPutResult->Record.ItemsSize() == statuses.size());
        return vMultiPutResult;
    }

    void InitVPutResults(TDeque<TPutImpl::TPutEvent>& vPuts, TDeque<TPutResultEvent>& vPutResults) {
        for (auto& ev : vPuts) {
            std::visit([&](auto& ev) {
                vPutResults.push_back(InitResult(*ev));
            }, ev);
        }
    }

    void PermutateVPutResults(ui64 resIdx, bool &isAborted, TDeque<TPutResultEvent> &vPutResults) {
        // select result in range [resIdx, vPutResults.size())
        if (resIdx + 1 < CheckStack.size()) {
            ui32 tgt = CheckStack[resIdx];
            UNIT_ASSERT(tgt < vPutResults.size());
            UNIT_ASSERT(tgt >= resIdx);
            std::swap(vPutResults[resIdx], vPutResults[tgt]);
        } else if (resIdx + 1 == CheckStack.size()) {
            ui32 &tgt = CheckStack[resIdx];
            tgt++;
            if (tgt >= vPutResults.size()) {
                isAborted = true;
                CheckStack.pop_back();
                return;
            }
        } else {
            CheckStack.push_back(resIdx);
        }
    }

    bool Step(TPutImpl &putImpl,
            TDeque<TPutResultEvent> &vPutResults,
            TPutImpl::TPutResultVec &putResults)
    {
        bool isAborted = false;
        for (ui64 resIdx = 0; resIdx < vPutResults.size(); ++resIdx) {
            PermutateVPutResults(resIdx, isAborted, vPutResults);
            if (isAborted) {
                break;
            }

            std::visit([&](auto &ev) { putImpl.ProcessResponse(*ev); }, vPutResults[resIdx]);
            putImpl.Step(LogCtx, putResults, &Group.GetInfo()->GetTopology(), false);
            auto vPuts = putImpl.GeneratePutRequests();
            if (putResults.size() == BlobCount) {
                break;
            }

            for (auto& put : vPuts) {
                std::visit([&](auto& ev) { vPutResults.push_back(InitResult(*ev)); }, put);
            }
        }

        return isAborted;
    }

    void Run() {
        ui64 i = 0;
        for (; i < MaxIterations; ++i) {
            Group.Wipe();
            TBatchedVec<TEvBlobStorage::TEvPut::TPtr> events;
            for (auto &blobId : BlobIds) {
                std::unique_ptr<TEvBlobStorage::TEvPut> vPut(new TEvBlobStorage::TEvPut(blobId, Data, TInstant::Max(),
                        NKikimrBlobStorage::TabletLog, TEvBlobStorage::TEvPut::TacticDefault));
                events.emplace_back(static_cast<TEventHandle<TEvBlobStorage::TEvPut> *>(
                        new IEventHandle(TActorId(), TActorId(), vPut.release())));
            }

            TMaybe<TPutImpl> putImpl;
            TPutImpl::TPutResultVec putResults;
            if constexpr (IsVPut) {
                putImpl.ConstructInPlace(Group.GetInfo(), GroupQueues, events[0]->Get(), Mon, false, TActorId(), 0, NWilson::TTraceId(),
                        TAccelerationParams{}, false);
            } else {
                putImpl.ConstructInPlace(Group.GetInfo(), GroupQueues, events, Mon,
                        NKikimrBlobStorage::TabletLog, TEvBlobStorage::TEvPut::TacticDefault, false, TAccelerationParams{}, false);
            }

            putImpl->GenerateInitialRequests(LogCtx, PartSets);
            putImpl->Step(LogCtx, putResults, &Group.GetInfo()->GetTopology(), false);
            auto vPuts = putImpl->GeneratePutRequests();
            UNIT_ASSERT(vPuts.size() == 6 || !IsVPut);
            TDeque<TPutResultEvent> vPutResults;
            InitVPutResults(vPuts, vPutResults);

            bool isAborted = Step(*putImpl, vPutResults, putResults);
            if (!isAborted) {
                UNIT_ASSERT_VALUES_EQUAL(putResults.size(), BlobCount);
                for (auto& [blobIdx, result] : putResults) {
                    UNIT_ASSERT(result->Status == NKikimrProto::OK);
                    UNIT_ASSERT(result->Id == BlobIds[blobIdx]);
                }
            } else {
                if (CheckStack.size() == 0) {
                    break;
                }
            }
        }

        UNIT_ASSERT(i != MaxIterations || !IsVPut);
    }
};

Y_UNIT_TEST(TestBlock42PutAllOk) {
    TTestPutAllOk<TErasureType::Erasure4Plus2Block, TPAOM_VPUT>().Run();
}

Y_UNIT_TEST(TestBlock42MultiPutAllOk) {
    TTestPutAllOk<TErasureType::Erasure4Plus2Block, TPAOM_VMULTIPUT>().Run();
}

Y_UNIT_TEST(TestMirror3dcWith3x3MinLatencyMod) {
    TTestBasicRuntime runtime;
    SetupRuntime(runtime);
    TDSProxyEnv env;
    env.Configure(runtime, TErasureType::ErasureMirror3dc, 1, 0);

    i32 size = 786;
    TLogoBlobID blobId(72075186224047637, 1, 863, 1, size, 24576);
    TString data = AlphaData(size);
    TEvBlobStorage::TEvPut ev(blobId, data, TInstant::Max(), NKikimrBlobStorage::TabletLog,
            TEvBlobStorage::TEvPut::TacticMinLatency);
    TPutImpl putImpl(env.Info, env.GroupQueues, &ev, env.Mon, true, TActorId(), 0, NWilson::TTraceId(), TAccelerationParams{}, false);

    TLogContext logCtx(NKikimrServices::BS_PROXY_PUT, false);
    logCtx.LogAcc.IsLogEnabled = false;

    const ui32 totalParts = env.Info->Type.TotalPartCount();
    TBatchedVec<TStackVec<TRope, TypicalPartsInBlob>> partSetSingleton(1);
    partSetSingleton[0].resize(totalParts);

    TString encryptedData = data;
    char *dataBytes = encryptedData.Detach();
    Encrypt(dataBytes, dataBytes, 0, encryptedData.size(), blobId, *env.Info);
    ErasureSplit((TErasureType::ECrcMode)blobId.CrcMode(), env.Info->Type, TRope(encryptedData), partSetSingleton[0],
        nullptr, GetDefaultRcBufAllocator());
    putImpl.GenerateInitialRequests(logCtx, partSetSingleton);
    TPutImpl::TPutResultVec putResults;
    putImpl.Step(logCtx, putResults, &env.Info->GetTopology(), false);
    auto vPuts = putImpl.GeneratePutRequests();

    UNIT_ASSERT_VALUES_EQUAL(vPuts.size(), 9);
    using TVDiskIDTuple = decltype(std::declval<TVDiskID>().ConvertToTuple());
    THashSet<TVDiskIDTuple> vDiskIds;
    for (auto &vPut : vPuts) {
        TVDiskID vDiskId = VDiskIDFromVDiskID(std::get<0>(vPut)->Record.GetVDiskID());
        bool inserted = vDiskIds.insert(vDiskId.ConvertToTuple()).second;
        UNIT_ASSERT(inserted);
    }
    for (ui32 diskOrderNumber = 0; diskOrderNumber < env.Info->Type.BlobSubgroupSize(); ++diskOrderNumber) {
        TVDiskID vDiskId = env.Info->GetVDiskId(diskOrderNumber);
        auto it = vDiskIds.find(vDiskId.ConvertToTuple());
        UNIT_ASSERT(it != vDiskIds.end());
    }
}

template <bool MultiPut>
void TestMirror3dcMinLatencyNotReady(ui32 failedRealm, bool splitFailures = false,
        NKikimrProto::EReplyStatus firstStatus = NKikimrProto::NOTREADY,
        NKikimrProto::EReplyStatus secondStatus = NKikimrProto::NOTREADY,
        NKikimrProto::EReplyStatus retryStatus = NKikimrProto::NOTREADY) {
    TTestBasicRuntime runtime;
    SetupRuntime(runtime);
    TDSProxyEnv env;
    env.Configure(runtime, TErasureType::ErasureMirror3dc, 1, 0);
    TTestState testState(runtime, env.Info);

    constexpr ui32 blobCount = MultiPut ? 2 : 1;
    const TLogoBlobID blobId(72075186224047637, 1, 863, 1, 786, 24576);
    const TString data = AlphaData(blobId.BlobSize());
    TVector<TBlobTestSet::TBlob> blobs{{blobId, data}};
    if constexpr (MultiPut) {
        TBlobStorageGroupInfo::TOrderNums subgroup;
        env.Info->GetTopology().PickSubgroup(blobId.Hash(), subgroup);
        // Keep the same disk placement so that each request batches both blobs.
        for (ui32 cookie = blobId.Cookie() + 1; cookie < blobId.Cookie() + 1000; ++cookie) {
            TLogoBlobID candidate(blobId.TabletID(), blobId.Generation(), blobId.Step(), blobId.Channel(),
                blobId.BlobSize(), cookie);
            TBlobStorageGroupInfo::TOrderNums candidateSubgroup;
            env.Info->GetTopology().PickSubgroup(candidate.Hash(), candidateSubgroup);
            if (candidateSubgroup == subgroup) {
                blobs.emplace_back(candidate, data);
                break;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(blobs.size(), blobCount);
    }

    using TRequest = std::conditional_t<MultiPut, TEvBlobStorage::TEvVMultiPut, TEvBlobStorage::TEvVPut>;
    using TResult = std::conditional_t<MultiPut, TEvBlobStorage::TEvVMultiPutResult,
        TEvBlobStorage::TEvVPutResult>;
    TBlockEvents<TRequest> requests(runtime);
    TBatchedVec<TEvBlobStorage::TEvPut::TPtr> events;
    testState.CreatePutRequests(blobs, std::back_inserter(events), TEvBlobStorage::TEvPut::TacticMinLatency,
        NKikimrBlobStorage::TabletLog);
    auto putActor = [&] {
        if constexpr (MultiPut) {
            return env.CreatePutRequestActor(events, TEvBlobStorage::TEvPut::TacticMinLatency,
                NKikimrBlobStorage::TabletLog);
        } else {
            return env.CreatePutRequestActor(events.front());
        }
    }();
    runtime.Register(putActor.release());
    // Scheduling is disabled for this actor: only the responses can issue extra requests.
    runtime.SimulateSleep(TDuration::MilliSeconds(1));
    UNIT_ASSERT_VALUES_EQUAL(requests.size(), 6);

    TVector<TVector<typename TRequest::TPtr>> initialRequests(3);
    THashSet<TVDiskID> requestedDisks;
    while (!requests.empty()) {
        auto request = std::move(requests.front());
        requests.pop_front();
        const TVDiskID vdiskId = VDiskIDFromVDiskID(request->Get()->Record.GetVDiskID());
        UNIT_ASSERT(requestedDisks.insert(vdiskId).second);
        if constexpr (MultiPut) {
            UNIT_ASSERT_VALUES_EQUAL(request->Get()->Record.ItemsSize(), blobCount);
        }
        initialRequests[vdiskId.FailRealm].push_back(std::move(request));
    }
    for (const auto& realmRequests : initialRequests) {
        UNIT_ASSERT_VALUES_EQUAL(realmRequests.size(), 2);
    }

    auto reply = [&](typename TRequest::TPtr& request, NKikimrProto::EReplyStatus status) {
        auto result = std::make_unique<TResult>();
        // Use the same NOTREADY result as BS_QUEUE, including multi-put item normalization.
        result->MakeError(status, TString(), request->Get()->Record);
        result->Record.MutableMsgQoS()->MutableMsgId()->CopyFrom(request->Get()->Record.GetMsgQoS().GetMsgId());
        runtime.Send(new IEventHandle(request->Sender, request->Recipient, result.release(), 0, request->Cookie));
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
    };

    reply(initialRequests[failedRealm][0], firstStatus);
    UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
    const TVDiskID retryDisk = VDiskIDFromVDiskID(requests.front()->Get()->Record.GetVDiskID());
    UNIT_ASSERT_VALUES_EQUAL(retryDisk.FailRealm, failedRealm);
    UNIT_ASSERT(requestedDisks.insert(retryDisk).second);
    initialRequests[failedRealm].push_back(std::move(requests.front()));
    requests.pop_front();
    UNIT_ASSERT(runtime.CaptureMailboxEvents(testState.EdgeActor.Hint(), testState.EdgeActor.NodeId()).empty());

    const ui32 secondFailedRealm = splitFailures ? (failedRealm + 1) % 3 : failedRealm;
    reply(initialRequests[secondFailedRealm][splitFailures ? 0 : 1], secondStatus);
    UNIT_ASSERT(runtime.CaptureMailboxEvents(testState.EdgeActor.Hint(), testState.EdgeActor.NodeId()).empty());
    if (splitFailures) {
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(VDiskIDFromVDiskID(requests.front()->Get()->Record.GetVDiskID()).FailRealm,
            secondFailedRealm);
        return;
    }
    // The first two failures only cause a retry on the third disk in this DC.
    UNIT_ASSERT(requests.empty());
    reply(initialRequests[failedRealm][2], retryStatus);
    UNIT_ASSERT(runtime.CaptureMailboxEvents(testState.EdgeActor.Hint(), testState.EdgeActor.NodeId()).empty());
    if (firstStatus != NKikimrProto::NOTREADY || secondStatus != NKikimrProto::NOTREADY
            || retryStatus == NKikimrProto::ERROR) {
        // A DC with ordinary errors does not trigger these extra survivor requests.
        UNIT_ASSERT(requests.empty());
        return;
    }

    TVector<typename TRequest::TPtr> hedges(3);
    if (retryStatus == NKikimrProto::OK) {
        // Recovery of the third disk makes one successful replica in each DC sufficient.
        UNIT_ASSERT(requests.empty());
    } else {
        // All three disks are NOTREADY before any surviving DC has replied.
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 2);
        while (!requests.empty()) {
            auto request = std::move(requests.front());
            requests.pop_front();
            const TVDiskID vdiskId = VDiskIDFromVDiskID(request->Get()->Record.GetVDiskID());
            UNIT_ASSERT(vdiskId.FailRealm != failedRealm);
            UNIT_ASSERT(!hedges[vdiskId.FailRealm]);
            UNIT_ASSERT(requestedDisks.insert(vdiskId).second);
            hedges[vdiskId.FailRealm] = std::move(request);
        }
    }
    for (ui32 realm = 0; realm < 3; ++realm) {
        if (realm != failedRealm) {
            reply(initialRequests[realm][0], NKikimrProto::OK);
        }
    }
    if (retryStatus == NKikimrProto::NOTREADY) {
        UNIT_ASSERT(runtime.CaptureMailboxEvents(testState.EdgeActor.Hint(), testState.EdgeActor.NodeId()).empty());
        for (ui32 realm = 0; realm < 3; ++realm) {
            if (realm != failedRealm) {
                UNIT_ASSERT(hedges[realm]);
                reply(hedges[realm], NKikimrProto::OK);
            }
        }
    }
    // One initial request in each surviving DC is still pending.
    auto results = runtime.CaptureMailboxEvents(testState.EdgeActor.Hint(), testState.EdgeActor.NodeId());
    UNIT_ASSERT_VALUES_EQUAL(results.size(), blobCount);
    THashSet<TLogoBlobID> completed;
    for (const auto& event : results) {
        UNIT_ASSERT_VALUES_EQUAL(event->GetTypeRewrite(), TEvBlobStorage::TEvPutResult::EventType);
        const auto* result = event->Get<TEvBlobStorage::TEvPutResult>();
        UNIT_ASSERT_VALUES_EQUAL(result->Status, NKikimrProto::OK);
        UNIT_ASSERT(completed.insert(result->Id).second);
    }
    for (const auto& blob : blobs) {
        UNIT_ASSERT(completed.contains(blob.Id));
    }
    UNIT_ASSERT(requests.empty());
}

Y_UNIT_TEST(TestMirror3dcMinLatencyNotReady) {
    for (ui32 failedRealm = 0; failedRealm < 3; ++failedRealm) {
        TestMirror3dcMinLatencyNotReady<false>(failedRealm);
        TestMirror3dcMinLatencyNotReady<false>(failedRealm, false,
            NKikimrProto::NOTREADY, NKikimrProto::NOTREADY, NKikimrProto::OK);
    }
    TestMirror3dcMinLatencyNotReady<false>(0, true);
    TestMirror3dcMinLatencyNotReady<false>(0, false, NKikimrProto::ERROR, NKikimrProto::ERROR);
    TestMirror3dcMinLatencyNotReady<false>(0, false, NKikimrProto::ERROR, NKikimrProto::NOTREADY);
    TestMirror3dcMinLatencyNotReady<false>(0, false, NKikimrProto::NOTREADY, NKikimrProto::ERROR);
    TestMirror3dcMinLatencyNotReady<false>(0, false,
        NKikimrProto::NOTREADY, NKikimrProto::NOTREADY, NKikimrProto::ERROR);
}

Y_UNIT_TEST(TestMirror3dcMultiPutMinLatencyNotReady) {
    for (ui32 failedRealm = 0; failedRealm < 3; ++failedRealm) {
        TestMirror3dcMinLatencyNotReady<true>(failedRealm);
        TestMirror3dcMinLatencyNotReady<true>(failedRealm, false,
            NKikimrProto::NOTREADY, NKikimrProto::NOTREADY, NKikimrProto::OK);
    }
    TestMirror3dcMinLatencyNotReady<true>(0, true);
    TestMirror3dcMinLatencyNotReady<true>(0, false, NKikimrProto::ERROR, NKikimrProto::ERROR);
    TestMirror3dcMinLatencyNotReady<true>(0, false, NKikimrProto::ERROR, NKikimrProto::NOTREADY);
    TestMirror3dcMinLatencyNotReady<true>(0, false, NKikimrProto::NOTREADY, NKikimrProto::ERROR);
    TestMirror3dcMinLatencyNotReady<true>(0, false,
        NKikimrProto::NOTREADY, NKikimrProto::NOTREADY, NKikimrProto::ERROR);
}

Y_UNIT_TEST(TestMirror3dcNotReadyStateIsDistinctFromError) {
    TTestBasicRuntime runtime;
    SetupRuntime(runtime);
    TDSProxyEnv env;
    env.Configure(runtime, TErasureType::ErasureMirror3dc, 1, 0);
    TBlackboard blackboard(env.Info, env.GroupQueues, NKikimrBlobStorage::TabletLog,
        NKikimrBlobStorage::AsyncRead);
    const TLogoBlobID blobId(72075186224047637, 1, 863, 1, 786, 24576);
    blackboard.RegisterBlobForPut(blobId, 0);
    const auto& state = blackboard[blobId];
    const TLogoBlobID replicaId(blobId, 1);

    blackboard.AddErrorResponse(replicaId, state.Disks[0].OrderNumber, "Queue is not ready", NKikimrProto::NOTREADY);
    blackboard.AddErrorResponse(replicaId, state.Disks[3].OrderNumber, "VDisk error");
    const auto& notReady = state.Disks[0].DiskParts[0];
    const auto& error = state.Disks[3].DiskParts[0];
    UNIT_ASSERT(notReady.Situation == TBlobState::ESituation::NotReady);
    UNIT_ASSERT(error.Situation == TBlobState::ESituation::Error);
    UNIT_ASSERT_VALUES_EQUAL(notReady.ErrorReason, "Queue is not ready");
    UNIT_ASSERT_VALUES_EQUAL(error.ErrorReason, "VDisk error");
}

Y_UNIT_TEST(TestMirror3dcDefaultTacticPostponesNotReady) {
    TTestBasicRuntime runtime;
    SetupRuntime(runtime);
    TDSProxyEnv env;
    env.Configure(runtime, TErasureType::ErasureMirror3dc, 1, 0);
    TTestState testState(runtime, env.Info);
    TBlockEvents<TEvBlobStorage::TEvVPut> requests(runtime);
    const TLogoBlobID blobId(72075186224047637, 1, 863, 1, 786, 24576);
    auto event = testState.CreatePutRequest({blobId, AlphaData(blobId.BlobSize())},
        TEvBlobStorage::TEvPut::TacticDefault, NKikimrBlobStorage::TabletLog);
    auto putActor = env.CreatePutRequestActor(event);
    runtime.Register(putActor.release());
    runtime.SimulateSleep(TDuration::MilliSeconds(1));
    UNIT_ASSERT_VALUES_EQUAL(requests.size(), 3);

    auto request = std::move(requests.front());
    requests.pop_front();
    const TVDiskID vdiskId = VDiskIDFromVDiskID(request->Get()->Record.GetVDiskID());
    auto response = testState.CreateEventResultPtr(request, NKikimrProto::NOTREADY, vdiskId);
    runtime.Send(response.Release());
    runtime.SimulateSleep(TDuration::MilliSeconds(1));
    UNIT_ASSERT_VALUES_EQUAL(requests.size(), 2);
    UNIT_ASSERT(runtime.CaptureMailboxEvents(testState.EdgeActor.Hint(), testState.EdgeActor.NodeId()).empty());
}

void TestPutResultWithVDiskResults(TBlobStorageGroupType type, TMap<TVDiskID, NKikimrProto::EReplyStatus> vdiskStatuses, uint expectedVdiskRequests, NKikimrProto::EReplyStatus resultStatus) {
    TTestBasicRuntime runtime(1, false);
    runtime.SetDispatchTimeout(TDuration::Seconds(1));
    runtime.SetLogPriority(NKikimrServices::BS_PROXY_PUT, NLog::PRI_DEBUG);
    SetupRuntime(runtime);
    TDSProxyEnv env;
    env.Configure(runtime, type, 0, 0);
    TTestState testState(runtime, env.Info);

    TLogoBlobID blobId(72075186224047637, 1, 863, 1, 786, 24576);
    TStringBuilder dataBuilder;
    for (size_t i = 0; i < blobId.BlobSize(); ++i) {
        dataBuilder << 'a';
    }
    TBlobTestSet::TBlob blob(blobId, dataBuilder);

    TGroupMock &groupMock = testState.GetGroupMock();
    for (const auto& status : vdiskStatuses) {
        groupMock.SetError(status.first, status.second);
    }


    TEvBlobStorage::TEvPut::ETactic tactic = TEvBlobStorage::TEvPut::TacticDefault;
    NKikimrBlobStorage::EPutHandleClass handleClass = NKikimrBlobStorage::TabletLog;

    TEvBlobStorage::TEvPut::TPtr ev = testState.CreatePutRequest(blob, tactic, handleClass);
    auto putActor = env.CreatePutRequestActor(ev);
    runtime.Register(putActor.release());

    auto reportActor = std::unique_ptr<IActor>(CreateRequestReportingThrottler(1, 60000, 1));
    runtime.Register(reportActor.release());

    for (ui64 idx = 0; idx < expectedVdiskRequests; ++idx) {
        TEvBlobStorage::TEvVPut::TPtr ev = testState.GrabEventPtr<TEvBlobStorage::TEvVPut>();
        TVDiskID vDiskId = VDiskIDFromVDiskID(ev->Get()->Record.GetVDiskID());
        NKikimrProto::EReplyStatus status = groupMock.OnVPut(*ev->Get());
        TEvBlobStorage::TEvVPutResult::TPtr result = testState.CreateEventResultPtr(ev, status, vDiskId);
        runtime.Send(result.Release());
    }

    TMap<TLogoBlobID, NKikimrProto::EReplyStatus> expectedStatus {
        {blobId, resultStatus}
    };
    testState.ReceivePutResults(1, expectedStatus);
}

Y_UNIT_TEST(TestBlock42PutStatusOkWith_0_0_VdiskErrors) {
    TestPutResultWithVDiskResults({TErasureType::Erasure4Plus2Block}, {}, 6, NKikimrProto::OK);
}

Y_UNIT_TEST(TestBlock42PutStatusOkWith_1_0_VdiskErrors) {
    TMap<TVDiskID, NKikimrProto::EReplyStatus> vdiskStatuses {
        {TVDiskID(0, 1, 0, 0, 0), NKikimrProto::ERROR},
    };
    TestPutResultWithVDiskResults({TErasureType::Erasure4Plus2Block}, vdiskStatuses, 7, NKikimrProto::OK);
}

Y_UNIT_TEST(TestBlock42PutStatusOkWith_1_1_VdiskErrors) {
    TMap<TVDiskID, NKikimrProto::EReplyStatus> vdiskStatuses {
        {TVDiskID(0, 1, 0, 0, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 0, 6, 0), NKikimrProto::ERROR},
    };
    TestPutResultWithVDiskResults({TErasureType::Erasure4Plus2Block}, vdiskStatuses, 8, NKikimrProto::OK);
}

Y_UNIT_TEST(TestBlock42PutStatusOkWith_2_0_VdiskErrors) {
    TMap<TVDiskID, NKikimrProto::EReplyStatus> vdiskStatuses {
        {TVDiskID(0, 1, 0, 0, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 0, 1, 0), NKikimrProto::ERROR},
    };
    TestPutResultWithVDiskResults({TErasureType::Erasure4Plus2Block}, vdiskStatuses, 8, NKikimrProto::OK);
}

Y_UNIT_TEST(TestBlock42PutStatusErrorWith_2_1_VdiskErrors) {
    TMap<TVDiskID, NKikimrProto::EReplyStatus> vdiskStatuses {
        {TVDiskID(0, 1, 0, 0, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 0, 1, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 0, 6, 0), NKikimrProto::ERROR},
    };
    TestPutResultWithVDiskResults({TErasureType::Erasure4Plus2Block}, vdiskStatuses, 8, NKikimrProto::ERROR);
}

Y_UNIT_TEST(TestBlock42PutStatusErrorWith_3_0_VdiskErrors) {
    TMap<TVDiskID, NKikimrProto::EReplyStatus> vdiskStatuses {
        {TVDiskID(0, 1, 0, 0, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 0, 1, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 0, 2, 0), NKikimrProto::ERROR},
    };
    TestPutResultWithVDiskResults({TErasureType::Erasure4Plus2Block}, vdiskStatuses, 6, NKikimrProto::ERROR);
}

Y_UNIT_TEST(TestBlock42PutStatusErrorWith_1_2_VdiskErrors) {
    TMap<TVDiskID, NKikimrProto::EReplyStatus> vdiskStatuses {
        {TVDiskID(0, 1, 0, 0, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 0, 6, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 0, 7, 0), NKikimrProto::ERROR},
    };
    TestPutResultWithVDiskResults({TErasureType::Erasure4Plus2Block}, vdiskStatuses, 8, NKikimrProto::ERROR);
}

Y_UNIT_TEST(TestMirror3dcPutStatusOkWith_0_0_0_VdiskErrors) {
    TestPutResultWithVDiskResults({TErasureType::ErasureMirror3dc}, {}, 3, NKikimrProto::OK);
}

Y_UNIT_TEST(TestMirror3dcPutStatusOkWith_1_0_0_VdiskErrors) {
    TMap<TVDiskID, NKikimrProto::EReplyStatus> vdiskStatuses {
        {TVDiskID(0, 1, 0, 1, 0), NKikimrProto::ERROR},
    };
    TestPutResultWithVDiskResults({TErasureType::ErasureMirror3dc}, vdiskStatuses, 4, NKikimrProto::OK);
}

Y_UNIT_TEST(TestMirror3dcPutStatusOkWith_2_0_0_VdiskErrors) {
    TMap<TVDiskID, NKikimrProto::EReplyStatus> vdiskStatuses {
        {TVDiskID(0, 1, 0, 1, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 0, 2, 0), NKikimrProto::ERROR},
    };
    TestPutResultWithVDiskResults({TErasureType::ErasureMirror3dc}, vdiskStatuses, 5, NKikimrProto::OK);
}

Y_UNIT_TEST(TestMirror3dcPutStatusOkWith_3_0_0_VdiskErrors) {
    TMap<TVDiskID, NKikimrProto::EReplyStatus> vdiskStatuses {
        {TVDiskID(0, 1, 0, 1, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 0, 2, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 0, 0, 0), NKikimrProto::ERROR},
    };
    TestPutResultWithVDiskResults({TErasureType::ErasureMirror3dc}, vdiskStatuses, 7, NKikimrProto::OK);
}

Y_UNIT_TEST(TestMirror3dcPutStatusOkWith_1_1_0_VdiskErrors) {
    TMap<TVDiskID, NKikimrProto::EReplyStatus> vdiskStatuses {
        {TVDiskID(0, 1, 0, 1, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 1, 1, 0), NKikimrProto::ERROR},
    };
    TestPutResultWithVDiskResults({TErasureType::ErasureMirror3dc}, vdiskStatuses, 5, NKikimrProto::OK);
}

Y_UNIT_TEST(TestMirror3dcPutStatusOkWith_2_1_0_VdiskErrors) {
    TMap<TVDiskID, NKikimrProto::EReplyStatus> vdiskStatuses {
        {TVDiskID(0, 1, 0, 1, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 0, 2, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 1, 1, 0), NKikimrProto::ERROR},
    };
    TestPutResultWithVDiskResults({TErasureType::ErasureMirror3dc}, vdiskStatuses, 6, NKikimrProto::OK);
}

Y_UNIT_TEST(TestMirror3dcPutStatusErrorWith_1_1_1_VdiskErrors) {
    TMap<TVDiskID, NKikimrProto::EReplyStatus> vdiskStatuses {
        {TVDiskID(0, 1, 0, 1, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 1, 1, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 2, 1, 0), NKikimrProto::ERROR},
    };
    TestPutResultWithVDiskResults({TErasureType::ErasureMirror3dc}, vdiskStatuses, 3, NKikimrProto::ERROR);
}

Y_UNIT_TEST(TestMirror3dcPutStatusErrorWith_2_2_0_VdiskErrors) {
    TMap<TVDiskID, NKikimrProto::EReplyStatus> vdiskStatuses {
        {TVDiskID(0, 1, 0, 1, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 0, 2, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 1, 1, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 1, 2, 0), NKikimrProto::ERROR},
    };
    TestPutResultWithVDiskResults({TErasureType::ErasureMirror3dc}, vdiskStatuses, 5, NKikimrProto::ERROR);
}

Y_UNIT_TEST(TestMirror3dcPutStatusOkWith_3_1_0_VdiskErrors) {
    TMap<TVDiskID, NKikimrProto::EReplyStatus> vdiskStatuses {
        {TVDiskID(0, 1, 0, 0, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 0, 1, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 0, 2, 0), NKikimrProto::ERROR},
        {TVDiskID(0, 1, 1, 1, 0), NKikimrProto::ERROR},
    };
    TestPutResultWithVDiskResults({TErasureType::ErasureMirror3dc}, vdiskStatuses, 8, NKikimrProto::OK);
}

} // Y_UNIT_TEST_SUITE TDSProxyPutTest
} // namespace NDSProxyPutTest
} // namespace NKikimr
