#include "defs.h"
#include "dsproxy_vdisk_mock_ut.h"
#include "dsproxy_env_mock_ut.h"

#include <ydb/core/blobstorage/dsproxy/dsproxy_put_impl.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/actor_helpers.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
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

void TestPutMaxPartCountOnHandoff(TErasureType::EErasureSpecies erasureSpecies) {
    TActorSystemStub actorSystemStub;
    i32 size = 786;
    TLogoBlobID blobId(72075186224047637, 1, 863, 1, size, 24576);
    TString data = AlphaData(size);

    const ui32 groupId = 0;
    TBlobStorageGroupType groupType(erasureSpecies);
    const ui32 domainCount = groupType.BlobSubgroupSize();;

    TGroupMock group(groupId, erasureSpecies, domainCount, 1);
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

    TString encryptedData = data;
    char *dataBytes = encryptedData.Detach();
    Encrypt(dataBytes, dataBytes, 0, encryptedData.size(), blobId, *group.GetInfo());

    TBatchedVec<TStackVec<TRope, TypicalPartsInBlob>> partSetSingleton(1);
    partSetSingleton[0].resize(totalParts);
    ErasureSplit((TErasureType::ECrcMode)blobId.CrcMode(), group.GetInfo()->Type, TRope(encryptedData), partSetSingleton[0]);

    TEvBlobStorage::TEvPut ev(blobId, data, TInstant::Max(), NKikimrBlobStorage::TabletLog,
            TEvBlobStorage::TEvPut::TacticDefault);

    TPutImpl putImpl(group.GetInfo(), groupQueues, &ev, mon, false, TActorId(), 0, NWilson::TTraceId(), TAccelerationParams{});

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

    for (ui32 vPutIdx = 0; vPutIdx < vPuts.size(); ++vPutIdx) {
        ui32 nextVPut = vPutIdx;
        ui32 diskPos = (ui32)-1;
        for (ui32 i = vPutIdx; i < vPuts.size(); ++i) {
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
    TestPutMaxPartCountOnHandoff(TErasureType::Erasure4Plus2Block);
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
        , Group(GroupId, ErasureSpecies, GroupType.BlobSubgroupSize(), 1)
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
            ErasureSplit((TErasureType::ECrcMode)blobId.CrcMode(), Group.GetInfo()->Type, TRope(encryptedData), PartSets[blobIdx]);
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
                        TAccelerationParams{});
            } else {
                putImpl.ConstructInPlace(Group.GetInfo(), GroupQueues, events, Mon,
                        NKikimrBlobStorage::TabletLog, TEvBlobStorage::TEvPut::TacticDefault, false, TAccelerationParams{});
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
    TPutImpl putImpl(env.Info, env.GroupQueues, &ev, env.Mon, true, TActorId(), 0, NWilson::TTraceId(), TAccelerationParams{});

    TLogContext logCtx(NKikimrServices::BS_PROXY_PUT, false);
    logCtx.LogAcc.IsLogEnabled = false;

    const ui32 totalParts = env.Info->Type.TotalPartCount();
    TBatchedVec<TStackVec<TRope, TypicalPartsInBlob>> partSetSingleton(1);
    partSetSingleton[0].resize(totalParts);

    TString encryptedData = data;
    char *dataBytes = encryptedData.Detach();
    Encrypt(dataBytes, dataBytes, 0, encryptedData.size(), blobId, *env.Info);
    ErasureSplit((TErasureType::ECrcMode)blobId.CrcMode(), env.Info->Type, TRope(encryptedData), partSetSingleton[0]);
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

} // Y_UNIT_TEST_SUITE TDSProxyPutTest
} // namespace NDSProxyPutTest
} // namespace NKikimr
