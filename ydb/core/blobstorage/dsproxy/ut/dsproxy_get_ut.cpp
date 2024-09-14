#include "defs.h"
#include "dsproxy_vdisk_mock_ut.h"

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_get_impl.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_put_impl.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/random/shuffle.h>
#include <util/stream/str.h>

namespace NKikimr {
namespace NDSProxyGetTest {

constexpr bool IsVerbose = false;

#define VERBOSE(str)            \
do {                            \
    if (IsVerbose) {            \
    Cout << str << Endl;        \
    }                           \
} while (false)                 \


Y_UNIT_TEST_SUITE(TDSProxyGetTest) {

void TestIntervalsAndCrcAllOk(TErasureType::EErasureSpecies erasureSpecies, bool isVerboseNoDataEnabled, bool checkCrc) {
    TActorSystemStub actorSystemStub;

    TBlobStorageGroupType groupType(erasureSpecies);

    const ui32 groupId = 0;
    const ui32 domainCount = groupType.BlobSubgroupSize();

    TGroupMock group(groupId, erasureSpecies, domainCount, 1);
    TIntrusivePtr<TGroupQueues> groupQueues = group.MakeGroupQueues();

    const ui64 maxQueryCount = 32;

    TBlobTestSet blobSet;
    if (checkCrc) {
        blobSet.GenerateSet(2, maxQueryCount);
    } else {
        blobSet.GenerateSet(0, maxQueryCount);
    }
    group.PutBlobSet(blobSet);

    for (ui64 queryCount = 1; queryCount <= maxQueryCount; ++queryCount) {
        CTEST << "queryCount# " << queryCount << Endl;
        for (ui64 blobCount = 1; blobCount <= maxQueryCount; ++blobCount) {
            TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesA(new TEvBlobStorage::TEvGet::TQuery[maxQueryCount]);
            TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesB(new TEvBlobStorage::TEvGet::TQuery[maxQueryCount]);
            for (ui64 queryIdx = 0; queryIdx < queryCount; ++queryIdx) {
                TEvBlobStorage::TEvGet::TQuery &q = queriesA[queryIdx];
                q.Id = blobSet.Get(queryIdx % blobCount).Id;
                if (checkCrc) {
                    q.Shift = (queryIdx % groupType.DataParts()) * groupType.PartUserSize(q.Id.BlobSize());
                    q.Shift = q.Shift <= q.Id.BlobSize() ? q.Shift : 0;
                    q.Size = Max((ui64)16, (ui64)(queryIdx * 177) % (q.Id.BlobSize() - q.Shift));
                } else {
                    q.Shift = (queryIdx * 177) % q.Id.BlobSize();
                    q.Size = Min((ui64)70, (ui64)q.Id.BlobSize() - (ui64)q.Shift);
                }
                queriesB[queryIdx] = q;
                CTEST << "query# " << queryIdx << " shift# " << q.Shift << " size# " << q.Size << Endl;
            }
            TEvBlobStorage::TEvGet ev(queriesA, queryCount, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead, false, false);
            ev.IsVerboseNoDataEnabled = isVerboseNoDataEnabled;
            TGetImpl getImpl(group.GetInfo(), groupQueues, &ev, nullptr, TAccelerationParams{});
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
            TLogContext logCtx(NKikimrServices::BS_PROXY_GET, false);
            logCtx.LogAcc.IsLogEnabled = false;
            getImpl.GenerateInitialRequests(logCtx, vGets);

            if (IsVerbose) {
                for (ui32 i = 0; i < vGets.size(); ++i) {
                    CTEST << "initial i# " << i << " vget# " << vGets[i]->ToString() << Endl;
                }
            }
            TAutoPtr<TEvBlobStorage::TEvGetResult> getResult;
            for (ui64 vGetIdx = 0; vGetIdx < vGets.size(); ++vGetIdx) {
                bool isLast = (vGetIdx == vGets.size() - 1);
                //ui64 messageCookie = request->Record.GetCookie();
                TEvBlobStorage::TEvVGetResult vGetResult;
                group.OnVGet(*vGets[vGetIdx], vGetResult);

                // TODO: generate result
                TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> nextVGets;
                TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> nextVPuts;
                getImpl.OnVGetResult(logCtx, vGetResult, nextVGets, nextVPuts, getResult);
                if (IsVerbose) {
                    for (ui32 i = 0; i < nextVGets.size(); ++i) {
                        CTEST << "next i# " << i << " vget# " << nextVGets[i]->ToString() << Endl;
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL(nextVGets.size(), 0);
                UNIT_ASSERT_VALUES_EQUAL(nextVPuts.size(), 0);
                if (getResult) {
                    // TODO: remove this
                    break;
                }
                if (!isLast) {
                    UNIT_ASSERT_VALUES_EQUAL(getResult, nullptr);
                }
            }
            UNIT_ASSERT(getResult);
            UNIT_ASSERT_VALUES_EQUAL(getResult->ResponseSz, queryCount);
            // TODO: check the data vs queriesB
            for (ui64 queryIdx = 0; queryIdx < queryCount; ++queryIdx) {
                TEvBlobStorage::TEvGetResult::TResponse &a = getResult->Responses[queryIdx];
                TEvBlobStorage::TEvGet::TQuery &q = queriesB[queryIdx];
                UNIT_ASSERT_VALUES_EQUAL(q.Id, a.Id);
                UNIT_ASSERT_VALUES_EQUAL(a.Status, NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(q.Shift, a.Shift);
                UNIT_ASSERT_VALUES_EQUAL(q.Size, a.RequestedSize);
                blobSet.Check(queryIdx % blobCount, q.Id, q.Shift, q.Size, a.Buffer.ConvertToString());
            }
        }
    }
    return;
}

// Without CRC
Y_UNIT_TEST(TestBlock42GetIntervalsAllOk) {
    TestIntervalsAndCrcAllOk(TErasureType::Erasure4Plus2Block, false, false);
}

//Y_UNIT_TEST(TestBlock42GetIntervalsAllOkVerbose) {
//    TestIntervalsAndCrcAllOk(TErasureType::Erasure4Plus2Block, true, false);
//}

Y_UNIT_TEST(TestMirror32GetIntervalsAllOk) {
    TestIntervalsAndCrcAllOk(TErasureType::ErasureMirror3Plus2, false, false);
}

// With CRC
Y_UNIT_TEST(TestBlock42GetBlobCrcCheck) {
    TestIntervalsAndCrcAllOk(TErasureType::Erasure4Plus2Block, false, true);
}

//Y_UNIT_TEST(TestBlock42GetBlobCrcCheckVerbose) {
//    TestIntervalsAndCrcAllOk(TErasureType::Erasure4Plus2Block, true, true);
//}

Y_UNIT_TEST(TestMirror32GetBlobCrcCheck) {
    TestIntervalsAndCrcAllOk(TErasureType::ErasureMirror3Plus2, false, true);
}

class TTestWipedAllOkStep {
public:
    struct TVPutInfo {
        TVDiskID VDiskId;
        TLogoBlobID BlobId;
        ui64 BlobIdx;

        bool operator<(const TVPutInfo &other) const {
            return std::tie(VDiskId, BlobId, BlobIdx) < std::tie(other.VDiskId, other.BlobId, other.BlobIdx);
        }

        bool operator==(const TVPutInfo &other) const {
            return std::tie(VDiskId, BlobId, BlobIdx) == std::tie(other.VDiskId, other.BlobId, other.BlobIdx);
        }
    };

    ui32 GroupId;
    TErasureType::EErasureSpecies ErasureSpecies;
    ui32 DomainCount;
    ui32 GenerateBlobsMode = 0;
    const TVector<ui64> &QueryCounts;
    ui64 MaxQueryCount;

    const TVector<TBlobTestSet::TBlob> *Blobs = nullptr;

    bool IsVerboseNoDataEnabled;
    bool IsRestore;

    TVector<TVPutInfo> SendVPuts;

private:
    TMaybe<TGroupMock> Group;
    TIntrusivePtr<TGroupQueues> GroupQueues;
    TMaybe<TBlobTestSet> BlobSet;

    ui64 VPutRequests = 0;
    ui64 VPutResponses = 0;
    ui64 RequestIndex = 0;
    ui64 ResponseIndex = 0;

public:
    TTestWipedAllOkStep(ui32 groupId, TErasureType::EErasureSpecies erasureSpecies, ui32 domainCount,
            const TVector<ui64> &queryCounts, bool isVerboseNoDataEnabled, bool isRestore)
        : GroupId(groupId)
        , ErasureSpecies(erasureSpecies)
        , DomainCount(domainCount)
        , QueryCounts(queryCounts)
        , MaxQueryCount(*MaxElement(QueryCounts.begin(), QueryCounts.end()))
        , IsVerboseNoDataEnabled(isVerboseNoDataEnabled)
        , IsRestore(isRestore)
    {
    }

    void SetGenerateBlobsMode(ui64 generateBlobsMode) {
        GenerateBlobsMode = generateBlobsMode;
    }

    void SetBlobs(const TVector<TBlobTestSet::TBlob> &blobs) {
        Blobs = &blobs;
    }

    void AddWipedVDisk(ui64 idx, ui64 error) {
        switch (error) {
        case 0:
            Group->Wipe(idx);
            break;
        case 1:
            Group->SetError(idx, NKikimrProto::ERROR);
            break;
        case 2:
            Group->SetError(idx, NKikimrProto::CORRUPTED);
            break;
        case 3:
            Group->SetError(idx, NKikimrProto::VDISK_ERROR_STATE);
            break;
        case 4:
            Group->SetError(idx, NKikimrProto::NOT_YET);
            break;
        }
    }

    void Run() {
        for (ui64 qci = 0; qci < QueryCounts.size(); ++qci) {
            ui64 queryCount = QueryCounts[qci];
            for (ui64 bci = 0; bci < QueryCounts.size(); ++bci) {
                ui64 blobCount = QueryCounts[bci];
                SubStep(queryCount, blobCount);
            }
        }
    }

    void Init() {
        SendVPuts.clear();
        Group.Clear();
        Group.ConstructInPlace(GroupId, ErasureSpecies, DomainCount, 1);
        GroupQueues = Group->MakeGroupQueues();
        BlobSet.Clear();
        BlobSet.ConstructInPlace();
        if (Blobs) {
            BlobSet->AddBlobs(*Blobs);
        } else {
            BlobSet->GenerateSet(GenerateBlobsMode, MaxQueryCount);
        }
        Group->PutBlobSet(*BlobSet);
    }

private:
    void ClearCounters() {
        VPutRequests = 0;
        VPutResponses = 0;
        RequestIndex = 0;
        ResponseIndex = 0;
    }

    void AssertCounters(TGetImpl &getImpl) {
        UNIT_ASSERT_C(VPutResponses == getImpl.GetVPutResponses()
                && VPutRequests == getImpl.GetVPutRequests()
                && RequestIndex == getImpl.GetRequestIndex()
                && ResponseIndex == getImpl.GetResponseIndex(),
                "Not equal expected VPutRequest and VPutResponse with given"
                << " VPutRequests# " << VPutRequests
                << " VPutResponses# " << VPutResponses
                << " getImpl.VPutRequests# " << getImpl.GetVPutRequests()
                << " getImpl.VPutResponses# " << getImpl.GetVPutResponses()
                << " RequestIndex# " << RequestIndex
                << " ResponseIndex# " << ResponseIndex
                << " getImpl.RequestIndex# " << getImpl.GetRequestIndex()
                << " getImpl.ResponseIndex# " << getImpl.GetResponseIndex());
    }

    void ProcessVPuts(TLogContext &logCtx, TGetImpl &getImpl,
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &vGets, TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> &vPuts,
            TAutoPtr<TEvBlobStorage::TEvGetResult> &getResult) {
        for (ui64 vPutIdx = 0; vPutIdx < vPuts.size(); ++vPutIdx) {
            auto &putRequest = vPuts[vPutIdx]->Record;
            auto vdisk = VDiskIDFromVDiskID(putRequest.GetVDiskID());
            auto blobId = LogoBlobIDFromLogoBlobID(putRequest.GetBlobID());
            SendVPuts.push_back({vdisk, blobId, 0});
            TEvBlobStorage::TEvVPutResult vPutResult;
            vPutResult.MakeError(NKikimrProto::OK, TString(), putRequest);

            TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> nextVGets;
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> nextVPuts;
            getImpl.OnVPutResult(logCtx, vPutResult, nextVGets, nextVPuts, getResult);
            VPutResponses++;
            RequestIndex += nextVGets.size();
            VPutRequests += nextVPuts.size();
            AssertCounters(getImpl);

            std::move(nextVGets.begin(), nextVGets.end(), std::back_inserter(vGets));
            std::move(nextVPuts.begin(), nextVPuts.end(), std::back_inserter(vPuts));
            if (getResult) {
                break;
            }
        }
        vPuts.clear();
    }

    void SubStep(ui64 queryCount, ui64 blobCount) {
        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesA(
                new TEvBlobStorage::TEvGet::TQuery[MaxQueryCount]);
        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesB(
                new TEvBlobStorage::TEvGet::TQuery[MaxQueryCount]);
        for (ui64 queryIdx = 0; queryIdx < queryCount; ++queryIdx) {
            TEvBlobStorage::TEvGet::TQuery &q = queriesA[queryIdx];
            q.Id = BlobSet->Get(queryIdx % blobCount).Id;
            q.Shift = (queryIdx * 177) % q.Id.BlobSize();
            q.Size = Min((ui64)70, (ui64)q.Id.BlobSize() - (ui64)q.Shift);
            queriesB[queryIdx] = q;
        }
        TEvBlobStorage::TEvGet ev(queriesA, queryCount, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead, IsRestore, false);
        ev.IsVerboseNoDataEnabled = IsVerboseNoDataEnabled;
        TGetImpl getImpl(Group->GetInfo(), GroupQueues, &ev, nullptr, TAccelerationParams{});
        ClearCounters();
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
        TLogContext logCtx(NKikimrServices::BS_PROXY_GET, false);
        logCtx.LogAcc.IsLogEnabled = false;
        getImpl.GenerateInitialRequests(logCtx, vGets);
        RequestIndex += vGets.size();
        AssertCounters(getImpl);

        TAutoPtr<TEvBlobStorage::TEvGetResult> getResult;
        for (ui64 vGetIdx = 0; vGetIdx < vGets.size(); ++vGetIdx) {

            bool isLast = (vGetIdx == vGets.size() - 1);
            //ui64 messageCookie = request->Record.GetCookie();
            TEvBlobStorage::TEvVGetResult vGetResult;
            Group->OnVGet(*vGets[vGetIdx], vGetResult);

            // TODO: generate result
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> nextVGets;
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> nextVPuts;
            getImpl.OnVGetResult(logCtx, vGetResult, nextVGets, nextVPuts, getResult);
            ResponseIndex++;
            RequestIndex += nextVGets.size();
            VPutRequests += nextVPuts.size();
            AssertCounters(getImpl);

            std::move(nextVGets.begin(), nextVGets.end(), std::back_inserter(vGets));
            if (IsRestore) {
                std::move(nextVPuts.begin(), nextVPuts.end(), std::back_inserter(vPuts));
            } else {
                UNIT_ASSERT_VALUES_EQUAL(nextVPuts.size(), 0);
            }
            if (getResult) {
                break;
            }
            ProcessVPuts(logCtx, getImpl, vGets, vPuts, getResult);
            if (getResult) {
                break;
            }
            if (!isLast) {
                UNIT_ASSERT_VALUES_EQUAL(getResult, nullptr);
            }
        }
        UNIT_ASSERT(getResult);
        UNIT_ASSERT_VALUES_EQUAL(getResult->ResponseSz, queryCount);
        // TODO: check the data vs queriesB
        for (ui64 queryIdx = 0; queryIdx < queryCount; ++queryIdx) {
            TEvBlobStorage::TEvGetResult::TResponse &a = getResult->Responses[queryIdx];
            TEvBlobStorage::TEvGet::TQuery &q = queriesB[queryIdx];
            UNIT_ASSERT_VALUES_EQUAL(q.Id, a.Id);
            UNIT_ASSERT_VALUES_EQUAL(a.Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(q.Shift, a.Shift);
            UNIT_ASSERT_VALUES_EQUAL(q.Size, a.RequestedSize);
            BlobSet->Check(queryIdx % blobCount, q.Id, q.Shift, q.Size, a.Buffer.ConvertToString());
        }
    }
};

void TestIntervalsWipedAllOk(TErasureType::EErasureSpecies erasureSpecies, bool isVerboseNoDataEnabled) {
    TActorSystemStub actorSystemStub;

    const ui32 groupId = 0;
    TBlobStorageGroupType groupType(erasureSpecies);
    const ui32 domainCount = groupType.BlobSubgroupSize();

    TVector<ui64> queryCounts = {1, 2, 3, 13, 34, 55};

    for (bool isRestore : {false, true}) {
        for (ui32 generateMode = 0; generateMode < 2; ++generateMode) {
            for (ui64 wiped1 = 0; wiped1 < domainCount; ++wiped1) {
                for (ui64 wiped2 = 0; wiped2 <= wiped1; ++wiped2) {
                    ui64 maxErrorMask = (wiped1 == wiped2 ? 4 : 24);
                    for (ui64 errorMask = 0; errorMask <= maxErrorMask; ++errorMask) {
                        ui64 error1 = errorMask % 5;
                        ui64 error2 = errorMask / 5;
                        TTestWipedAllOkStep testStep(
                                groupId, erasureSpecies, domainCount, queryCounts,
                                isVerboseNoDataEnabled, isRestore);
                        testStep.SetGenerateBlobsMode(generateMode);
                        testStep.Init();
                        testStep.AddWipedVDisk(wiped1, error1);
                        testStep.AddWipedVDisk(wiped2, error2);
                        testStep.Run();
                    }
                }
            }
        }
    }
}

class TGetSimulator {
    TGroupMock Group;
    TIntrusivePtr<TGroupQueues> GroupQueues;
public:
    TBlobTestSet BlobSet;

    TGetSimulator(ui32 groupId, TErasureType::EErasureSpecies erasureSpecies, ui32 failDomains,
            ui32 drivesPerFailDomain)
        : Group(groupId, erasureSpecies, failDomains, drivesPerFailDomain)
        , GroupQueues(Group.MakeGroupQueues())
    {}

    void GenerateBlobSet(ui32 setIdx, ui32 count, ui32 forceSize = 0) {
        BlobSet.GenerateSet(setIdx, count, forceSize);
        Group.PutBlobSet(BlobSet);
    }

    void SetBlobSet(const TVector<TBlobTestSet::TBlob>& blobs) {
        BlobSet.AddBlobs(blobs);
        Group.PutBlobSet(BlobSet);
    }

    void SetError(ui32 domainIdx, NKikimrProto::EReplyStatus status) {
        Group.SetError(domainIdx, status);
    }

    void SetCorrupted(ui32 domainIdx) {
        Group.SetCorrupted(domainIdx);
    }

    void SetPredictedDelayNs(ui32 domainIdx, ui64 predictedDelayNs) {
        Group.SetPredictedDelayNs(domainIdx, predictedDelayNs);
    }

    TIntrusivePtr<TGroupQueues> GetSessionsState() const {
        return GroupQueues;
    }

    TAutoPtr<TEvBlobStorage::TEvGetResult> Simulate(TEvBlobStorage::TEvGet *ev) {
        TAutoPtr<TEvBlobStorage::TEvGetResult> getResult;

        TGetImpl getImpl(Group.GetInfo(), GroupQueues, ev, nullptr, TAccelerationParams{});
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
        TLogContext logCtx(NKikimrServices::BS_PROXY_GET, false);
        logCtx.LogAcc.IsLogEnabled = false;
        getImpl.GenerateInitialRequests(logCtx, vGets);
        
        for (ui64 vGetIdx = 0; vGetIdx < vGets.size(); ++vGetIdx) {

            bool isLast = (vGetIdx == vGets.size() - 1);
            TEvBlobStorage::TEvVGetResult vGetResult;
            Group.OnVGet(*vGets[vGetIdx], vGetResult);

            // TODO: generate result
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> nextVGets;
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> nextVPuts;
            getImpl.OnVGetResult(logCtx, vGetResult, nextVGets, nextVPuts, getResult);
            std::move(nextVGets.begin(), nextVGets.end(), std::back_inserter(vGets));
            if (ev->MustRestoreFirst) {
                std::move(nextVPuts.begin(), nextVPuts.end(), std::back_inserter(vPuts));
            } else {
                UNIT_ASSERT_VALUES_EQUAL(nextVPuts.size(), 0);
            }
            if (getResult) {
                break;
            }
            for (ui64 vPutIdx = 0; vPutIdx < vPuts.size(); ++vPutIdx) {
                auto &putRequest = vPuts[vPutIdx]->Record;
                TEvBlobStorage::TEvVPutResult vPutResult;
                vPutResult.MakeError(NKikimrProto::OK, TString(), putRequest);

                TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> nextVGets;
                TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> nextVPuts;
                getImpl.OnVPutResult(logCtx, vPutResult, nextVGets, nextVPuts, getResult);
                std::move(nextVGets.begin(), nextVGets.end(), std::back_inserter(vGets));
                std::move(nextVPuts.begin(), nextVPuts.end(), std::back_inserter(vPuts));
                if (getResult) {
                    break;
                }
            }
            vPuts.clear();
            if (getResult) {
                break;
            }
            if (!isLast) {
                UNIT_ASSERT_VALUES_EQUAL(getResult, nullptr);
            }
        }

        return getResult;
    }

};

Y_UNIT_TEST(TestBlock42VGetCountWithErasure) {
    bool isVerboseNoDataEnabled = false;
    TErasureType::EErasureSpecies erasureSpecies = TErasureType::Erasure4Plus2Block;
    TActorSystemStub actorSystemStub;

    TVector<TLogoBlobID> blobIDs = {
        TLogoBlobID(72075186224047637, 1, 863, 1, 786, 24576),
    };

    TVector<TBlobTestSet::TBlob> blobs;
    for (const auto& id : blobIDs) {
        TStringBuilder builder;
        for (size_t i = 0; i < id.BlobSize(); ++i) {
            builder << 'a';
        }
        blobs.emplace_back(id, builder);
    }

    const ui32 groupId = 0;
    TBlobStorageGroupType groupType(erasureSpecies);
    const ui32 domainCount = groupType.BlobSubgroupSize();

    const ui64 queryCount = 1;

    const ui32 isRestore = 0;

    TGroupMock group(groupId, erasureSpecies, domainCount, 1);
    TIntrusivePtr<TGroupQueues> groupQueues = group.MakeGroupQueues();
    TBlobTestSet blobSet;
    blobSet.AddBlobs(blobs);
    group.PutBlobSet(blobSet);

    for (ui32 idx = 0; idx < domainCount; ++idx) {
        group.SetPredictedDelayNs(idx, 1);
    }
    group.SetPredictedDelayNs(7, 10);

    group.SetNotYetBlob(0, TLogoBlobID(blobIDs[0], 1));

    ui64 blobCount = queryCount;
    TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesA(
            new TEvBlobStorage::TEvGet::TQuery[queryCount]);
    TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesB(
            new TEvBlobStorage::TEvGet::TQuery[queryCount]);
    for (ui64 queryIdx = 0; queryIdx < queryCount; ++queryIdx) {
        TEvBlobStorage::TEvGet::TQuery &q = queriesA[queryIdx];
        q.Id = blobSet.Get(queryIdx % blobCount).Id;
        q.Shift = 0;
        q.Size = (ui64)q.Id.BlobSize();
        queriesB[queryIdx] = q;
    }
    TEvBlobStorage::TEvGet ev(queriesA, queryCount, TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::FastRead, isRestore, false);
    ev.IsVerboseNoDataEnabled = isVerboseNoDataEnabled;

    TAutoPtr<TEvBlobStorage::TEvGetResult> getResult;

    TGetImpl getImpl(group.GetInfo(), groupQueues, &ev, nullptr, TAccelerationParams{});
    TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
    TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
    TLogContext logCtx(NKikimrServices::BS_PROXY_GET, false);
    logCtx.LogAcc.IsLogEnabled = false;
    getImpl.GenerateInitialRequests(logCtx, vGets);

    for (ui64 vGetIdx = 0; vGetIdx < vGets.size(); ++vGetIdx) {
        if (vGetIdx == 3) {
            vGets.push_back(std::move(vGets[3]));
            continue;
        }
        bool isLast = (vGetIdx == vGets.size() - 1);

        TEvBlobStorage::TEvVGetResult vGetResult;
        group.OnVGet(*vGets[vGetIdx], vGetResult);

        // TODO: generate result
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> nextVGets;
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> nextVPuts;

        getImpl.OnVGetResult(logCtx, vGetResult, nextVGets, nextVPuts, getResult);

        std::move(nextVGets.begin(), nextVGets.end(), std::back_inserter(vGets));
        if (ev.MustRestoreFirst) {
            std::move(nextVPuts.begin(), nextVPuts.end(), std::back_inserter(vPuts));
        } else {
            UNIT_ASSERT_VALUES_EQUAL(nextVPuts.size(), 0);
        }
        if (getResult) {
            break;
        }
        for (ui64 vPutIdx = 0; vPutIdx < vPuts.size(); ++vPutIdx) {
            auto &putRequest = vPuts[vPutIdx]->Record;
            TEvBlobStorage::TEvVPutResult vPutResult;
            vPutResult.MakeError(NKikimrProto::OK, TString(), putRequest);

            TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> nextVGets;
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> nextVPuts;
            getImpl.OnVPutResult(logCtx, vPutResult, nextVGets, nextVPuts, getResult);
            std::move(nextVGets.begin(), nextVGets.end(), std::back_inserter(vGets));
            std::move(nextVPuts.begin(), nextVPuts.end(), std::back_inserter(vPuts));
            if (getResult) {
                break;
            }
        }
        vPuts.clear();
        if (getResult) {
            break;
        }
        if (!isLast) {
            UNIT_ASSERT_VALUES_EQUAL(getResult, nullptr);
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(vGets.size(), 8);

    UNIT_ASSERT(getResult);
    UNIT_ASSERT_VALUES_EQUAL(getResult->ResponseSz, queryCount);
    // TODO: check the data vs queriesB
    for (ui64 queryIdx = 0; queryIdx < queryCount; ++queryIdx) {
        TEvBlobStorage::TEvGetResult::TResponse &a = getResult->Responses[queryIdx];
        TEvBlobStorage::TEvGet::TQuery &q = queriesB[queryIdx];
        UNIT_ASSERT_VALUES_EQUAL(q.Id, a.Id);
        if (a.Status != NKikimrProto::ERROR) {
            if (a.Status == NKikimrProto::OK) {
                UNIT_ASSERT_VALUES_EQUAL(q.Shift, a.Shift);
                UNIT_ASSERT_VALUES_EQUAL(q.Size, a.RequestedSize);
                blobSet.Check(queryIdx % blobCount, q.Id, q.Shift, q.Size, a.Buffer.ConvertToString());
            } else {
                TStringStream str;
                str << " isRestore# " << isRestore
                    << " status# " << a.Status;
                UNIT_ASSERT_C(false, str.Str());
            }
        }
    } // for queryIdx

    return;
}

Y_UNIT_TEST(TestBlock42WipedOneDiskAndErrorDurringGet) {
    bool isVerboseNoDataEnabled = false;
    TErasureType::EErasureSpecies erasureSpecies = TErasureType::Erasure4Plus2Block;
    TActorSystemStub actorSystemStub;

    TVector<TLogoBlobID> blobIDs = {
        TLogoBlobID(72075186224047637, 1, 863, 1, 786, 24576),
        // TLogoBlobID(72075186224047637, 1, 2194, 1, 142, 12288)
    };

    TVector<TBlobTestSet::TBlob> blobs;
    for (const auto& id : blobIDs) {
        TStringBuilder builder;
        for (size_t i = 0; i < id.BlobSize(); ++i) {
            builder << 'a';
        }

        blobs.emplace_back(id, builder);
    }

    const ui32 groupId = 0;
    TBlobStorageGroupType groupType(erasureSpecies);
    const ui32 domainCount = groupType.BlobSubgroupSize();

    const ui64 queryCount = 1;

    const ui32 isRestore = 0;

    TGroupMock group(groupId, erasureSpecies, domainCount, 1);
    TIntrusivePtr<TGroupQueues> groupQueues = group.MakeGroupQueues();
    TBlobTestSet blobSet;
    blobSet.AddBlobs(blobs);
    group.PutBlobSet(blobSet);

    for (ui32 idx = 0; idx < domainCount; ++idx) {
        group.SetPredictedDelayNs(idx, 1);
    }
    group.SetPredictedDelayNs(7, 10);

    group.SetNotYetBlob(0, TLogoBlobID(blobIDs[0], 1));

    ui64 blobCount = queryCount;
    TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesA(
            new TEvBlobStorage::TEvGet::TQuery[queryCount]);
    TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesB(
            new TEvBlobStorage::TEvGet::TQuery[queryCount]);
    for (ui64 queryIdx = 0; queryIdx < queryCount; ++queryIdx) {
        TEvBlobStorage::TEvGet::TQuery &q = queriesA[queryIdx];
        q.Id = blobSet.Get(queryIdx % blobCount).Id;
        q.Shift = 0;
        q.Size = (ui64)q.Id.BlobSize();
        queriesB[queryIdx] = q;
    }
    TEvBlobStorage::TEvGet ev(queriesA, queryCount, TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::FastRead, isRestore, false);
    ev.IsVerboseNoDataEnabled = isVerboseNoDataEnabled;

    TAutoPtr<TEvBlobStorage::TEvGetResult> getResult;

    TGetImpl getImpl(group.GetInfo(), groupQueues, &ev, nullptr, TAccelerationParams{});
    TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
    TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
    TLogContext logCtx(NKikimrServices::BS_PROXY_GET, false);
    logCtx.LogAcc.IsLogEnabled = false;
    getImpl.GenerateInitialRequests(logCtx, vGets);

    for (ui64 vGetIdx = 0; vGetIdx < vGets.size(); ++vGetIdx) {
        if (vGetIdx == 3) {
            vGets.push_back(std::move(vGets[3]));
            continue;
        }
        if (vGetIdx == 5) {
            group.SetError(2, NKikimrProto::ERROR);
            group.SetPredictedDelayNs(7, 1);
        }
        bool isLast = (vGetIdx == vGets.size() - 1);

        TEvBlobStorage::TEvVGetResult vGetResult;
        group.OnVGet(*vGets[vGetIdx], vGetResult);

        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> nextVGets;
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> nextVPuts;

        getImpl.OnVGetResult(logCtx, vGetResult, nextVGets, nextVPuts, getResult);

        std::move(nextVGets.begin(), nextVGets.end(), std::back_inserter(vGets));
        if (ev.MustRestoreFirst) {
            std::move(nextVPuts.begin(), nextVPuts.end(), std::back_inserter(vPuts));
        } else {
            UNIT_ASSERT_VALUES_EQUAL(nextVPuts.size(), 0);
        }
        if (getResult) {
            break;
        }
        for (ui64 vPutIdx = 0; vPutIdx < vPuts.size(); ++vPutIdx) {
            auto &putRequest = vPuts[vPutIdx]->Record;
            TEvBlobStorage::TEvVPutResult vPutResult;
            vPutResult.MakeError(NKikimrProto::OK, TString(), putRequest);

            TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> nextVGets;
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> nextVPuts;
            getImpl.OnVPutResult(logCtx, vPutResult, nextVGets, nextVPuts, getResult);
            std::move(nextVGets.begin(), nextVGets.end(), std::back_inserter(vGets));
            std::move(nextVPuts.begin(), nextVPuts.end(), std::back_inserter(vPuts));
            if (getResult) {
                break;
            }
        }
        vPuts.clear();
        if (getResult) {
            break;
        }
        if (!isLast) {
            UNIT_ASSERT_VALUES_EQUAL(getResult, nullptr);
        }
    }

    UNIT_ASSERT(getResult);
    UNIT_ASSERT_VALUES_EQUAL(getResult->ResponseSz, queryCount);
    // TODO: check the data vs queriesB
    for (ui64 queryIdx = 0; queryIdx < queryCount; ++queryIdx) {
        TEvBlobStorage::TEvGetResult::TResponse &a = getResult->Responses[queryIdx];
        TEvBlobStorage::TEvGet::TQuery &q = queriesB[queryIdx];
        UNIT_ASSERT_VALUES_EQUAL(q.Id, a.Id);
        if (a.Status != NKikimrProto::ERROR) {
            if (a.Status == NKikimrProto::OK) {
                UNIT_ASSERT_VALUES_EQUAL(q.Shift, a.Shift);
                UNIT_ASSERT_VALUES_EQUAL(q.Size, a.RequestedSize);
                blobSet.Check(queryIdx % blobCount, q.Id, q.Shift, q.Size, a.Buffer.ConvertToString());
            } else {
                TStringStream str;
                str << " isRestore# " << isRestore
                    << " status# " << a.Status;
                UNIT_ASSERT_C(false, str.Str());
            }
        }
    } // for queryIdx

    return;
}

void ApplyError(TGetSimulator &simulator, ui64 idx, ui64 error) {
    switch (error) {
        case 0:
            simulator.SetError(idx, NKikimrProto::ERROR);
        break;
        case 1:
            simulator.SetError(idx, NKikimrProto::CORRUPTED);
        break;
        case 2:
            simulator.SetError(idx, NKikimrProto::VDISK_ERROR_STATE);
        break;
        case 3:
            simulator.SetError(idx, NKikimrProto::NOT_YET);
        break;
    }
}

void TestIntervalsWipedError(TErasureType::EErasureSpecies erasureSpecies, bool isVerboseNoDataEnabled = false) {
    TActorSystemStub actorSystemStub;

    const ui32 groupId = 0;
    TBlobStorageGroupType groupType(erasureSpecies);
    const ui32 domainCount = groupType.BlobSubgroupSize();

    const ui64 maxQueryCount = 13;
    const ui64 queryCounts[] = {1, 2, 3, 13};

    for (ui32 isRestore = 0; isRestore < 2; ++isRestore) {
        for (ui32 setIdx = 0; setIdx < 2; ++setIdx) {
            for (ui64 wiped1 = 0; wiped1 < domainCount; ++wiped1) {
                for (ui64 wiped2 = 0; wiped2 < wiped1; ++wiped2) {
                    for (ui64 error3 = 0; error3 < domainCount; ++error3) {
                        if (wiped1 == error3 || wiped2 == error3) {
                            continue;
                        }
                        for (ui64 error4 = 0; error4 <= error3; ++error4) {
                            if (wiped1 == error4 || wiped2 == error4) {
                                continue;
                            }
                            ui64 maxErrorMask = 15;
                            for (ui64 errorMask = 0; errorMask <= maxErrorMask; ++errorMask) {
                                TGetSimulator simulator(groupId, erasureSpecies, domainCount, 1);
                                simulator.GenerateBlobSet(setIdx, maxQueryCount);

                                ui64 error1 = errorMask % 4;
                                ApplyError(simulator, wiped1, error1);

                                ui64 error2 = errorMask / 4;
                                ApplyError(simulator, wiped2, error2);

                                simulator.SetError(error3, NKikimrProto::ERROR);
                                simulator.SetError(error4, NKikimrProto::ERROR);

                                for (ui64 qci = 0; qci < sizeof(queryCounts) / sizeof(queryCounts[0]); ++qci) {
                                    ui64 queryCount = queryCounts[qci];
                                    for (ui64 bci = 0; bci <= qci; ++bci) {
                                        ui64 blobCount = queryCounts[bci];
                                        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesA(
                                                new TEvBlobStorage::TEvGet::TQuery[maxQueryCount]);
                                        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesB(
                                                new TEvBlobStorage::TEvGet::TQuery[maxQueryCount]);
                                        for (ui64 queryIdx = 0; queryIdx < queryCount; ++queryIdx) {
                                            TEvBlobStorage::TEvGet::TQuery &q = queriesA[queryIdx];
                                            q.Id = simulator.BlobSet.Get(queryIdx % blobCount).Id;
                                            q.Shift = (queryIdx * 177) % q.Id.BlobSize();
                                            q.Size = Min((ui64)70, (ui64)q.Id.BlobSize() - (ui64)q.Shift);
                                            queriesB[queryIdx] = q;
                                        }
                                        TEvBlobStorage::TEvGet ev(queriesA, queryCount, TInstant::Max(),
                                                NKikimrBlobStorage::EGetHandleClass::FastRead, isRestore, false);
                                        ev.IsVerboseNoDataEnabled = isVerboseNoDataEnabled;

                                        TAutoPtr<TEvBlobStorage::TEvGetResult> getResult = simulator.Simulate(&ev);

                                        UNIT_ASSERT(getResult);
                                        UNIT_ASSERT_VALUES_EQUAL(getResult->ResponseSz, queryCount);
                                        // TODO: check the data vs queriesB
                                        for (ui64 queryIdx = 0; queryIdx < queryCount; ++queryIdx) {
                                            TEvBlobStorage::TEvGetResult::TResponse &a = getResult->Responses[queryIdx];
                                            TEvBlobStorage::TEvGet::TQuery &q = queriesB[queryIdx];
                                            UNIT_ASSERT_VALUES_EQUAL(q.Id, a.Id);
                                            if (a.Status != NKikimrProto::ERROR) {
                                                if (a.Status == NKikimrProto::OK) {
                                                    UNIT_ASSERT_VALUES_EQUAL(q.Shift, a.Shift);
                                                    UNIT_ASSERT_VALUES_EQUAL(q.Size, a.RequestedSize);
                                                    simulator.BlobSet.Check(queryIdx % blobCount, q.Id, q.Shift, q.Size, a.Buffer.ConvertToString());
                                                } else {
                                                    TStringStream str;
                                                    str << " isRestore# " << isRestore
                                                        << " setIdx# " << setIdx
                                                        << " wiped1# " << wiped1 << " (case# " << error1 << ")"
                                                        << " wiped2# " << wiped2 << " (case# " << error2 << ")"
                                                        << " error3# " << error3
                                                        << " error4# " << error4
                                                        << " status# " << a.Status;
                                                    UNIT_ASSERT_C(false, str.Str());
                                                }
                                            }
                                        } // for queryIdx
                                    } // for bci
                                } // for qci
                            } // for errorMask
                        } // for error4
                    } // for error3
                } // for wiped2
            } // for wiped1
        } // for setIdx
    } // for isRestore
    return;
}

void TestWipedErrorWithTwoBlobs(TErasureType::EErasureSpecies erasureSpecies, bool isVerboseNoDataEnabled = false) {
    TActorSystemStub actorSystemStub;

    TVector<TLogoBlobID> blobIDs = {
        TLogoBlobID(72075186224047637, 1, 863, 1, 786, 24576),
        TLogoBlobID(72075186224047637, 1, 2194, 1, 142, 12288)
    };

    TVector<TBlobTestSet::TBlob> blobs;
    for (const auto& id : blobIDs) {
        TStringBuilder builder;
        for (size_t i = 0; i < id.BlobSize(); ++i) {
            builder << 'a';
        }

        blobs.emplace_back(id, builder);
    }

    const ui32 groupId = 0;
    TBlobStorageGroupType groupType(erasureSpecies);
    const ui32 domainCount = groupType.BlobSubgroupSize();

    const ui64 queryCount = 1;
    const ui32 isRestore = 0;
    ui64 seed = 0;

    for (i32 slowDisk = -1; slowDisk < (i32)domainCount; ++slowDisk) {
        for (i32 wipedDisk = -1; wipedDisk < (i32) domainCount; ++wipedDisk) {
            if (wipedDisk == slowDisk && slowDisk != -1) {
                continue;
            }
            i32 endMayErrorSendIteration = domainCount;
            for (i32 errorIteration = -1; errorIteration < endMayErrorSendIteration; ++errorIteration) {
                for (ui32 errorDisk = 0; errorDisk < domainCount; ++errorDisk) {
                    if (errorIteration == -1 && errorDisk > 0) {
                        break;
                    }
                    if (errorIteration != -1 && (errorDisk == (ui32)slowDisk || errorDisk == (ui32)wipedDisk)) {
                        continue;
                    }

                    for (ui64 it = 0; it < 100; ++it, ++seed) {
                        SetRandomSeed(seed);
                        TGroupMock group(groupId, erasureSpecies, domainCount, 1);
                        TIntrusivePtr<TGroupQueues> groupQueues = group.MakeGroupQueues();
                        TBlobTestSet blobSet;
                        blobSet.AddBlobs(blobs);
                        group.PutBlobSet(blobSet);

                        for (ui32 idx = 0; idx < domainCount; ++idx) {
                            group.SetPredictedDelayNs(idx, 1);
                        }
                        if (slowDisk != -1) {
                            group.SetPredictedDelayNs(slowDisk, 10);
                        }

                        if (wipedDisk != -1) {
                            group.SetError(wipedDisk, NKikimrProto::NOT_YET);
                        }

                        ui64 blobCount = queryCount;
                        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesA(
                                new TEvBlobStorage::TEvGet::TQuery[queryCount]);
                        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesB(
                                new TEvBlobStorage::TEvGet::TQuery[queryCount]);
                        for (ui64 queryIdx = 0; queryIdx < queryCount; ++queryIdx) {
                            TEvBlobStorage::TEvGet::TQuery &q = queriesA[queryIdx];
                            q.Id = blobSet.Get(queryIdx % blobCount).Id;
                            q.Shift = 0;
                            q.Size = (ui64)q.Id.BlobSize();
                            queriesB[queryIdx] = q;
                        }
                        TEvBlobStorage::TEvGet ev(queriesA, queryCount, TInstant::Max(),
                                NKikimrBlobStorage::EGetHandleClass::FastRead, isRestore, false);
                        ev.IsVerboseNoDataEnabled = isVerboseNoDataEnabled;

                        TAutoPtr<TEvBlobStorage::TEvGetResult> getResult;

                        TGetImpl getImpl(group.GetInfo(), groupQueues, &ev, nullptr, TAccelerationParams{});
                        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
                        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
                        TLogContext logCtx(NKikimrServices::BS_PROXY_GET, false);
                        logCtx.LogAcc.IsLogEnabled = false;
                        getImpl.GenerateInitialRequests(logCtx, vGets);

                        for (ui64 vGetIdx = 0; vGetIdx < vGets.size(); ++vGetIdx) {
                            ui32 needIdx = RandomNumber<ui32>(vGets.size() - vGetIdx);
                            std::swap(vGets[vGetIdx], vGets[vGetIdx + needIdx]);

                            bool isLast = (vGetIdx == vGets.size() - 1);

                            if ((ui32)errorIteration == vGetIdx) {
                                group.SetError(errorDisk, NKikimrProto::ERROR);
                            }

                            TEvBlobStorage::TEvVGetResult vGetResult;
                            group.OnVGet(*vGets[vGetIdx], vGetResult);

                            // TODO: generate result
                            TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> nextVGets;
                            TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> nextVPuts;

                            getImpl.OnVGetResult(logCtx, vGetResult, nextVGets, nextVPuts, getResult);

                            std::move(nextVGets.begin(), nextVGets.end(), std::back_inserter(vGets));
                            if (ev.MustRestoreFirst) {
                                std::move(nextVPuts.begin(), nextVPuts.end(), std::back_inserter(vPuts));
                            } else {
                                UNIT_ASSERT_VALUES_EQUAL(nextVPuts.size(), 0);
                            }
                            if (getResult) {
                                break;
                            }
                            for (ui64 vPutIdx = 0; vPutIdx < vPuts.size(); ++vPutIdx) {
                                auto &putRequest = vPuts[vPutIdx]->Record;
                                TEvBlobStorage::TEvVPutResult vPutResult;
                                vPutResult.MakeError(NKikimrProto::OK, TString(), putRequest);

                                TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> nextVGets;
                                TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> nextVPuts;
                                getImpl.OnVPutResult(logCtx, vPutResult, nextVGets, nextVPuts, getResult);
                                std::move(nextVGets.begin(), nextVGets.end(), std::back_inserter(vGets));
                                std::move(nextVPuts.begin(), nextVPuts.end(), std::back_inserter(vPuts));
                                if (getResult) {
                                    break;
                                }
                            }
                            vPuts.clear();
                            if (getResult) {
                                break;
                            }
                            if (!isLast) {
                                UNIT_ASSERT_VALUES_EQUAL(getResult, nullptr);
                            }
                        }

                        UNIT_ASSERT(getResult);
                        UNIT_ASSERT_VALUES_EQUAL(getResult->ResponseSz, queryCount);
                        // TODO: check the data vs queriesB
                        for (ui64 queryIdx = 0; queryIdx < queryCount; ++queryIdx) {
                            TEvBlobStorage::TEvGetResult::TResponse &a = getResult->Responses[queryIdx];
                            TEvBlobStorage::TEvGet::TQuery &q = queriesB[queryIdx];
                            UNIT_ASSERT_VALUES_EQUAL(q.Id, a.Id);
                            if (a.Status != NKikimrProto::ERROR) {
                                if (a.Status == NKikimrProto::OK) {
                                    UNIT_ASSERT_VALUES_EQUAL(q.Shift, a.Shift);
                                    UNIT_ASSERT_VALUES_EQUAL(q.Size, a.RequestedSize);
                                    blobSet.Check(queryIdx % blobCount, q.Id, q.Shift, q.Size, a.Buffer.ConvertToString());
                                } else {
                                    TStringStream str;
                                    str << " isRestore# " << isRestore
                                        << " status# " << a.Status;
                                    UNIT_ASSERT_C(false, str.Str());
                                }
                            }
                        } // for queryIdx
                    }
                }
            }
        }
    }
    return;
}

Y_UNIT_TEST(TestBlock42GetIntervalsWipedAllOk) {
    TestIntervalsWipedAllOk(TErasureType::Erasure4Plus2Block, false);
}

Y_UNIT_TEST(TestBlock42GetIntervalsWipedError) {
    TestIntervalsWipedError(TErasureType::Erasure4Plus2Block);
}

Y_UNIT_TEST(TestBlock42WipedErrorWithTwoBlobs) {
    TestWipedErrorWithTwoBlobs(TErasureType::Erasure4Plus2Block);
}

Y_UNIT_TEST(TestMirror32GetIntervalsWipedAllOk) {
    TestIntervalsWipedAllOk(TErasureType::ErasureMirror3Plus2, false);
}

void SpecificTest(ui32 badA, ui32 badB, ui32 blobSize, TMap<i64, i64> sizeForOffset) {
    TActorSystemStub actorSystemStub;
    TErasureType::EErasureSpecies erasureSpecies = TErasureType::Erasure4Plus2Block;


    const ui32 groupId = 0;
    TBlobStorageGroupType groupType(erasureSpecies);
    const ui32 domainCount = groupType.BlobSubgroupSize();

    TGetSimulator simulator(groupId, erasureSpecies, domainCount, 1);
    simulator.GenerateBlobSet(0, 1, blobSize);
    simulator.SetError(badA, NKikimrProto::ERROR);
    simulator.SetError(badB, NKikimrProto::ERROR);

    TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesA(new TEvBlobStorage::TEvGet::TQuery[sizeForOffset.size()]);
    TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesB(new TEvBlobStorage::TEvGet::TQuery[sizeForOffset.size()]);

    size_t idx = 0;
    for (auto it = sizeForOffset.begin(); it != sizeForOffset.end(); ++it) {
        TEvBlobStorage::TEvGet::TQuery &q = queriesA[idx];
        q.Id = simulator.BlobSet.Get(0).Id;
        q.Shift = it->first;
        q.Size = Min((ui64)it->second, (ui64)q.Id.BlobSize() - (ui64)q.Shift);
        queriesB[idx] = q;
        ++idx;
    }

    TEvBlobStorage::TEvGet ev(queriesA, sizeForOffset.size(), TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::FastRead, false, false);
    ev.IsVerboseNoDataEnabled = false;

    VERBOSE("simulation start");
    TInstant begin = TInstant::Now();
    TAutoPtr<TEvBlobStorage::TEvGetResult> getResult = simulator.Simulate(&ev);
    TInstant end = TInstant::Now();
    VERBOSE("duration " << (end-begin));

    UNIT_ASSERT(getResult);
    UNIT_ASSERT_VALUES_EQUAL(getResult->ResponseSz, sizeForOffset.size());
    // TODO: check the data vs queriesB
    for (size_t i = 0; i < sizeForOffset.size(); ++i) {
        TEvBlobStorage::TEvGetResult::TResponse &a = getResult->Responses[i];
        TEvBlobStorage::TEvGet::TQuery &qb = queriesB[i];
        UNIT_ASSERT_VALUES_EQUAL(qb.Id, a.Id);
        if (a.Status != NKikimrProto::ERROR) {
            if (a.Status == NKikimrProto::OK) {
                UNIT_ASSERT_VALUES_EQUAL(qb.Shift, a.Shift);
                UNIT_ASSERT_VALUES_EQUAL(qb.Size, a.RequestedSize);
                simulator.BlobSet.Check(0, qb.Id, qb.Shift, qb.Size, a.Buffer.ConvertToString());
            } else {
                TStringStream str;
                str << " isRestore# false setIdx# 0 status# " << a.Status;
                UNIT_ASSERT_C(false, str.Str());
            }
        }
    }
}

bool SpecificTestCorrupted(TErasureType::EErasureSpecies erasureSpecies, ui32 badDomainIdx, ui32 blobSize) {
    TActorSystemStub actorSystemStub;

    const ui32 groupId = 0;
    TBlobStorageGroupType groupType(erasureSpecies);
    const ui32 domainCount = groupType.BlobSubgroupSize();
    TGetSimulator simulator(groupId, erasureSpecies, domainCount, 1);
    simulator.GenerateBlobSet(0, 1, blobSize);
    simulator.SetCorrupted(badDomainIdx);

    const auto& blobId = simulator.BlobSet.Get(0).Id;
    const ui32 shift = 0;
    const ui32 size = blobSize;
    const TInstant deadline = TInstant::Max();
    const auto handleClass = NKikimrBlobStorage::EGetHandleClass::FastRead;

    TEvBlobStorage::TEvGet ev(blobId, shift, size, deadline, handleClass, false, false);

    ev.IntegrityCheck = true;
    ev.IsVerboseNoDataEnabled = false;

    VERBOSE("simulation start");
    TInstant begin = TInstant::Now();
    TAutoPtr<TEvBlobStorage::TEvGetResult> getResult = simulator.Simulate(&ev);
    TInstant end = TInstant::Now();
    VERBOSE("duration " << (end - begin));

    UNIT_ASSERT(getResult);
    UNIT_ASSERT_VALUES_EQUAL(getResult->ResponseSz, 1);
    if (getResult->Responses[0].IntegrityCheckFailed) {
        UNIT_ASSERT(getResult->Responses[0].CorruptedPartFound);
    }
    return getResult->Responses[0].Status == NKikimrProto::ERROR && getResult->Responses[0].IntegrityCheckFailed;
}

Y_UNIT_TEST(TestBlock42GetSpecific) {
    TMap<i64, i64> sizeForOffset;
    sizeForOffset[1999000] = 7000;
    SpecificTest(5, 6, 8000000, sizeForOffset);
}

Y_UNIT_TEST(TestBlock42GetSpecific2) {
    TMap<i64, i64> sizeForOffset;
    sizeForOffset[0] = 267;
    SpecificTest(6, 7, 267, sizeForOffset);
}

Y_UNIT_TEST(TestBlock42GetSpecific3) {
    TMap<i64, i64> sizeForOffset;
    for (i64 i = 0; i < 570; ++i) {
        sizeForOffset[i * 14000] = 7000;
    }
    SpecificTest(5, 6, 8000000, sizeForOffset);
}

Y_UNIT_TEST(TestBlock42GetSpecificCorrupted) {
    ui32 testPassed = 0;
    for (ui32 domainIdx = 0; domainIdx < 8; ++domainIdx) {
        if (SpecificTestCorrupted(TErasureType::Erasure4Plus2Block, domainIdx, 8000000)) {
            ++testPassed;
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(testPassed, 6);
}

Y_UNIT_TEST(TestMirror3dcGetSpecificCorrupted) {
    ui32 testPassed = 0;
    for (ui32 domainIdx = 0; domainIdx < 9; ++domainIdx) {
        if (SpecificTestCorrupted(TErasureType::ErasureMirror3dc, domainIdx, 8000000)) {
            ++testPassed;
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(testPassed, 3);
}

Y_UNIT_TEST(TestMirror3of4GetSpecificCorrupted) {
    ui32 testPassed = 0;
    for (ui32 domainIdx = 0; domainIdx < 8; ++domainIdx) {
        if (SpecificTestCorrupted(TErasureType::ErasureMirror3of4, domainIdx, 8000000)) {
            ++testPassed;
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(testPassed, 4);
}

}

Y_UNIT_TEST_SUITE(TDSProxyLooksLikeLostTheBlob) {

class TTestPossibleBlobLost {
    TErasureType::EErasureSpecies ErasureSpecies;
    TBlobStorageGroupType GroupType;
    const ui32 DomainCount;
    const ui64 MaxQueryCount;
    const ui64 BlobSize;
    TGroupMock Group;
    TVector<NKikimrProto::EReplyStatus> ErroneousVDisks;
    TBlobTestSet BlobSet;
    ui32 InitialRequestsSize;
    TVector<ui64> RequestsOrder;
    TActorSystemStub actorSystemStub;

    static constexpr ui64 RunTestStep = 100;
    const ui64 SeedDays = TInstant::Now().Days();
    ui64 TestIteration = SeedDays;

    ui32 CalculateInitialRequestsSize() {
        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesA(new TEvBlobStorage::TEvGet::TQuery[MaxQueryCount]);
        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesB(new TEvBlobStorage::TEvGet::TQuery[MaxQueryCount]);
        for (ui64 queryIdx = 0; queryIdx < MaxQueryCount; ++queryIdx) {
            TEvBlobStorage::TEvGet::TQuery &q = queriesA[queryIdx];
            q.Id = BlobSet.Get(queryIdx).Id;
            q.Shift = 0;
            q.Size = BlobSize;
            queriesB[queryIdx] = q;
            VERBOSE("query# " << queryIdx << " shift# " << q.Shift << " size# " << q.Size);
        }
        TIntrusivePtr<TGroupQueues> groupQueues = Group.MakeGroupQueues();
        TLogContext logCtx(NKikimrServices::BS_PROXY_GET, false);
        logCtx.LogAcc.IsLogEnabled = false;
        TEvBlobStorage::TEvGet ev(queriesA, MaxQueryCount, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::Discover, true, false);
        ev.IsVerboseNoDataEnabled = false;
        TGetImpl getImpl(Group.GetInfo(), groupQueues, &ev, nullptr, TAccelerationParams{});
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
        getImpl.GenerateInitialRequests(logCtx, vGets);
        return vGets.size();
    }


public:
    TTestPossibleBlobLost(TErasureType::EErasureSpecies erasure, const ui64 blobSize, const ui64 maxQueryCount,
            TVector<NKikimrProto::EReplyStatus> injectErrors)
        : ErasureSpecies(erasure)
        , GroupType(erasure)
        , DomainCount(GroupType.BlobSubgroupSize())
        , MaxQueryCount(maxQueryCount)
        , BlobSize(blobSize)
        , Group(0, ErasureSpecies, DomainCount, 1)
        , ErroneousVDisks(Group.GetInfo()->Type.BlobSubgroupSize(), NKikimrProto::OK)
    {
        BlobSet.GenerateSet(0, MaxQueryCount, BlobSize);
        Group.PutBlobSet(BlobSet);

        UNIT_ASSERT(injectErrors.size() <= ErroneousVDisks.size());
        Copy(injectErrors.begin(), injectErrors.end(), ErroneousVDisks.begin());

        InitialRequestsSize = CalculateInitialRequestsSize();
        RequestsOrder.resize(InitialRequestsSize);
        for (ui32 i = 0; i < RequestsOrder.size(); ++i) {
            RequestsOrder[i] = i;
        }
        Sort(ErroneousVDisks.begin(), ErroneousVDisks.end());
    }

    void TestStep(NKikimrProto::EReplyStatus &gotResultPrevStatus,
            TVector<NKikimrProto::EReplyStatus> &gotResultPrevBlobStatus, bool isFirstIteraion) {
        TStringStream currentTestState;
        TAutoPtr<TEvBlobStorage::TEvGetResult> getResult;
        currentTestState << "VDisk's errors mask# { ";
        for (const NKikimrProto::EReplyStatus error : ErroneousVDisks) {
            currentTestState << error << ", ";
        }
        currentTestState << "} ";
        currentTestState << "Requests's order # { ";
        for (const ui32 idx : RequestsOrder) {
            currentTestState << idx << ", ";
        }
        currentTestState << "} ";
        currentTestState << "SeedDays# " << SeedDays << " ";
        VERBOSE(currentTestState.Str());
        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesA(new TEvBlobStorage::TEvGet::TQuery[MaxQueryCount]);
        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesB(new TEvBlobStorage::TEvGet::TQuery[MaxQueryCount]);
        for (ui64 queryIdx = 0; queryIdx < MaxQueryCount; ++queryIdx) {
            TEvBlobStorage::TEvGet::TQuery &q = queriesA[queryIdx];
            q.Id = BlobSet.Get(queryIdx).Id;
            q.Shift = 0;
            q.Size = BlobSize;
            queriesB[queryIdx] = q;
            VERBOSE("query# " << queryIdx << " shift# " << q.Shift << " size# " << q.Size);
        }

        TIntrusivePtr<TGroupQueues> groupQueues = Group.MakeGroupQueues();
        TLogContext logCtx(NKikimrServices::BS_PROXY_GET, false);
        logCtx.LogAcc.IsLogEnabled = false;
        TEvBlobStorage::TEvGet ev(queriesA, MaxQueryCount, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::Discover, true, false);
        ev.IsVerboseNoDataEnabled = false;
        TGetImpl getImpl(Group.GetInfo(), groupQueues, &ev, nullptr, TAccelerationParams{});
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
        getImpl.GenerateInitialRequests(logCtx, vGets);
        for (ui32 i = 0; i < ErroneousVDisks.size(); ++i) {
            if (ErroneousVDisks[i] == NKikimrProto::OK) {
                Group.UnsetError(i);
            } else {
                Group.SetError(i, ErroneousVDisks[i]);
            }
        }

        Y_ABORT_UNLESS(RequestsOrder.size() == vGets.size());
        for (ui64 vDIdx = 0; vDIdx < RequestsOrder.size(); ++vDIdx) {
            const ui64 vGetIdx = RequestsOrder[vDIdx];
            auto &request = vGets[vGetIdx]->Record;
            VERBOSE("vGetIdx# " << vGetIdx);
            VERBOSE("Send TEvVGet to VDiskID# " << VDiskIDFromVDiskID(request.GetVDiskID()));
            //ui64 messageCookie = request->Record.GetCookie();
            TEvBlobStorage::TEvVGetResult vGetResult;
            Group.OnVGet(*vGets[vGetIdx], vGetResult);
            VERBOSE("vGetResult.ToString()# " << vGetResult.ToString());

            TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> nextVGets;
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> nextVPuts;
            getImpl.OnVGetResult(logCtx, vGetResult, nextVGets, nextVPuts, getResult);
            for (ui64 i = 0; i < nextVPuts.size(); ++i) {
                auto &vDiskID = nextVPuts[i]->Record.GetVDiskID();
                VERBOSE("Additional TEvVPut to VDiskID# " << VDiskIDFromVDiskID(vDiskID));
                vPuts.push_back(std::move(nextVPuts[i]));
            }
            for (ui64 vPutIdx = 0; vPutIdx < vPuts.size(); ++vPutIdx) {
                auto &putRequest = vPuts[vPutIdx]->Record;
                TEvBlobStorage::TEvVPutResult vPutResult;
                vPutResult.MakeError(NKikimrProto::OK, TString(), putRequest);

                TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> nextVGets;
                TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> nextVPuts;
                getImpl.OnVPutResult(logCtx, vPutResult, nextVGets, nextVPuts, getResult);
                UNIT_ASSERT_C(nextVGets.empty(), currentTestState.Str());
                std::move(nextVPuts.begin(), nextVPuts.end(), std::back_inserter(vPuts));
                if (getResult) {
                    break;
                }
            }
            for (ui64 i = 0; i < nextVGets.size(); ++i) {
                auto &vDiskID = nextVGets[i]->Record.GetVDiskID();
                VERBOSE("Additional TEvVGet to VDiskID# " << VDiskIDFromVDiskID(vDiskID));
                vGets.push_back(std::move(nextVGets[i]));
                RequestsOrder.push_back(RequestsOrder.size());
            }
            if (getResult) {
                break;
            }
        }
        UNIT_ASSERT_C(getResult, currentTestState.Str());
        currentTestState << "getResult # " << getResult->Print(false);
        if (isFirstIteraion) {
            gotResultPrevStatus = getResult->Status;
            for (ui64 queryIdx = 0; queryIdx < MaxQueryCount; ++queryIdx) {
                gotResultPrevBlobStatus[queryIdx] = getResult->Responses[queryIdx].Status;
            }
        } else {
            UNIT_ASSERT_C(gotResultPrevStatus == getResult->Status, currentTestState.Str());
            for (ui64 queryIdx = 0; queryIdx < MaxQueryCount; ++queryIdx) {
                UNIT_ASSERT_C(gotResultPrevBlobStatus[queryIdx] == getResult->Responses[queryIdx].Status,
                        currentTestState.Str());
            }
        }
        for (ui64 queryIdx = 0; queryIdx < MaxQueryCount; ++queryIdx) {
            if (getResult->Responses[queryIdx].Status == NKikimrProto::OK) {
                TEvBlobStorage::TEvGetResult::TResponse &a = getResult->Responses[queryIdx];
                TEvBlobStorage::TEvGet::TQuery &q = queriesB[queryIdx];
                UNIT_ASSERT_VALUES_EQUAL_C(q.Id, a.Id, currentTestState.Str());
                UNIT_ASSERT_VALUES_EQUAL_C(a.Status, NKikimrProto::OK, currentTestState.Str());
                UNIT_ASSERT_VALUES_EQUAL_C(q.Shift, a.Shift, currentTestState.Str());
                UNIT_ASSERT_VALUES_EQUAL_C(q.Size, a.RequestedSize, currentTestState.Str());
                BlobSet.Check(queryIdx, q.Id, q.Shift, q.Size, a.Buffer.ConvertToString());
            }
        }
        RequestsOrder.resize(InitialRequestsSize);
    }

    void Run() {
        do { // while(std::next_permutation(ErroneousVDisks.begin(), ErroneousVDisks.end()));
            NKikimrProto::EReplyStatus gotResultPrevStatus;
            TVector<NKikimrProto::EReplyStatus> gotResultPrevBlobStatus(MaxQueryCount);
            bool isFirstIteraion = true;
            Sort(RequestsOrder.begin(), RequestsOrder.end());
            do { // while(std::next_permutation(RequestsOrder.begin(), RequestsOrder.end()));
                ++TestIteration;
                if (TestIteration % RunTestStep != 0) {
                    continue; // Comment this statement to run all tests
                }
                TestStep(gotResultPrevStatus, gotResultPrevBlobStatus, isFirstIteraion);
                isFirstIteraion = false;
            } while(std::next_permutation(RequestsOrder.begin(), RequestsOrder.end()));
        } while(std::next_permutation(ErroneousVDisks.begin(), ErroneousVDisks.end()));
    }
};

Y_UNIT_TEST(TDSProxyLooksLikeLostTheBlobBlock42) {
    const ui64 blobSize = 128;
    for (ui32 i = 1; i < 3; ++i) {
        TTestPossibleBlobLost test(TErasureType::Erasure4Plus2Block, blobSize, i, {NKikimrProto::ERROR, NKikimrProto::ERROR});
        test.Run();
    }
}




class TTestNoDataRegression {
public:
    enum EMode {
        TwoReadErrors = 0,
        ReadAndWriteErrors = 1
    };
protected:
    TErasureType::EErasureSpecies ErasureSpecies = TErasureType::Erasure4Plus2Block;
    TBlobStorageGroupType GroupType;
    const ui32 DomainCount;
    const ui64 MaxQueryCount = 1;
    const ui64 BlobSize = 128;
    TGroupMock Group;
    TBlobTestSet BlobSet;
    ui32 InitialRequestsSize;
    EMode Mode;
    TActorSystemStub actorSystemStub;

    ui32 CalculateInitialRequestsSize() {
        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesA(new TEvBlobStorage::TEvGet::TQuery[MaxQueryCount]);
        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesB(new TEvBlobStorage::TEvGet::TQuery[MaxQueryCount]);
        for (ui64 queryIdx = 0; queryIdx < MaxQueryCount; ++queryIdx) {
            TEvBlobStorage::TEvGet::TQuery &q = queriesA[queryIdx];
            q.Id = BlobSet.Get(queryIdx).Id;
            q.Shift = 0;
            q.Size = BlobSize;
            queriesB[queryIdx] = q;
            VERBOSE("query# " << queryIdx << " shift# " << q.Shift << " size# " << q.Size);
        }
        TIntrusivePtr<TGroupQueues> groupQueues = Group.MakeGroupQueues();
        TLogContext logCtx(NKikimrServices::BS_PROXY_GET, false);
        logCtx.LogAcc.IsLogEnabled = false;
        TEvBlobStorage::TEvGet ev(queriesA, MaxQueryCount, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::Discover, true, false);
        ev.IsVerboseNoDataEnabled = false;
        TGetImpl getImpl(Group.GetInfo(), groupQueues, &ev, nullptr, TAccelerationParams{});
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
        getImpl.GenerateInitialRequests(logCtx, vGets);
        return vGets.size();
    }


public:
    TTestNoDataRegression(EMode mode)
        : GroupType(ErasureSpecies)
        , DomainCount(GroupType.BlobSubgroupSize())
        , Group(0, ErasureSpecies, DomainCount, 1)
        , Mode(mode)
    {
        BlobSet.GenerateSet(0, MaxQueryCount, BlobSize);

        // Put part idx# 0 to handoff# 0
        Group.PutBlobSet(BlobSet, 1);

        InitialRequestsSize = CalculateInitialRequestsSize();
    }

    void Run() {
        TLogoBlobID id = BlobSet.Get(0).Id;
        ui32 readError = Group.DomainIdxForBlobSubgroupIdx(id, 1);
        ui32 readError2= Group.DomainIdxForBlobSubgroupIdx(id, 5);

        TVector<ui32> requestOrder; // fail domian idx
        requestOrder.reserve(8);
        requestOrder.push_back(Group.DomainIdxForBlobSubgroupIdx(id, 0)); // no data (was offilne)
        requestOrder.push_back(Group.DomainIdxForBlobSubgroupIdx(id, 7)); // no data (empty handoff)
        requestOrder.push_back(Group.DomainIdxForBlobSubgroupIdx(id, 5)); // readError2
        requestOrder.push_back(Group.DomainIdxForBlobSubgroupIdx(id, 2)); // p2
        requestOrder.push_back(Group.DomainIdxForBlobSubgroupIdx(id, 3)); // p3
        requestOrder.push_back(Group.DomainIdxForBlobSubgroupIdx(id, 4)); // p4
        requestOrder.push_back(Group.DomainIdxForBlobSubgroupIdx(id, 6)); // handoff p0

        ui32 firstPutReplyErrorDomainIdx = Group.DomainIdxForBlobSubgroupIdx(id, 6);

        if (Mode == TwoReadErrors) {
            requestOrder.push_back(Group.DomainIdxForBlobSubgroupIdx(id, 1)); // readError
            Group.SetError(readError, NKikimrProto::ERROR);
        }

        Group.SetError(readError2, NKikimrProto::ERROR);


        TAutoPtr<TEvBlobStorage::TEvGetResult> getResult;
        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesA(new TEvBlobStorage::TEvGet::TQuery[MaxQueryCount]);
        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queriesB(new TEvBlobStorage::TEvGet::TQuery[MaxQueryCount]);
        for (ui64 queryIdx = 0; queryIdx < MaxQueryCount; ++queryIdx) {
            TEvBlobStorage::TEvGet::TQuery &q = queriesA[queryIdx];
            q.Id = BlobSet.Get(queryIdx).Id;
            q.Shift = 0;
            q.Size = BlobSize;
            queriesB[queryIdx] = q;
            VERBOSE("query# " << queryIdx << " shift# " << q.Shift << " size# " << q.Size);
        }

        TIntrusivePtr<TGroupQueues> groupQueues = Group.MakeGroupQueues();
        TLogContext logCtx(NKikimrServices::BS_PROXY_GET, false);
        logCtx.LogAcc.IsLogEnabled = false;
        TEvBlobStorage::TEvGet ev(queriesA, MaxQueryCount, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::Discover, true, false);
        ev.IsVerboseNoDataEnabled = false;
        TGetImpl getImpl(Group.GetInfo(), groupQueues, &ev, nullptr, TAccelerationParams{});
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> vGets;
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> vPuts;
        getImpl.GenerateInitialRequests(logCtx, vGets);

        for (ui64 vDIdx = 0; vDIdx < requestOrder.size(); ++vDIdx) {
            const ui64 domainIdx = requestOrder[vDIdx];
            ui64 vGetIdx = 0;
            for (ui32 i = 0; i < vGets.size(); ++i) {
                if (VDiskIDFromVDiskID(vGets[i]->Record.GetVDiskID()).FailDomain == domainIdx) {
                    vGetIdx = i;
                }
            }
            auto &request = vGets[vGetIdx]->Record;
            VERBOSE("vGetIdx# " << vGetIdx << " request# " << vDIdx << " to domainIdx# " << domainIdx);
            VERBOSE("Send TEvVGet to VDiskID# " << VDiskIDFromVDiskID(request.GetVDiskID()));
            //ui64 messageCookie = request->Record.GetCookie();
            TEvBlobStorage::TEvVGetResult vGetResult;
            Group.OnVGet(*vGets[vGetIdx], vGetResult);
            VERBOSE("vGetResult.ToString()# " << vGetResult.ToString());

            TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> nextVGets;
            TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> nextVPuts;
            getImpl.OnVGetResult(logCtx, vGetResult, nextVGets, nextVPuts, getResult);
            for (ui64 i = 0; i < nextVPuts.size(); ++i) {
                auto &vDiskID = nextVPuts[i]->Record.GetVDiskID();
                VERBOSE("Additional TEvVPut to VDiskID# " << VDiskIDFromVDiskID(vDiskID));
                vPuts.push_back(std::move(nextVPuts[i]));
            }

            for (ui64 i = 0; i < nextVGets.size(); ++i) {
                auto &vDiskID = nextVGets[i]->Record.GetVDiskID();
                VERBOSE("Additional TEvVGet to VDiskID# " << VDiskIDFromVDiskID(vDiskID));
                vGets.push_back(std::move(nextVGets[i]));
                requestOrder.push_back(requestOrder.size());
            }
            if (getResult) {
                break;
            }
        }

        if (Mode == ReadAndWriteErrors) {
            Group.SetError(firstPutReplyErrorDomainIdx, NKikimrProto::ERROR);
            for (ui64 vPutIdx = 0; vPutIdx < vPuts.size(); ++vPutIdx) {
                TVDiskID vDiskId = VDiskIDFromVDiskID(vPuts[vPutIdx]->Record.GetVDiskID());
                if (vDiskId.FailDomain == firstPutReplyErrorDomainIdx) {
                    if (vPutIdx != 0) {
                        std::swap(vPuts[0], vPuts[vPutIdx]);
                    }
                }
            }
        }

        if (!getResult) {
            for (ui64 vPutIdx = 0; vPutIdx < vPuts.size(); ++vPutIdx) {
                auto &putRequest = vPuts[vPutIdx]->Record;
                TEvBlobStorage::TEvVPutResult vPutResult;
                if (Mode == ReadAndWriteErrors && vPutIdx == 0) {
                    vPutResult.MakeError(NKikimrProto::ERROR, TString(), putRequest);
                } else {
                    vPutResult.MakeError(NKikimrProto::OK, TString(), putRequest);
                }

                TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> nextVGets;
                TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> nextVPuts;
                getImpl.OnVPutResult(logCtx, vPutResult, nextVGets, nextVPuts, getResult);
                std::move(nextVPuts.begin(), nextVPuts.end(), std::back_inserter(vPuts));
                if (getResult) {
                    break;
                }
            }
        }
        VERBOSE("GetResult# " << getResult->ToString());
        UNIT_ASSERT(getResult);
        UNIT_ASSERT_VALUES_EQUAL(getResult->Status, NKikimrProto::OK);
        for (ui64 queryIdx = 0; queryIdx < MaxQueryCount; ++queryIdx) {
            TEvBlobStorage::TEvGetResult::TResponse &a = getResult->Responses[queryIdx];
            TEvBlobStorage::TEvGet::TQuery &q = queriesB[queryIdx];
            UNIT_ASSERT_VALUES_EQUAL(q.Id, a.Id);
            UNIT_ASSERT_VALUES_EQUAL(a.Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(q.Shift, a.Shift);
            UNIT_ASSERT_VALUES_EQUAL(q.Size, a.RequestedSize);
            BlobSet.Check(queryIdx, q.Id, q.Shift, q.Size, a.Buffer.ConvertToString());
        }
    }
};

Y_UNIT_TEST(TDSProxyNoDataRegressionBlock42) {
    TTestNoDataRegression test(TTestNoDataRegression::ReadAndWriteErrors);
    test.Run();
}

Y_UNIT_TEST(TDSProxyErrorRegressionBlock42) {
    TTestNoDataRegression test(TTestNoDataRegression::TwoReadErrors);
    test.Run();
}

} // Y_UNIT_TEST_SUITE TDSProxyLooksLikeLostTheBlob
} // namespace NDSProxyGetTest
} // namespace NKikimr
