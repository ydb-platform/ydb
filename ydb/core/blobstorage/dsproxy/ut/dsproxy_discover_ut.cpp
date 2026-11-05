#include "defs.h"
#include "dsproxy_env_mock_ut.h"
#include "dsproxy_vdisk_mock_ut.h"
#include "dsproxy_test_state_ut.h"

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

namespace NKikimr {
namespace NDSProxyDiscoverTest {

Y_UNIT_TEST_SUITE(TDSProxyDiscover) {

void CheckMirror3of4PendingPages(NKikimrProto::EReplyStatus firstPageStatus, bool trailingFailures = true) {
    TBlobStorageGroupType type(TErasureType::ErasureMirror3of4);
    TTestBasicRuntime runtime;
    SetupRuntime(runtime);
    TDSProxyEnv env;
    env.Configure(runtime, type, 0, 0, TBlobStorageGroupInfo::EEM_NONE);
    TTestState testState(runtime, env.Info);

    const ui64 tabletId = 72075186224047637;
    const TLogoBlobID fragmentId(tabletId, 1, 2, 0, testState.BlobSize, 0);
    auto& group = testState.GetGroupMock();
    constexpr ui32 pageSize = 32;
    for (ui32 index = 5; index < 8; ++index) {
        auto& disk = group.GetVDiskInSubgroup(fragmentId, index);
        for (ui32 step = 2; step < 2 + pageSize; ++step) {
            const TLogoBlobID id(tabletId, 1, step, 0, testState.BlobSize, 0);
            UNIT_ASSERT_VALUES_EQUAL(disk.Put(TLogoBlobID(id, 3), TString()), NKikimrProto::OK);
        }
    }
    group.GetVDiskInSubgroup(fragmentId, 4).SetError(NKikimrProto::ERROR);

    auto discover = std::make_unique<TEvBlobStorage::TEvDiscover>(
        tabletId, 1, true, false, TInstant::Max(), 0, true);
    runtime.Send(new IEventHandle(env.RealProxyActorId, testState.EdgeActor, discover.release()), 0, true);

    // Model abandoned MinLatency puts: only metadata on three disks survived.
    // Each disk has exactly one full page of unrecoverable remnants. The other
    // disks return one error and four empty ranges.
    TVector<TEvBlobStorage::TEvVGet::TPtr> initial;
    for (ui32 i = 0; i < 8; ++i) {
        initial.push_back(testState.GrabEventPtr<TEvBlobStorage::TEvVGet>());
    }
    for (auto& request : initial) {
        UNIT_ASSERT(request->Get()->Record.HasRangeQuery());
        const auto disk = VDiskIDFromVDiskID(request->Get()->Record.GetVDiskID());
        const ui32 index = env.Info->GetIdxInSubgroup(disk, fragmentId.Hash());
        auto reply = testState.CreateEventResultPtr(request, NKikimrProto::UNKNOWN);
        group.OnVGet(*request->Get(), *reply->Get());
        if (index >= 5) {
            UNIT_ASSERT_VALUES_EQUAL(request->Get()->Record.GetRangeQuery().GetMaxResults(), pageSize);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.ResultSize(), pageSize);
            UNIT_ASSERT(reply->Get()->Record.GetIsRangeOverflow());
        }
        runtime.Send(reply.Release());
    }

    // Each metadata stream is queried again below fragmentId. Hold whole requests
    // at three independent mock disks; no events within one disk are reordered.
    TVector<TEvBlobStorage::TEvVGet::TPtr> nextPages;
    THashSet<ui32> queriedDisks;
    for (ui32 i = 0; i < 3; ++i) {
        auto request = testState.GrabEventPtr<TEvBlobStorage::TEvVGet>();
        const auto& record = request->Get()->Record;
        UNIT_ASSERT(record.HasRangeQuery());
        UNIT_ASSERT(LogoBlobIDFromLogoBlobID(record.GetRangeQuery().GetFrom()) < fragmentId);
        const auto disk = VDiskIDFromVDiskID(record.GetVDiskID());
        const ui32 index = env.Info->GetIdxInSubgroup(disk, fragmentId.Hash());
        UNIT_ASSERT(index >= 5 && index < 8);
        UNIT_ASSERT(queriedDisks.insert(index).second);
        nextPages.push_back(std::move(request));
    }
    for (ui32 i = 0; i < nextPages.size(); ++i) {
        const auto status = i == 0 ? firstPageStatus
            : trailingFailures ? NKikimrProto::ERROR : NKikimrProto::OK;
        auto& request = nextPages[i];
        if (status != NKikimrProto::OK) {
            group.SetError(VDiskIDFromVDiskID(request->Get()->Record.GetVDiskID()), status);
        }
        auto reply = testState.CreateEventResultPtr(request, NKikimrProto::UNKNOWN);
        group.OnVGet(*request->Get(), *reply->Get());
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), status);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.ResultSize(), 0);
        runtime.Send(reply.Release());
    }

    // With trailing failures, the failure model is exceeded. An early empty
    // page (or the second failed disk) cannot justify NODATA. The controls have
    // at most two failed disks and must return NODATA after all remaining pages.
    auto result = testState.GrabEventPtr<TEvBlobStorage::TEvDiscoverResult>();
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status,
        trailingFailures ? NKikimrProto::ERROR : NKikimrProto::NODATA);
}

Y_UNIT_TEST(Mirror3of4EmptyPageDoesNotHidePendingFailures) {
    CheckMirror3of4PendingPages(NKikimrProto::OK);
}

Y_UNIT_TEST(Mirror3of4FailedPageDoesNotHidePendingFailures) {
    CheckMirror3of4PendingPages(NKikimrProto::ERROR);
}

Y_UNIT_TEST(Mirror3of4EmptyRemainingPagesReturnNoData) {
    CheckMirror3of4PendingPages(NKikimrProto::OK, false);
}

Y_UNIT_TEST(Mirror3of4TwoFailedDisksAndEmptyRemainderReturnNoData) {
    CheckMirror3of4PendingPages(NKikimrProto::ERROR, false);
}

void CheckOtherErasurePendingPages(TErasureType::EErasureSpecies erasure,
        NKikimrProto::EReplyStatus firstPageStatus, bool trailingFailures) {
    const bool mirror = erasure == TErasureType::ErasureMirror3dc;
    TBlobStorageGroupType type(erasure);
    TTestBasicRuntime runtime;
    SetupRuntime(runtime);
    TDSProxyEnv env;
    env.Configure(runtime, type, 0, 0, TBlobStorageGroupInfo::EEM_NONE);
    TTestState state(runtime, env.Info);
    auto& group = state.GetGroupMock();
    const ui64 tabletId = 72075186224047637;
    const TLogoBlobID anchor(tabletId, 1, 2, 0, state.BlobSize, 0);
    const TVector<ui32> slots = mirror ? TVector<ui32>{0, 3, 6} : TVector<ui32>{0, 6, 7};
    TVector<TVDiskMock*> pageDisks;
    THashSet<TVDiskID> pageDiskIds;
    for (ui32 slot : slots) {
        auto& disk = group.GetVDiskInSubgroup(anchor, slot);
        pageDisks.push_back(&disk);
        pageDiskIds.insert(disk.GetVDiskId());
    }
    const ui32 pageSize = mirror ? 100 : 10;
    TVector<TLogoBlobID> ids;
    for (ui32 step = 2; ids.size() < pageSize && step < 100000; ++step) {
        const TLogoBlobID id(tabletId, 1, step, 0, state.BlobSize, 0);
        bool valid = true;
        for (auto* disk : pageDisks) {
            valid &= bool(TIngress::CreateIngressWithLocal(&env.Info->GetTopology(),
                disk->GetVDiskId(), TLogoBlobID(id, 1)));
        }
        if (!valid) {
            continue;
        }
        ids.push_back(id);
        for (auto* disk : pageDisks) {
            UNIT_ASSERT_VALUES_EQUAL(disk->Put(TLogoBlobID(id, 1),
                TString(type.PartSize(id), 'x')), NKikimrProto::OK);
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(ids.size(), pageSize);
    // block-4-2: three copies of one part are still an unrecoverable fragment.
    // mirror-3dc: copies in only one realm are recoverable but not fully written.
    // Two initial failures in a different realm still fit its failure model.
    group.GetVDiskInSubgroup(anchor, mirror ? 1 : 4).SetError(NKikimrProto::ERROR);
    if (mirror) {
        group.GetVDiskInSubgroup(anchor, 4).SetError(NKikimrProto::ERROR);
    }
    auto discover = std::make_unique<TEvBlobStorage::TEvDiscover>(
        tabletId, 1, true, false, TInstant::Max(), 0, true);
    runtime.Send(new IEventHandle(env.RealProxyActorId, state.EdgeActor, discover.release()), 0, true);
    TVector<TEvBlobStorage::TEvVGet::TPtr> initial;
    for (ui32 i = 0; i < env.Info->GetTotalVDisksNum(); ++i) {
        initial.push_back(state.GrabEventPtr<TEvBlobStorage::TEvVGet>());
    }
    for (auto& request : initial) {
        UNIT_ASSERT(request->Get()->Record.HasRangeQuery());
        auto reply = state.CreateEventResultPtr(request, NKikimrProto::UNKNOWN);
        group.OnVGet(*request->Get(), *reply->Get());
        const auto disk = VDiskIDFromVDiskID(request->Get()->Record.GetVDiskID());
        if (pageDiskIds.contains(disk)) {
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.ResultSize(), pageSize);
            UNIT_ASSERT(reply->Get()->Record.GetIsRangeOverflow());
        }
        runtime.Send(reply.Release());
    }
    if (mirror) {
        // Lose the unconfirmed copies after the index read, before body reads.
        // Get must return NODATA for each phantom before Discover asks for more.
        for (auto* disk : pageDisks) {
            disk->Wipe();
        }
    }
    TVector<TEvBlobStorage::TEvVGet::TPtr> nextPages;
    THashSet<TVDiskID> queried;
    ui32 bodyQueries = 0;
    while (nextPages.size() < pageDisks.size()) {
        auto request = state.GrabEventPtr<TEvBlobStorage::TEvVGet>();
        const auto& record = request->Get()->Record;
        if (!record.HasRangeQuery()) {
            UNIT_ASSERT(mirror);
            UNIT_ASSERT(++bodyQueries < 10000);
            auto reply = state.CreateEventResultPtr(request, NKikimrProto::UNKNOWN);
            group.OnVGet(*request->Get(), *reply->Get());
            runtime.Send(reply.Release());
            continue;
        }
        const auto disk = VDiskIDFromVDiskID(record.GetVDiskID());
        UNIT_ASSERT(pageDiskIds.contains(disk));
        UNIT_ASSERT(queried.insert(disk).second);
        UNIT_ASSERT(LogoBlobIDFromLogoBlobID(record.GetRangeQuery().GetFrom()) < ids.front());
        nextPages.push_back(std::move(request));
    }
    UNIT_ASSERT(!mirror || bodyQueries > 0);
    for (ui32 i = 0; i < nextPages.size(); ++i) {
        auto& request = nextPages[i];
        const auto status = i == 0 ? firstPageStatus
            : trailingFailures ? NKikimrProto::ERROR : NKikimrProto::OK;
        if (status != NKikimrProto::OK) {
            group.SetError(VDiskIDFromVDiskID(request->Get()->Record.GetVDiskID()), status);
        }
        auto reply = state.CreateEventResultPtr(request, NKikimrProto::UNKNOWN);
        group.OnVGet(*request->Get(), *reply->Get());
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), status);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.ResultSize(), 0);
        runtime.Send(reply.Release());
    }
    auto result = state.GrabEventPtr<TEvBlobStorage::TEvDiscoverResult>();
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status,
        trailingFailures ? NKikimrProto::ERROR : NKikimrProto::NODATA);
}

#define PENDING_PAGE_TESTS(Name, Erasure) \
Y_UNIT_TEST(Name##EmptyPageDoesNotHidePendingFailures) { \
    CheckOtherErasurePendingPages(Erasure, NKikimrProto::OK, true); \
} \
Y_UNIT_TEST(Name##FailedPageDoesNotHidePendingFailures) { \
    CheckOtherErasurePendingPages(Erasure, NKikimrProto::ERROR, true); \
} \
Y_UNIT_TEST(Name##EmptyRemainingPagesReturnNoData) { \
    CheckOtherErasurePendingPages(Erasure, NKikimrProto::OK, false); \
} \
Y_UNIT_TEST(Name##ToleratedFailureAndEmptyRemainderReturnNoData) { \
    CheckOtherErasurePendingPages(Erasure, NKikimrProto::ERROR, false); \
}

PENDING_PAGE_TESTS(Block42, TErasureType::Erasure4Plus2Block)
PENDING_PAGE_TESTS(Mirror3dc, TErasureType::ErasureMirror3dc)
#undef PENDING_PAGE_TESTS

Y_UNIT_TEST(Block42Success) {
    TBlobStorageGroupType type(TErasureType::Erasure4Plus2Block);
    TTestBasicRuntime runtime;
    SetupRuntime(runtime);
    TDSProxyEnv env;
    env.Configure(runtime, type, 0, 0, TBlobStorageGroupInfo::EEM_NONE);
    TTestState testState(runtime, env.Info);

    const ui64 tabletId = 72075186224047637;
    TLogoBlobID blobId1(tabletId, 1, 1, 0, testState.BlobSize, 0);
    TLogoBlobID blobId2(tabletId, 1, 2, 0, testState.BlobSize, 0);

    TGroupMock& groupMock = testState.GetGroupMock();
    groupMock.Put(blobId1, testState.BlobData);
    groupMock.Put(blobId2, testState.BlobData);

    auto discover = std::make_unique<TEvBlobStorage::TEvDiscover>(
        tabletId, 1, true, false, TInstant::Max(), 0, true);
    runtime.Send(new IEventHandle(env.RealProxyActorId, testState.EdgeActor, discover.release()), 0, true);

    testState.HandleVGetsWithMock(8); // range queries
    testState.HandleVGetsWithMock(8); // get the blob

    TEvBlobStorage::TEvDiscoverResult::TPtr ev = testState.GrabEventPtr<TEvBlobStorage::TEvDiscoverResult>();
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Id, blobId2);
}

Y_UNIT_TEST(Block42SuccessLastBlobMissingParts) {
    TBlobStorageGroupType type(TErasureType::Erasure4Plus2Block);
    TTestBasicRuntime runtime;
    SetupRuntime(runtime);
    TDSProxyEnv env;
    env.Configure(runtime, type, 0, 0, TBlobStorageGroupInfo::EEM_NONE);
    TTestState testState(runtime, env.Info);

    const ui64 tabletId = 72075186224047637;
    TLogoBlobID blobId1(tabletId, 1, 1, 0, testState.BlobSize, 0);
    TLogoBlobID blobId2(tabletId, 1, 2, 0, testState.BlobSize, 0);

    TGroupMock& groupMock = testState.GetGroupMock();
    groupMock.Put(blobId1, testState.BlobData);
    THashSet<ui32> selectedParts{3, 4, 5, 6};
    groupMock.Put(blobId2, testState.BlobData, 0, &selectedParts);

    auto discover = std::make_unique<TEvBlobStorage::TEvDiscover>(
        tabletId, 1, true, false, TInstant::Max(), 0, true);
    runtime.Send(new IEventHandle(env.RealProxyActorId, testState.EdgeActor, discover.release()), 0, true);

    testState.HandleVGetsWithMock(8);
    testState.HandleVGetsWithMock(8);
    testState.HandleVPutsWithMock(2); // restore puts

    TEvBlobStorage::TEvDiscoverResult::TPtr ev = testState.GrabEventPtr<TEvBlobStorage::TEvDiscoverResult>();
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Id, blobId2);
}

Y_UNIT_TEST(Block42SuccessLastBlobNotFullyWritten) {
    TBlobStorageGroupType type(TErasureType::Erasure4Plus2Block);
    TTestBasicRuntime runtime;
    SetupRuntime(runtime);
    TDSProxyEnv env;
    env.Configure(runtime, type, 0, 0, TBlobStorageGroupInfo::EEM_NONE);
    TTestState testState(runtime, env.Info);

    const ui64 tabletId = 72075186224047637;
    TLogoBlobID blobId1(tabletId, 1, 1, 0, testState.BlobSize, 0);
    TLogoBlobID blobId2(tabletId, 1, 2, 0, testState.BlobSize, 0);

    TGroupMock& groupMock = testState.GetGroupMock();
    groupMock.Put(blobId1, testState.BlobData);
    THashSet<ui32> selectedParts{1, 2, 3};
    groupMock.Put(blobId2, testState.BlobData, 0, &selectedParts);

    auto discover = std::make_unique<TEvBlobStorage::TEvDiscover>(
        tabletId, 1, true, false, TInstant::Max(), 0, true);
    runtime.Send(new IEventHandle(env.RealProxyActorId, testState.EdgeActor, discover.release()), 0, true);

    testState.HandleVGetsWithMock(8);
    testState.HandleVGetsWithMock(8);

    TEvBlobStorage::TEvDiscoverResult::TPtr ev = testState.GrabEventPtr<TEvBlobStorage::TEvDiscoverResult>();
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Id, blobId1);
}

Y_UNIT_TEST(Block42ErrorWhenBlobIsLostAfterDiscover) {
    TBlobStorageGroupType type(TErasureType::Erasure4Plus2Block);
    TTestBasicRuntime runtime;
    SetupRuntime(runtime);
    TDSProxyEnv env;
    env.Configure(runtime, type, 0, 0, TBlobStorageGroupInfo::EEM_NONE);
    TTestState testState(runtime, env.Info);

    const ui64 tabletId = 72075186224047637;
    TLogoBlobID blobId1(tabletId, 1, 1, 0, testState.BlobSize, 0);
    TLogoBlobID blobId2(tabletId, 1, 2, 0, testState.BlobSize, 0);

    TGroupMock& groupMock = testState.GetGroupMock();
    groupMock.Put(blobId1, testState.BlobData);
    THashSet<ui32> selectedParts{1, 2, 3, 4};
    groupMock.Put(blobId2, testState.BlobData, 0, &selectedParts);

    auto discover = std::make_unique<TEvBlobStorage::TEvDiscover>(
        tabletId, 1, true, false, TInstant::Max(), 0, true);
    runtime.Send(new IEventHandle(env.RealProxyActorId, testState.EdgeActor, discover.release()), 0, true);

    testState.HandleVGetsWithMock(8);
    testState.GroupMock.SetError(TVDiskID(0, 1, 0, 2, 0), NKikimrProto::ERROR);
    testState.HandleVGetsWithMock(8);

    TEvBlobStorage::TEvDiscoverResult::TPtr ev = testState.GrabEventPtr<TEvBlobStorage::TEvDiscoverResult>();
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::ERROR);
}

void CheckMirror3dcSurvivingDiscovery(const TVector<NKikimrBlobStorage::TGroupInfo::TFailRealm::EState>& states,
        bool readBody, bool hasBlob, bool indexError = false, bool ghost = false) {
    using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    const TBlobStorageGroupType type(TErasureType::ErasureMirror3dc);
    TTestBasicRuntime runtime;
    SetupRuntime(runtime);
    TDSProxyEnv env;
    env.Configure(runtime, type, 0, 0, TBlobStorageGroupInfo::EEM_NONE,
        DormantTimeoutDefaultControl, false, states);
    TTestState testState(runtime, env.Info);
    TGroupMock& group = testState.GetGroupMock();
    const ui64 tabletId = 72075186224047637;
    const TLogoBlobID blobId(tabletId, 1, 1, 0, testState.BlobSize, 0);
    const TLogoBlobID ghostId(tabletId, 1, 2, 0, testState.BlobSize, 0);
    TVDiskID sourceDisk;
    for (ui32 idx = 0; idx < 9; ++idx) {
        TVDiskMock& disk = group.GetVDiskInSubgroup(blobId, idx);
        const ui32 realm = disk.GetVDiskId().FailRealm;
        if (states[realm] != TFailRealm::F && states[realm] != TFailRealm::R) {
            sourceDisk = disk.GetVDiskId();
            if (hasBlob) {
                UNIT_ASSERT_VALUES_EQUAL(disk.Put(TLogoBlobID(blobId, idx % 3 + 1), testState.BlobData), NKikimrProto::OK);
            }
            break;
        }
    }
    ui32 rangeRequests = 0;
    bool ghostInjected = false;
    bool readData = false;
    runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
        if (std::find(env.VDisks.begin(), env.VDisks.end(), event->Recipient) == env.VDisks.end()) {
            return TTestActorRuntime::EEventAction::PROCESS;
        }
        switch (event->GetTypeRewrite()) {
            case TEvBlobStorage::EvVGet: {
                TEvBlobStorage::TEvVGet::TPtr request(static_cast<TEventHandle<TEvBlobStorage::TEvVGet>*>(event.Release()));
                const TVDiskID disk = VDiskIDFromVDiskID(request->Get()->Record.GetVDiskID());
                UNIT_ASSERT_C(!env.Info->IsFailedFailRealm(disk.FailRealm), "Discover must not query F");
                TEvBlobStorage::TEvVGetResult::TPtr reply = testState.CreateEventResultPtr(request, NKikimrProto::UNKNOWN);
                if (request->Get()->Record.HasRangeQuery()) {
                    ++rangeRequests;
                    if (ghost && !ghostInjected && disk == sourceDisk) {
                        const ui32 part = env.Info->GetIdxInSubgroup(disk, ghostId.Hash()) % 3 + 1;
                        const TMaybe<TIngress> ingress = TIngress::CreateIngressWithLocal(
                            &env.Info->GetTopology(), disk, TLogoBlobID(ghostId, part));
                        UNIT_ASSERT(ingress);
                        ui64 rawIngress = ingress->Raw();
                        const NMatrix::TVectorType local = ingress->LocalParts(type);
                        reply->Get()->AddResult(NKikimrProto::OK, ghostId, nullptr, &rawIngress, &local, true, false);
                        ghostInjected = true;
                    }
                } else {
                    if (states[disk.FailRealm] == TFailRealm::N0) {
                        UNIT_ASSERT_C(!request->Get()->Record.GetIndexOnly(), "E=0 must verify bytes even without ReadBody");
                    }
                    readData |= !request->Get()->Record.GetIndexOnly();
                }
                group.OnVGet(*request->Get(), *reply->Get());
                if (indexError && request->Get()->Record.HasRangeQuery() && disk == sourceDisk) {
                    reply->Get()->Record.SetStatus(NKikimrProto::ERROR);
                }
                runtime.Send(reply.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            case TEvBlobStorage::EvVPut: {
                TEvBlobStorage::TEvVPut::TPtr request(static_cast<TEventHandle<TEvBlobStorage::TEvVPut>*>(event.Release()));
                const TVDiskID disk = group.GetVDiskID(*request->Get());
                UNIT_ASSERT(!env.Info->IsFailedFailRealm(disk.FailRealm));
                const NKikimrProto::EReplyStatus status = group.OnVPut(*request->Get());
                TEvBlobStorage::TEvVPutResult::TPtr reply = testState.CreateEventResultPtr(request, status, disk);
                runtime.Send(reply.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            case TEvBlobStorage::EvVGetBlock: {
                TEvBlobStorage::TEvVGetBlock::TPtr request(static_cast<TEventHandle<TEvBlobStorage::TEvVGetBlock>*>(event.Release()));
                const TVDiskID disk = VDiskIDFromVDiskID(request->Get()->Record.GetVDiskID());
                UNIT_ASSERT(!env.Info->IsFailedFailRealm(disk.FailRealm));
                TEvBlobStorage::TEvVGetBlockResult::TPtr reply = testState.CreateEventResultPtr(request, NKikimrProto::OK, tabletId);
                reply->Get()->Record.SetGeneration(7);
                runtime.Send(reply.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            default:
                return TTestActorRuntime::EEventAction::PROCESS;
        }
    });
    std::unique_ptr<TEvBlobStorage::TEvDiscover> discover = std::make_unique<TEvBlobStorage::TEvDiscover>(
        tabletId, 1, readBody, true, TInstant::Max(), 0, true);
    runtime.Send(new IEventHandle(env.RealProxyActorId, testState.EdgeActor, discover.release()), 0, true);
    const TEvBlobStorage::TEvDiscoverResult::TPtr result = testState.GrabEventPtr<TEvBlobStorage::TEvDiscoverResult>();
    const bool allFailed = std::all_of(states.begin(), states.end(), [](TFailRealm::EState state) { return state == TFailRealm::F; });
    // With E=0, an index-only candidate may be a lost blob; Get must report ERROR.
    const NKikimrProto::EReplyStatus expected = indexError || allFailed || ghost ? NKikimrProto::ERROR
        : hasBlob ? NKikimrProto::OK : NKikimrProto::NODATA;
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, expected);
    if (expected != NKikimrProto::ERROR) {
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->BlockedGeneration, 7);
    }
    if (expected == NKikimrProto::OK) {
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Id, blobId);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Buffer, readBody ? testState.BlobData : TString());
    }
    UNIT_ASSERT_VALUES_EQUAL(rangeRequests, 3 * std::count_if(states.begin(), states.end(),
        [](TFailRealm::EState state) { return state != TFailRealm::F; }));
    if (!indexError && (hasBlob || ghost)) {
        UNIT_ASSERT(readData || !readBody);
    }
    runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
}

Y_UNIT_TEST(Mirror3dcZeroEnsuredSurvivingCopy) {
    using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    for (ui32 realm = 0; realm < 3; ++realm) {
        TVector<TFailRealm::EState> states(3, TFailRealm::F);
        states[realm] = TFailRealm::N0;
        CheckMirror3dcSurvivingDiscovery(states, true, true);
        CheckMirror3dcSurvivingDiscovery(states, false, true);
    }
}

Y_UNIT_TEST(Mirror3dcZeroEnsuredEmptyAndFailedIndexes) {
    using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    CheckMirror3dcSurvivingDiscovery({TFailRealm::N0, TFailRealm::F, TFailRealm::F}, false, false);
    CheckMirror3dcSurvivingDiscovery({TFailRealm::N0, TFailRealm::F, TFailRealm::F}, true, true, true);
    CheckMirror3dcSurvivingDiscovery({TFailRealm::F, TFailRealm::F, TFailRealm::F}, false, false);
}

Y_UNIT_TEST(Mirror3dcZeroEnsuredRejectsIndexOnlyCandidates) {
    using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    CheckMirror3dcSurvivingDiscovery({TFailRealm::N0, TFailRealm::F, TFailRealm::F}, false, true, false, true);
    CheckMirror3dcSurvivingDiscovery({TFailRealm::N0, TFailRealm::F, TFailRealm::F}, false, false, false, true);
}

Y_UNIT_TEST(Mirror3dcSurvivingCopyWithRecoveringAndEnsuredRealms) {
    using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    CheckMirror3dcSurvivingDiscovery({TFailRealm::N0, TFailRealm::R, TFailRealm::F}, true, true);
    CheckMirror3dcSurvivingDiscovery({TFailRealm::N1, TFailRealm::F, TFailRealm::F}, true, true);
}

Y_UNIT_TEST(Mirror3dcSuccess) {
    TBlobStorageGroupType type(TErasureType::ErasureMirror3dc);
    TTestBasicRuntime runtime;
    SetupRuntime(runtime);
    TDSProxyEnv env;
    env.Configure(runtime, type, 0, 0, TBlobStorageGroupInfo::EEM_NONE);
    TTestState testState(runtime, env.Info);

    const ui64 tabletId = 72075186224047637;
    TLogoBlobID blobId1(tabletId, 1, 1, 0, testState.BlobSize, 0);
    TLogoBlobID blobId2(tabletId, 1, 2, 0, testState.BlobSize, 0);

    TGroupMock& groupMock = testState.GetGroupMock();
    groupMock.Put(blobId1, testState.BlobData);
    groupMock.Put(blobId2, testState.BlobData);

    auto discover = std::make_unique<TEvBlobStorage::TEvDiscover>(
        tabletId, 1, true, false, TInstant::Max(), 0, true);
    runtime.Send(new IEventHandle(env.RealProxyActorId, testState.EdgeActor, discover.release()), 0, true);

    testState.HandleVGetsWithMock(9);
    testState.HandleVGetsWithMock(9);
    testState.HandleVPutsWithMock(1);

    TEvBlobStorage::TEvDiscoverResult::TPtr ev = testState.GrabEventPtr<TEvBlobStorage::TEvDiscoverResult>();
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Id, blobId2);
}

Y_UNIT_TEST(Mirror3dcSuccessLastBlobMissingParts) {
    TBlobStorageGroupType type(TErasureType::ErasureMirror3dc);
    TTestBasicRuntime runtime;
    SetupRuntime(runtime);
    TDSProxyEnv env;
    env.Configure(runtime, type, 0, 0, TBlobStorageGroupInfo::EEM_NONE);
    TTestState testState(runtime, env.Info);

    const ui64 tabletId = 72075186224047637;
    TLogoBlobID blobId1(tabletId, 1, 1, 0, testState.BlobSize, 0);
    TLogoBlobID blobId2(tabletId, 1, 2, 0, testState.BlobSize, 0);

    TGroupMock& groupMock = testState.GetGroupMock();
    groupMock.Put(blobId1, testState.BlobData);
    THashSet<ui32> selectedParts{1};
    groupMock.Put(blobId2, testState.BlobData, 0, &selectedParts);

    auto discover = std::make_unique<TEvBlobStorage::TEvDiscover>(
        tabletId, 1, true, false, TInstant::Max(), 0, true);
    runtime.Send(new IEventHandle(env.RealProxyActorId, testState.EdgeActor, discover.release()), 0, true);

    testState.HandleVGetsWithMock(9);
    testState.HandleVGetsWithMock(9);
    testState.HandleVPutsWithMock(3);

    TEvBlobStorage::TEvDiscoverResult::TPtr ev = testState.GrabEventPtr<TEvBlobStorage::TEvDiscoverResult>();
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Id, blobId2);
}

Y_UNIT_TEST(Mirror3dcErrorWhenBlobIsLostAfterDiscover) {
    TBlobStorageGroupType type(TErasureType::ErasureMirror3dc);
    TTestBasicRuntime runtime;
    SetupRuntime(runtime);
    TDSProxyEnv env;
    env.Configure(runtime, type, 0, 0, TBlobStorageGroupInfo::EEM_NONE);
    TTestState testState(runtime, env.Info);

    const ui64 tabletId = 72075186224047637;
    TLogoBlobID blobId1(tabletId, 1, 1, 0, testState.BlobSize, 0);
    TLogoBlobID blobId2(tabletId, 1, 2, 0, testState.BlobSize, 0);

    TGroupMock& groupMock = testState.GetGroupMock();
    groupMock.Put(blobId1, testState.BlobData);
    groupMock.Put(blobId2, testState.BlobData);

    auto discover = std::make_unique<TEvBlobStorage::TEvDiscover>(
        tabletId, 1, true, false, TInstant::Max(), 0, true);
    runtime.Send(new IEventHandle(env.RealProxyActorId, testState.EdgeActor, discover.release()), 0, true);

    testState.HandleVGetsWithMock(9);
    testState.GroupMock.SetError(TVDiskID(0, 1, 0, 0, 0), NKikimrProto::ERROR);
    testState.GroupMock.SetError(TVDiskID(0, 1, 1, 0, 0), NKikimrProto::NOT_YET);
    testState.GroupMock.SetError(TVDiskID(0, 1, 2, 0, 0), NKikimrProto::NOT_YET);
    testState.HandleVGetsWithMock(9);

    TEvBlobStorage::TEvDiscoverResult::TPtr ev = testState.GrabEventPtr<TEvBlobStorage::TEvDiscoverResult>();
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::ERROR);
}

} // TDSProxyDiscover

} // NDSProxyDiscoverTest
} // NKikimr
