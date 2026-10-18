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
