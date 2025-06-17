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
