#include "defs.h"

#include "dsproxy_env_mock_ut.h"
#include "dsproxy_test_state_ut.h"
#include "dsproxy_vdisk_mock_ut.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>
#include <ydb/core/util/stlog.h>

#include <cstring>


namespace NKikimr {

namespace NDSProxyCountersTest {


void SetLogPriorities(TTestBasicRuntime &runtime) {
    bool IsVerbose = false;
    if (IsVerbose) {
        runtime.SetLogPriority(NKikimrServices::BS_PROXY_PATCH, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::BS_PROXY_GET, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::BS_PROXY_PUT, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NActorsServices::TEST, NLog::PRI_DEBUG);
    }
    runtime.SetLogPriority(NKikimrServices::BS_PROXY, NLog::PRI_CRIT);
    runtime.SetLogPriority(NKikimrServices::BS_QUEUE, NLog::PRI_CRIT);
}


Y_UNIT_TEST_SUITE(DSProxyCounters) {

Y_UNIT_TEST(PutGeneratedSubrequestBytes) {
    NKikimr::TBlobStorageGroupType erasure = TErasureType::Erasure4Plus2Block;
    TTestBasicRuntime runtime(1, false);
    SetLogPriorities(runtime);
    SetupRuntime(runtime);
    TDSProxyEnv env;
    env.Configure(runtime, erasure, 1, 0);
    TTestState testState(runtime, erasure, env.Info);

    TEvBlobStorage::TEvPut::ETactic tactic = TEvBlobStorage::TEvPut::TacticDefault;
    NKikimrBlobStorage::EPutHandleClass handleClass = NKikimrBlobStorage::TabletLog;


    TLogoBlobID blobId = TLogoBlobID(72075186224047637, 1, 863, 1, 254, 24576);
    ui32 requestBytes = blobId.BlobSize();

    TRequestMonItem &requestMonItem = env.StoragePoolCounters->GetItem(HandleClassToHandleClass(handleClass), requestBytes);
    UNIT_ASSERT_VALUES_EQUAL(requestMonItem.RequestBytes->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(requestMonItem.GeneratedSubrequests->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(requestMonItem.GeneratedSubrequestBytes->Val(), 0);

    TString buffer = TString::Uninitialized(blobId.BlobSize());
    for (char &ch : buffer) {
        ch = 'a';
    }
    TVector<TBlobTestSet::TBlob> blobs {
        TBlobTestSet::TBlob(blobId, buffer)
    };

    TEvBlobStorage::TEvPut::TPtr put = testState.CreatePutRequest(blobs[0], tactic, handleClass);
    runtime.Register(env.CreatePutRequestActor(put).release());

    TMap<TPartLocation, NKikimrProto::EReplyStatus> specialStatuses;
    for (ui64 part = 1; part <= env.Info->Type.TotalPartCount(); ++part) {
        TLogoBlobID partBlobId(blobId, part);
        TPartLocation id = testState.PrimaryVDiskForBlobPart(partBlobId);
        specialStatuses[id] = NKikimrProto::OK;
    }

    TGroupMock &groupMock = testState.GetGroupMock();
    groupMock.SetSpecialStatuses(specialStatuses);

    testState.HandleVPutsWithMock(env.Info->Type.TotalPartCount());

    TMap<TLogoBlobID, NKikimrProto::EReplyStatus> expectedStatus;
    expectedStatus[blobId] = NKikimrProto::OK;
    testState.ReceivePutResults(expectedStatus.size(), expectedStatus);

    UNIT_ASSERT_VALUES_EQUAL(requestMonItem.RequestBytes->Val(), 254);
    UNIT_ASSERT_VALUES_EQUAL(requestMonItem.GeneratedSubrequests->Val(), 6);
    UNIT_ASSERT_VALUES_EQUAL(requestMonItem.GeneratedSubrequestBytes->Val(), 64 * 6);
}

Y_UNIT_TEST(MultiPutGeneratedSubrequestBytes) {
    return; // KIKIMR-9016

    NKikimr::TBlobStorageGroupType erasure = TErasureType::Erasure4Plus2Block;
    TTestBasicRuntime runtime(1, false);
    SetLogPriorities(runtime);
    SetupRuntime(runtime);
    TDSProxyEnv env;
    env.Configure(runtime, erasure, 1, 0);
    TTestState testState(runtime, erasure, env.Info);

    TEvBlobStorage::TEvPut::ETactic tactic = TEvBlobStorage::TEvPut::TacticDefault;
    NKikimrBlobStorage::EPutHandleClass handleClass = NKikimrBlobStorage::TabletLog;


    TLogoBlobID blobId = TLogoBlobID(72075186224047637, 1, 863, 1, 254, 24576);
    ui32 requestBytes = blobId.BlobSize();

    TRequestMonItem &requestMonItem = env.StoragePoolCounters->GetItem(HandleClassToHandleClass(handleClass), requestBytes);
    UNIT_ASSERT_VALUES_EQUAL(requestMonItem.RequestBytes->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(requestMonItem.GeneratedSubrequests->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(requestMonItem.GeneratedSubrequestBytes->Val(), 0);

    TString buffer = TString::Uninitialized(blobId.BlobSize());
    for (char &ch : buffer) {
        ch = 'a';
    }
    TVector<TBlobTestSet::TBlob> blobs {
        TBlobTestSet::TBlob(blobId, buffer)
    };

    TBatchedVec<TEvBlobStorage::TEvPut::TPtr> batched;
    testState.CreatePutRequests(blobs, std::back_inserter(batched), tactic, handleClass);
    runtime.Register(env.CreatePutRequestActor(batched, tactic, handleClass).release());

    TMap<TPartLocation, NKikimrProto::EReplyStatus> specialStatuses;
    for (ui64 part = 1; part <= env.Info->Type.TotalPartCount(); ++part) {
        TLogoBlobID partBlobId(blobId, part);
        TPartLocation id = testState.PrimaryVDiskForBlobPart(partBlobId);
        specialStatuses[id] = NKikimrProto::OK;
    }

    TGroupMock &groupMock = testState.GetGroupMock();
    groupMock.SetSpecialStatuses(specialStatuses);

    testState.HandleVMultiPutsWithMock(env.Info->Type.TotalPartCount());

    TMap<TLogoBlobID, NKikimrProto::EReplyStatus> expectedStatus;
    expectedStatus[blobId] = NKikimrProto::OK;
    testState.ReceivePutResults(expectedStatus.size(), expectedStatus);

    UNIT_ASSERT_VALUES_EQUAL(requestMonItem.RequestBytes->Val(), 254);
    UNIT_ASSERT_VALUES_EQUAL(requestMonItem.GeneratedSubrequests->Val(), 6);
    UNIT_ASSERT_VALUES_EQUAL(requestMonItem.GeneratedSubrequestBytes->Val(), 64 * 6);
}


}


} // NDSProxyPatchTest

} // NKikimr
