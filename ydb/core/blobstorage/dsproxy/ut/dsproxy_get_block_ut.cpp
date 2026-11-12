#include "defs.h"
#include "dsproxy_env_mock_ut.h"
#include "dsproxy_test_state_ut.h"

#include <ydb/core/blobstorage/dsproxy/dsproxy_quorum_tracker.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_mirror3dc_state.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/actor_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

using TDataCenterState = NKikimrBlobStorage::TDataCenterState;

using namespace NKikimr;

namespace {

void TestQuorumResponse(TBlobStorageGroupType::EErasureSpecies erasure) {
    TTestBasicRuntime runtime(1, false);
    runtime.SetDispatchTimeout(TDuration::Seconds(1));
    SetupRuntime(runtime);

    TDSProxyEnv env;
    env.Configure(runtime, TBlobStorageGroupType(erasure), 0, 0);
    TTestState testState(runtime, env.Info);

    const ui64 tabletId = 123456;

    auto ev = std::make_unique<TEvBlobStorage::TEvGetBlock>(tabletId, TInstant::Max());
    runtime.Send(new IEventHandle(env.RealProxyActorId, testState.EdgeActor, ev.release()), 0);

    TVector<TEvBlobStorage::TEvVGetBlock::TPtr> requests;
    for (ui32 i = 0; i < env.Info->GetTotalVDisksNum(); ++i) {
        TEvBlobStorage::TEvVGetBlock::TPtr vgetBlock = testState.GrabEventPtr<TEvBlobStorage::TEvVGetBlock>();
        UNIT_ASSERT(vgetBlock);
        requests.push_back(vgetBlock);
    }

    TGroupQuorumTracker tracker(env.Info.Get());
    ui32 responsesNeeded = 0;

    for (auto& req : requests) {
        ++responsesNeeded;
        const TVDiskID vdisk = VDiskIDFromVDiskID(req->Get()->Record.GetVDiskID());

        auto response = testState.CreateEventResultPtr(req, NKikimrProto::OK, tabletId);
        runtime.Send(response.Release(), 0);

        NKikimrProto::EReplyStatus status = tracker.ProcessReply(vdisk, NKikimrProto::OK);

        if (status == NKikimrProto::OK) {
            TAutoPtr<IEventHandle> resultHandle;
            auto result = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvGetBlockResult>(resultHandle);
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(result->TabletId, tabletId);

            UNIT_ASSERT_C(responsesNeeded < requests.size(),
                "Expected to respond after " << responsesNeeded << " responses, but got " << requests.size() << " disks");
            return;
        }
    }

    UNIT_FAIL("Failed to reach quorum");
}

void TestFailModelViolation(TBlobStorageGroupType::EErasureSpecies erasure) {
    TTestBasicRuntime runtime(1, false);
    runtime.SetDispatchTimeout(TDuration::Seconds(1));
    SetupRuntime(runtime);

    TDSProxyEnv env;
    env.Configure(runtime, TBlobStorageGroupType(erasure), 0, 0);
    TTestState testState(runtime, env.Info);

    const ui64 tabletId = 123456;

    auto ev = std::make_unique<TEvBlobStorage::TEvGetBlock>(tabletId, TInstant::Max());
    runtime.Send(new IEventHandle(env.RealProxyActorId, testState.EdgeActor, ev.release()), 0);

    TVector<TEvBlobStorage::TEvVGetBlock::TPtr> requests;
    for (ui32 i = 0; i < env.Info->GetTotalVDisksNum(); ++i) {
        TEvBlobStorage::TEvVGetBlock::TPtr vgetBlock = testState.GrabEventPtr<TEvBlobStorage::TEvVGetBlock>();
        UNIT_ASSERT(vgetBlock);
        requests.push_back(vgetBlock);
    }

    TGroupQuorumTracker tracker(env.Info.Get());
    bool gotError = false;

    bool isMirror3dc = (erasure == TBlobStorageGroupType::ErasureMirror3dc);

    for (ui32 i = 0; i < requests.size(); ++i) {
        const TVDiskID vdisk = VDiskIDFromVDiskID(requests[i]->Get()->Record.GetVDiskID());

        NKikimrProto::EReplyStatus diskStatus;
        if (isMirror3dc) {
            diskStatus = (vdisk.FailDomain == 1) ? NKikimrProto::ERROR : NKikimrProto::OK;
        } else {
            ui32 maxFailures = env.Info->Type.TotalPartCount() - env.Info->Type.MinimalRestorablePartCount();
            diskStatus = (i < maxFailures + 1) ? NKikimrProto::ERROR : NKikimrProto::OK;
        }

        auto response = testState.CreateEventResultPtr(requests[i], diskStatus, tabletId);
        runtime.Send(response.Release(), 0);

        NKikimrProto::EReplyStatus status = tracker.ProcessReply(vdisk, diskStatus);

        if (status == NKikimrProto::ERROR) {
            TAutoPtr<IEventHandle> resultHandle;
            auto result = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvGetBlockResult>(resultHandle);
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Status, NKikimrProto::ERROR);
            gotError = true;
            break;
        }
    }

    UNIT_ASSERT_C(gotError, "Expected ERROR when fail model is violated");
}

void TestMaxAllowedFailures(TBlobStorageGroupType::EErasureSpecies erasure) {
    TTestBasicRuntime runtime(1, false);
    runtime.SetDispatchTimeout(TDuration::Seconds(1));
    SetupRuntime(runtime);

    TDSProxyEnv env;
    env.Configure(runtime, TBlobStorageGroupType(erasure), 0, 0);
    TTestState testState(runtime, env.Info);

    const ui64 tabletId = 123456;

    auto ev = std::make_unique<TEvBlobStorage::TEvGetBlock>(tabletId, TInstant::Max());
    runtime.Send(new IEventHandle(env.RealProxyActorId, testState.EdgeActor, ev.release()), 0);

    TVector<TEvBlobStorage::TEvVGetBlock::TPtr> requests;
    for (ui32 i = 0; i < env.Info->GetTotalVDisksNum(); ++i) {
        TEvBlobStorage::TEvVGetBlock::TPtr vgetBlock = testState.GrabEventPtr<TEvBlobStorage::TEvVGetBlock>();
        UNIT_ASSERT(vgetBlock);
        requests.push_back(vgetBlock);
    }

    ui32 maxFailures = env.Info->Type.TotalPartCount() - env.Info->Type.MinimalRestorablePartCount();
    TGroupQuorumTracker tracker(env.Info.Get());

    bool isMirror3dc = (erasure == TBlobStorageGroupType::ErasureMirror3dc);

    for (ui32 i = 0; i < requests.size(); ++i) {
        const TVDiskID vdisk = VDiskIDFromVDiskID(requests[i]->Get()->Record.GetVDiskID());

        NKikimrProto::EReplyStatus diskStatus;
        if (isMirror3dc) {
            diskStatus = ((vdisk.FailRealm == 0 && vdisk.FailDomain >= 1) ||
                         (vdisk.FailRealm == 1 && vdisk.FailDomain == 1))
                ? NKikimrProto::ERROR : NKikimrProto::OK;
        } else {
            diskStatus = (i < maxFailures) ? NKikimrProto::ERROR : NKikimrProto::OK;
        }

        auto response = testState.CreateEventResultPtr(requests[i], diskStatus, tabletId);
        runtime.Send(response.Release(), 0);

        NKikimrProto::EReplyStatus status = tracker.ProcessReply(vdisk, diskStatus);

        if (status == NKikimrProto::OK) {
            TAutoPtr<IEventHandle> resultHandle;
            auto result = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvGetBlockResult>(resultHandle);
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Status, NKikimrProto::OK);
            return;
        }
    }

    UNIT_FAIL("Failed to get OK response with max allowed failures");
}

} // namespace

namespace {

void CheckMirror3dcRealms(const TMirror3dcGroupState& realms,
        bool failNormalDisk, NKikimrProto::EReplyStatus expected) {
    TTestBasicRuntime runtime(1, false);
    runtime.SetDispatchTimeout(TDuration::Seconds(10));
    SetupRuntime(runtime);
    TDSProxyEnv env;
    env.Configure(runtime, TBlobStorageGroupType(TErasureType::ErasureMirror3dc), 0, 0,
        TBlobStorageGroupInfo::EEM_NONE, DormantTimeoutDefaultControl, false, TVector<EMirror3dcFailRealmState>(realms.FailRealmStates.begin(), realms.FailRealmStates.end()), realms.State);
    TTestState state(runtime, env.Info);
    const ui64 tabletId = 123456;
    auto query = std::make_unique<TEvBlobStorage::TEvGetBlock>(tabletId, TInstant::Max());
    runtime.Send(new IEventHandle(env.RealProxyActorId, state.EdgeActor, query.release()), 0);
    ui32 requests = 0;
    for (;;) {
        TAutoPtr<IEventHandle> handle;
        auto [getBlock, result] = runtime.GrabEdgeEventsRethrow<TEvBlobStorage::TEvVGetBlock,
            TEvBlobStorage::TEvGetBlockResult>(handle);
        if (result) {
            UNIT_ASSERT_VALUES_EQUAL(result->Status, expected);
            UNIT_ASSERT(requests > 0);
            break;
        }
        UNIT_ASSERT(getBlock);
        ++requests;
        const auto vd = VDiskIDFromVDiskID(getBlock->Record.GetVDiskID());
        UNIT_ASSERT(realms.FailRealmStates[vd.FailRealm] != TDataCenterState::EXCLUDED);
        const bool error = realms.FailRealmStates[vd.FailRealm] == TDataCenterState::RECOVERING || (failNormalDisk && vd.FailDomain == 0);
        TEvBlobStorage::TEvVGetBlock::TPtr request(
            static_cast<TEventHandle<TEvBlobStorage::TEvVGetBlock>*>(handle.Release()));
        auto response = state.CreateEventResultPtr(request, error ? NKikimrProto::ERROR : NKikimrProto::OK, tabletId);
        runtime.Send(response.Release(), 0);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TDSProxyGetBlockQuorumTest) {
    Y_UNIT_TEST(Mirror3dcSkipsFailedRealmsAndIgnoresRecoveryErrors) {
        CheckMirror3dcRealms({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, false, NKikimrProto::OK);
        CheckMirror3dcRealms({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2, {TDataCenterState::EXCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, true, NKikimrProto::OK);
        CheckMirror3dcRealms({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED, TDataCenterState::INCLUDED}}, true, NKikimrProto::ERROR);
    }

#define UNIT_TEST_QUORUM_RESPONSE(ERASURE) \
    Y_UNIT_TEST(QuorumResponse##ERASURE) { \
        TestQuorumResponse(TBlobStorageGroupType::ERASURE); \
    }

UNIT_TEST_QUORUM_RESPONSE(Erasure4Plus2Block)
UNIT_TEST_QUORUM_RESPONSE(ErasureMirror3dc)

#undef UNIT_TEST_QUORUM_RESPONSE

#define UNIT_TEST_FAIL_MODEL(ERASURE) \
    Y_UNIT_TEST(FailModelViolation##ERASURE) { \
        TestFailModelViolation(TBlobStorageGroupType::ERASURE); \
    }

UNIT_TEST_FAIL_MODEL(Erasure4Plus2Block)
UNIT_TEST_FAIL_MODEL(ErasureMirror3dc)

#undef UNIT_TEST_FAIL_MODEL

#define UNIT_TEST_MAX_FAILURES(ERASURE) \
    Y_UNIT_TEST(MaxAllowedFailures##ERASURE) { \
        TestMaxAllowedFailures(TBlobStorageGroupType::ERASURE); \
    }

UNIT_TEST_MAX_FAILURES(Erasure4Plus2Block)
UNIT_TEST_MAX_FAILURES(ErasureMirror3dc)

#undef UNIT_TEST_MAX_FAILURES

}
