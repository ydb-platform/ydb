#include "dsproxy_env_mock_ut.h"
#include "dsproxy_test_state_ut.h"

#include <ydb/core/blobstorage/dsproxy/dsproxy_request_reporting.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/common/env.h>

namespace NKikimr {
namespace NDSProxyRequestReportningTest {

void SimulateSleep(TTestBasicRuntime& runtime, TDuration duration) {
    runtime.AdvanceCurrentTime(duration);
    runtime.SimulateSleep(TDuration::MilliSeconds(1));
}

Y_UNIT_TEST_SUITE(TDSProxyRequestReportningTest) {

Y_UNIT_TEST(CheckDefaultBehaviour) {
    TActorSystemStub actorSystemStub;
    TTestBasicRuntime runtime;
    SetupRuntime(runtime);

    // allow 1 in 60 seconds
    TControlWrapper bucketSize(1, 1, 100000);
    TControlWrapper leakDurationMs(60000, 1, 3600000);
    TControlWrapper leakRate(1, 1, 100000);
    NActors::TActorId reportingThrottler = runtime.Register(CreateRequestReportingThrottler(bucketSize, leakDurationMs, leakRate));
    runtime.EnableScheduleForActor(reportingThrottler);
    SimulateSleep(runtime, TDuration::MilliSeconds(10));

    UNIT_ASSERT(PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(!PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(PopAllowToken(NKikimrBlobStorage::EPutHandleClass::AsyncBlob));

    // 10 seconds after last update
    SimulateSleep(runtime, TDuration::Seconds(10));
    UNIT_ASSERT(!PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(!PopAllowToken(NKikimrBlobStorage::EPutHandleClass::AsyncBlob));

    // 50 seconds after last update
    SimulateSleep(runtime, TDuration::Seconds(40));
    UNIT_ASSERT(!PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));

    // 61 seconds after last update
    SimulateSleep(runtime, TDuration::Seconds(21));
    UNIT_ASSERT(PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(PopAllowToken(NKikimrBlobStorage::EPutHandleClass::AsyncBlob));

    // Check more than 1 request allowed for duration < 60000

    // update: + 1 in bucket
    SimulateSleep(runtime, TDuration::Seconds(60));

    // 1 seconds before update
    SimulateSleep(runtime, TDuration::Seconds(59));
    UNIT_ASSERT(PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));

    // update
    SimulateSleep(runtime, TDuration::Seconds(1));

    // 1 seconds after update
    SimulateSleep(runtime, TDuration::Seconds(1));
    UNIT_ASSERT(PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));
}

Y_UNIT_TEST(CheckLeakyBucketBehaviour) {
    TActorSystemStub actorSystemStub;
    TTestBasicRuntime runtime;
    SetupRuntime(runtime);

    TControlWrapper bucketSize(3, 1, 100000);
    TControlWrapper leakDurationMs(60000, 1, 3600000);
    TControlWrapper leakRate(1, 1, 100000);
    NActors::TActorId reportingThrottler = runtime.Register(CreateRequestReportingThrottler(bucketSize, leakDurationMs, leakRate));
    runtime.EnableScheduleForActor(reportingThrottler);
    SimulateSleep(runtime, TDuration::MilliSeconds(10));

    UNIT_ASSERT(PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(!PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));

    // 61 seconds after last update
    SimulateSleep(runtime, TDuration::Seconds(61));
    UNIT_ASSERT(PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(!PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));

    // 121 seconds after last update
    SimulateSleep(runtime, TDuration::Seconds(121));
    UNIT_ASSERT(PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(!PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));

    // 181 seconds after last update
    SimulateSleep(runtime, TDuration::Seconds(181));
    UNIT_ASSERT(PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(!PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));

    // Check no more than 3 request allowed for duration < 60000

    // update: + 3 in bucket
    SimulateSleep(runtime, TDuration::Seconds(180));

    // 1 seconds before update
    SimulateSleep(runtime, TDuration::Seconds(59));
    UNIT_ASSERT(PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));

    // update
    SimulateSleep(runtime, TDuration::Seconds(1));

    // 1 seconds after update
    SimulateSleep(runtime, TDuration::Seconds(1));
    UNIT_ASSERT(PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(!PopAllowToken(NKikimrBlobStorage::EPutHandleClass::TabletLog));
}

} // Y_UNIT_TEST_SUITE TDSProxyRequestReportningTest
} // namespace NDSProxyRequestReportningTest
} // namespace NKikimr
