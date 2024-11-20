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

Y_UNIT_TEST_SUITE(TDSProxyRequestReportningTest) {

Y_UNIT_TEST(CheckDefaultBehaviour) {
    TActorSystemStub actorSystemStub;
    TTestBasicRuntime runtime;
    SetupRuntime(runtime);

    // allow 1 in 60 seconds
    TControlWrapper bucketSize(1, 1, 100000);
    TControlWrapper leakDurationMs(60000, 1, 3600000);
    TControlWrapper leakRate(1, 1, 100000);
    TControlWrapper updatingDurationMs(60000, 1, 3600000);
    NActors::TActorId reportingThrottler = runtime.Register(CreateRequestReportingThrottler(bucketSize, leakDurationMs, leakRate, updatingDurationMs));
    runtime.EnableScheduleForActor(reportingThrottler);
    runtime.AdvanceCurrentTime(TDuration::MilliSeconds(10));
    runtime.SimulateSleep(TDuration::MilliSeconds(1));

    UNIT_ASSERT(AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(!AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(AllowToReport(NKikimrBlobStorage::EPutHandleClass::AsyncBlob));

    // 10 seconds after last update
    runtime.UpdateCurrentTime(runtime.GetCurrentTime() + TDuration::MilliSeconds(10000));
    runtime.SimulateSleep(TDuration::MilliSeconds(1));
    UNIT_ASSERT(!AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(!AllowToReport(NKikimrBlobStorage::EPutHandleClass::AsyncBlob));

    // 50 seconds after last update
    runtime.UpdateCurrentTime(runtime.GetCurrentTime() + TDuration::MilliSeconds(40000));
    runtime.SimulateSleep(TDuration::MilliSeconds(1));
    UNIT_ASSERT(!AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));

    // 61 seconds after last update
    runtime.UpdateCurrentTime(runtime.GetCurrentTime() + TDuration::MilliSeconds(21000));
    runtime.SimulateSleep(TDuration::MilliSeconds(1));
    UNIT_ASSERT(AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(AllowToReport(NKikimrBlobStorage::EPutHandleClass::AsyncBlob));

    // Check more than 1 request allowed for duration < 60000

    // update: + 1 in bucket
    runtime.UpdateCurrentTime(runtime.GetCurrentTime() + TDuration::MilliSeconds(60000));
    runtime.SimulateSleep(TDuration::MilliSeconds(1));

    // 1 seconds before update
    runtime.UpdateCurrentTime(runtime.GetCurrentTime() + TDuration::MilliSeconds(60000 - 1000));
    runtime.SimulateSleep(TDuration::MilliSeconds(1));
    UNIT_ASSERT(AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));

    // update
    runtime.UpdateCurrentTime(runtime.GetCurrentTime() + TDuration::MilliSeconds(1000));
    runtime.SimulateSleep(TDuration::MilliSeconds(1));

    // 1 seconds after update
    runtime.UpdateCurrentTime(runtime.GetCurrentTime() + TDuration::MilliSeconds(1000));
    runtime.SimulateSleep(TDuration::MilliSeconds(1));
    UNIT_ASSERT(AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));
}

Y_UNIT_TEST(CheckLeakyBucketBehaviour) {
    TActorSystemStub actorSystemStub;
    TTestBasicRuntime runtime;
    SetupRuntime(runtime);

    TControlWrapper bucketSize(3, 1, 100000);
    TControlWrapper leakDurationMs(60000, 1, 3600000);
    TControlWrapper leakRate(1, 1, 100000);
    TControlWrapper updatingDurationMs(1000, 1, 3600000);
    NActors::TActorId reportingThrottler = runtime.Register(CreateRequestReportingThrottler(bucketSize, leakDurationMs, leakRate, updatingDurationMs));
    runtime.EnableScheduleForActor(reportingThrottler);
    runtime.AdvanceCurrentTime(TDuration::MilliSeconds(10));
    runtime.SimulateSleep(TDuration::MilliSeconds(1));

    UNIT_ASSERT(AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(!AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));

    // 61 seconds after last update
    runtime.UpdateCurrentTime(runtime.GetCurrentTime() + TDuration::MilliSeconds(61000));
    runtime.SimulateSleep(TDuration::MilliSeconds(1));
    UNIT_ASSERT(AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(!AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));

    // 121 seconds after last update
    runtime.UpdateCurrentTime(runtime.GetCurrentTime() + TDuration::MilliSeconds(121000));
    runtime.SimulateSleep(TDuration::MilliSeconds(1));
    UNIT_ASSERT(AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(!AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));

    // 181 seconds after last update
    runtime.UpdateCurrentTime(runtime.GetCurrentTime() + TDuration::MilliSeconds(181000));
    runtime.SimulateSleep(TDuration::MilliSeconds(1));
    UNIT_ASSERT(AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(!AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));

    // Check no more than 3 request allowed for duration < 60000

    // update: + 3 in bucket
    runtime.UpdateCurrentTime(runtime.GetCurrentTime() + TDuration::MilliSeconds(60000 * 3));
    runtime.SimulateSleep(TDuration::MilliSeconds(1));

    // 1 seconds before update
    runtime.UpdateCurrentTime(runtime.GetCurrentTime() + TDuration::MilliSeconds(60000 - 1000));
    runtime.SimulateSleep(TDuration::MilliSeconds(1));
    UNIT_ASSERT(AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));
    UNIT_ASSERT(AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));

    // update
    runtime.UpdateCurrentTime(runtime.GetCurrentTime() + TDuration::MilliSeconds(1000));
    runtime.SimulateSleep(TDuration::MilliSeconds(1));

    // 1 seconds after update
    runtime.UpdateCurrentTime(runtime.GetCurrentTime() + TDuration::MilliSeconds(1000));
    runtime.SimulateSleep(TDuration::MilliSeconds(1));
    UNIT_ASSERT(!AllowToReport(NKikimrBlobStorage::EPutHandleClass::TabletLog));
}

} // Y_UNIT_TEST_SUITE TDSProxyRequestReportningTest
} // namespace NDSProxyRequestReportningTest
} // namespace NKikimr
