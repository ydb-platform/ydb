#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/graph/api/service.h>
#include <ydb/core/graph/api/events.h>

#ifdef NDEBUG
#define Ctest Cnull
#else
#define Ctest Cerr
#endif

namespace NKikimr {

using namespace Tests;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(Graph) {
    TTenantTestConfig GetTenantTestConfig() {
        return {
            .Domains = {
                {
                    .Name = DOMAIN1_NAME,
                    .SchemeShardId = SCHEME_SHARD1_ID,
                    .Subdomains = {TENANT1_1_NAME, TENANT1_2_NAME}
                }
            },
            .HiveId = HIVE_ID,
            .FakeTenantSlotBroker = true,
            .FakeSchemeShard = true,
            .CreateConsole = false,
            .Nodes = {
                {
                    .TenantPoolConfig = {
                        .StaticSlots = {
                            {
                                .Tenant = DOMAIN1_NAME,
                                .Limit = {
                                    .CPU = 1,
                                    .Memory = 1,
                                    .Network = 1
                                }
                            }
                        },
                        .NodeType = "node-type"
                    }
                }
            },
            .DataCenterCount = 1
        };
    }

    Y_UNIT_TEST(CreateGraphShard) {
        TTestBasicRuntime runtime;

        runtime.SetLogPriority(NKikimrServices::GRAPH, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::HIVE, NLog::PRI_TRACE);

        TTestEnv env(runtime);
        ui64 txId = 100;
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "db1"
        )");

        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "db1"
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            StoragePools {
                Name: "pool-1"
                Kind: "hdd"
            }
            ExternalSchemeShard: true
            ExternalHive: true
            GraphShard: true
        )");

        env.TestWaitNotification(runtime, txId);

        auto result = DescribePath(runtime, "/MyRoot/db1");
        UNIT_ASSERT(result.GetPathDescription().GetDomainDescription().GetProcessingParams().GetGraphShard() != 0);
    }

    Y_UNIT_TEST(UseGraphShard) {
        TTestBasicRuntime runtime;

        runtime.SetLogPriority(NKikimrServices::GRAPH, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::HIVE, NLog::PRI_TRACE);

        TTestEnv::ENABLE_SCHEMESHARD_LOG = false;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "db1"
        )");

        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "db1"
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            StoragePools {
                Name: "pool-1"
                Kind: "hdd"
            }
            ExternalSchemeShard: true
            ExternalHive: true
            GraphShard: true
        )");

        env.TestWaitNotification(runtime, txId);

        NKikimrScheme::TEvDescribeSchemeResult result = DescribePath(runtime, "/MyRoot/db1");
        UNIT_ASSERT(result.GetPathDescription().GetDomainDescription().GetProcessingParams().GetGraphShard() != 0);

        IActor* service = NGraph::CreateGraphService("/MyRoot/db1");
        TActorId serviceId = runtime.Register(service);
        runtime.RegisterService(NGraph::MakeGraphServiceId(), serviceId);
        TActorId sender = runtime.AllocateEdgeActor();

        // this call is needed to wait for establishing of pipe connection
        {
            NGraph::TEvGraph::TEvGetMetrics* event = new NGraph::TEvGraph::TEvGetMetrics();
            event->Record.AddMetrics("test.metric1");
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            TAutoPtr<IEventHandle> handle;
            NGraph::TEvGraph::TEvMetricsResult* response = runtime.GrabEdgeEventRethrow<NGraph::TEvGraph::TEvMetricsResult>(handle);
            Ctest << "Received result: " << response->Record.ShortDebugString() << Endl;
        }

        runtime.SimulateSleep(TDuration::Seconds(1));

        {
            NGraph::TEvGraph::TEvSendMetrics* event = new NGraph::TEvGraph::TEvSendMetrics();
            event->AddMetric("test.metric1", 13);
            event->AddHistogramMetric("test.metric2", {10, 100, 1000}, {5, 5, 5});
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
        }

        runtime.SimulateSleep(TDuration::Seconds(1));

        {
            NGraph::TEvGraph::TEvSendMetrics* event = new NGraph::TEvGraph::TEvSendMetrics();
            event->AddMetric("test.metric1", 14);
            event->AddHistogramMetric("test.metric2", {10, 100, 1000}, {2, 10, 3});
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
        }

        runtime.SimulateSleep(TDuration::Seconds(1));

        {
            NGraph::TEvGraph::TEvSendMetrics* event = new NGraph::TEvGraph::TEvSendMetrics();
            event->AddMetric("test.metric1", 15);
            event->AddHistogramMetric("test.metric2", {10, 100, 1000}, {1, 13, 1});
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
        }

        {
            NGraph::TEvGraph::TEvGetMetrics* event = new NGraph::TEvGraph::TEvGetMetrics();
            event->Record.AddMetrics("test.metric1");
            event->Record.AddMetrics("test.metric2.p50");
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            TAutoPtr<IEventHandle> handle;
            NGraph::TEvGraph::TEvMetricsResult* response = runtime.GrabEdgeEventRethrow<NGraph::TEvGraph::TEvMetricsResult>(handle);
            Ctest << "Received result: " << response->Record.ShortDebugString() << Endl;
            UNIT_ASSERT(response->Record.DataSize() > 0);
            UNIT_ASSERT(response->Record.GetData(0).ShortDebugString() == "Values: 13 Values: 14");
            UNIT_ASSERT(response->Record.GetData(1).ShortDebugString() == "Values: 46 Values: 55");
        }
    }

    Y_UNIT_TEST(MemoryBackendFullCycle) {
        TTestBasicRuntime runtime;

        runtime.SetLogPriority(NKikimrServices::GRAPH, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::HIVE, NLog::PRI_TRACE);

        TTestEnv::ENABLE_SCHEMESHARD_LOG = false;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "db1"
        )");

        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "db1"
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            StoragePools {
                Name: "pool-1"
                Kind: "hdd"
            }
            ExternalSchemeShard: true
            ExternalHive: true
            GraphShard: true
        )");

        env.TestWaitNotification(runtime, txId);

        NKikimrScheme::TEvDescribeSchemeResult result = DescribePath(runtime, "/MyRoot/db1");
        UNIT_ASSERT(result.GetPathDescription().GetDomainDescription().GetProcessingParams().GetGraphShard() != 0);

        IActor* service = NGraph::CreateGraphService("/MyRoot/db1");
        TActorId serviceId = runtime.Register(service);
        runtime.RegisterService(NGraph::MakeGraphServiceId(), serviceId);
        TActorId sender = runtime.AllocateEdgeActor();

        Ctest << "Preparing..." << Endl;
        // this call is needed to wait for establishing of pipe connection
        {
            NGraph::TEvGraph::TEvGetMetrics* event = new NGraph::TEvGraph::TEvGetMetrics();
            event->Record.AddMetrics("test.metric1");
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            TAutoPtr<IEventHandle> handle;
            NGraph::TEvGraph::TEvMetricsResult* response = runtime.GrabEdgeEventRethrow<NGraph::TEvGraph::TEvMetricsResult>(handle);
            Ctest << "Received result: " << response->Record.ShortDebugString() << Endl;
        }

        runtime.SimulateSleep(TDuration::Seconds(1));

        Ctest << "Filling..." << Endl;
        for (int minutes = 0; minutes < 10; ++minutes) {
            for (int seconds = 0; seconds < 60; ++seconds) {
                {
                    NGraph::TEvGraph::TEvSendMetrics* event = new NGraph::TEvGraph::TEvSendMetrics();
                    NKikimrGraph::TMetric* metric = event->Record.AddMetrics();
                    metric->SetName("test.metric1");
                    metric->SetValue(seconds);
                    event->Record.SetTime(60 * minutes + seconds);
                    runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
                }
            }
        }

        Ctest << "Checking..." << Endl;
        {
            NGraph::TEvGraph::TEvGetMetrics* event = new NGraph::TEvGraph::TEvGetMetrics();
            event->Record.AddMetrics("test.metric1");
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            TAutoPtr<IEventHandle> handle;
            NGraph::TEvGraph::TEvMetricsResult* response = runtime.GrabEdgeEventRethrow<NGraph::TEvGraph::TEvMetricsResult>(handle);
            Ctest << "Received result: " << response->Record.ShortDebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(response->Record.TimeSize(), 10 * 60);
        }
        runtime.AdvanceCurrentTime(TDuration::Seconds(10 * 60));

        Ctest << "Triggering aggregation..." << Endl;
        for (int seconds = 0; seconds < 20; ++seconds) {
            {
                NGraph::TEvGraph::TEvSendMetrics* event = new NGraph::TEvGraph::TEvSendMetrics();
                NKikimrGraph::TMetric* metric = event->Record.AddMetrics();
                metric->SetName("test.metric1");
                metric->SetValue(seconds);
                runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            }
            runtime.SimulateSleep(TDuration::Seconds(1));
        }

        Ctest << "Checking..." << Endl;
        {
            NGraph::TEvGraph::TEvGetMetrics* event = new NGraph::TEvGraph::TEvGetMetrics();
            event->Record.AddMetrics("test.metric1");
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            TAutoPtr<IEventHandle> handle;
            NGraph::TEvGraph::TEvMetricsResult* response = runtime.GrabEdgeEventRethrow<NGraph::TEvGraph::TEvMetricsResult>(handle);
            Ctest << "Received result: " << response->Record.ShortDebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(response->Record.TimeSize(), 25);
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetTime(0), 559);
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetTime(1), 569);
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetTime(2), 574);
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetTime(3), 579);
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetTime(4), 584);
            UNIT_ASSERT(abs(response->Record.GetData(0).GetValues(0) - 14.5) < 0.1);
            UNIT_ASSERT(abs(response->Record.GetData(0).GetValues(1) - 24.7) < 0.1);
            UNIT_ASSERT(abs(response->Record.GetData(0).GetValues(2) - 32) < 0.1);
        }
    }

    Y_UNIT_TEST(LocalBackendFullCycle) {
        TTestBasicRuntime runtime;

        runtime.SetLogPriority(NKikimrServices::GRAPH, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::HIVE, NLog::PRI_TRACE);

        TTestEnv::ENABLE_SCHEMESHARD_LOG = false;
        TTestEnv env(runtime, {
            .GraphBackendType_ = "Local"
        });
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "db1"
        )");

        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "db1"
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            StoragePools {
                Name: "pool-1"
                Kind: "hdd"
            }
            ExternalSchemeShard: true
            ExternalHive: true
            GraphShard: true
        )");

        env.TestWaitNotification(runtime, txId);

        NKikimrScheme::TEvDescribeSchemeResult result = DescribePath(runtime, "/MyRoot/db1");
        UNIT_ASSERT(result.GetPathDescription().GetDomainDescription().GetProcessingParams().GetGraphShard() != 0);

        IActor* service = NGraph::CreateGraphService("/MyRoot/db1");
        TActorId serviceId = runtime.Register(service);
        runtime.RegisterService(NGraph::MakeGraphServiceId(), serviceId);
        TActorId sender = runtime.AllocateEdgeActor();

        Ctest << "Preparing..." << Endl;
        // this call is needed to wait for establishing of pipe connection
        {
            NGraph::TEvGraph::TEvGetMetrics* event = new NGraph::TEvGraph::TEvGetMetrics();
            event->Record.AddMetrics("test.metric9");
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            TAutoPtr<IEventHandle> handle;
            NGraph::TEvGraph::TEvMetricsResult* response = runtime.GrabEdgeEventRethrow<NGraph::TEvGraph::TEvMetricsResult>(handle);
            Ctest << "Received result: " << response->Record.ShortDebugString() << Endl;
        }

        Ctest << "Send old metrics..." << Endl;

        {
            NGraph::TEvGraph::TEvSendMetrics* event = new NGraph::TEvGraph::TEvSendMetrics();
            {
                NKikimrGraph::TMetric* metric = event->Record.AddMetrics();
                metric->SetName("test.metric0");
                metric->SetValue(13);
            }
            {
                NKikimrGraph::TMetric* metric = event->Record.AddMetrics();
                metric->SetName("test.metric1");
                metric->SetValue(14);
            }
            event->Record.SetTime(0);
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
        }

        runtime.SimulateSleep(TDuration::Seconds(1));

        Ctest << "Filling..." << Endl;
        for (int minutes = 0; minutes < 2; ++minutes) {
            for (int seconds = 0; seconds < 60; ++seconds) {
                {
                    NGraph::TEvGraph::TEvSendMetrics* event = new NGraph::TEvGraph::TEvSendMetrics();
                    NKikimrGraph::TMetric* metric = event->Record.AddMetrics();
                    metric->SetName("test.metric9");
                    metric->SetValue(seconds);
                    event->Record.SetTime(60 * minutes + seconds);
                    runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
                }
            }
        }

        Ctest << "Checking..." << Endl;
        {
            NGraph::TEvGraph::TEvGetMetrics* event = new NGraph::TEvGraph::TEvGetMetrics();
            event->Record.AddMetrics("test.metric9");
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            TAutoPtr<IEventHandle> handle;
            NGraph::TEvGraph::TEvMetricsResult* response = runtime.GrabEdgeEventRethrow<NGraph::TEvGraph::TEvMetricsResult>(handle);
            UNIT_ASSERT_VALUES_EQUAL(response->Record.TimeSize(), 2 * 60);
        }
        runtime.AdvanceCurrentTime(TDuration::Seconds(2 * 60));

        Ctest << "Triggering aggregation..." << Endl;
        for (int seconds = 0; seconds < 20; ++seconds) {
            {
                NGraph::TEvGraph::TEvSendMetrics* event = new NGraph::TEvGraph::TEvSendMetrics();
                NKikimrGraph::TMetric* metric = event->Record.AddMetrics();
                metric->SetName("test.metric9");
                metric->SetValue(seconds);
                runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            }
            runtime.SimulateSleep(TDuration::Seconds(1));
        }

        Ctest << "Checking..." << Endl;
        {
            NGraph::TEvGraph::TEvGetMetrics* event = new NGraph::TEvGraph::TEvGetMetrics();
            event->Record.AddMetrics("test.metric9");
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            TAutoPtr<IEventHandle> handle;
            NGraph::TEvGraph::TEvMetricsResult* response = runtime.GrabEdgeEventRethrow<NGraph::TEvGraph::TEvMetricsResult>(handle);
            Ctest << response->Record.ShortDebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(response->Record.TimeSize(), 20);
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetTime(0), 79);
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetTime(1), 89);
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetTime(2), 94);
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetTime(3), 99);
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetTime(4), 104);
            UNIT_ASSERT(abs(response->Record.GetData(0).GetValues(0) - 14.5) < 0.1);
            UNIT_ASSERT(abs(response->Record.GetData(0).GetValues(1) - 24.5) < 0.1);
            UNIT_ASSERT(abs(response->Record.GetData(0).GetValues(2) - 32) < 0.1);
        }
    }

    Y_UNIT_TEST(MemoryBordersOnGet) {
        TTestBasicRuntime runtime;

        runtime.SetLogPriority(NKikimrServices::GRAPH, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::HIVE, NLog::PRI_TRACE);

        TTestEnv::ENABLE_SCHEMESHARD_LOG = false;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "db1"
        )");

        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "db1"
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            StoragePools {
                Name: "pool-1"
                Kind: "hdd"
            }
            ExternalSchemeShard: true
            ExternalHive: true
            GraphShard: true
        )");

        env.TestWaitNotification(runtime, txId);

        NKikimrScheme::TEvDescribeSchemeResult result = DescribePath(runtime, "/MyRoot/db1");
        UNIT_ASSERT(result.GetPathDescription().GetDomainDescription().GetProcessingParams().GetGraphShard() != 0);

        IActor* service = NGraph::CreateGraphService("/MyRoot/db1");
        TActorId serviceId = runtime.Register(service);
        runtime.RegisterService(NGraph::MakeGraphServiceId(), serviceId);
        TActorId sender = runtime.AllocateEdgeActor();

        Ctest << "Preparing..." << Endl;
        // this call is needed to wait for establishing of pipe connection
        {
            NGraph::TEvGraph::TEvGetMetrics* event = new NGraph::TEvGraph::TEvGetMetrics();
            event->Record.AddMetrics("test.metric0");
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            TAutoPtr<IEventHandle> handle;
            NGraph::TEvGraph::TEvMetricsResult* response = runtime.GrabEdgeEventRethrow<NGraph::TEvGraph::TEvMetricsResult>(handle);
            Ctest << "Received result: " << response->Record.ShortDebugString() << Endl;
        }

        runtime.SimulateSleep(TDuration::Seconds(1));

        Ctest << "Filling..." << Endl;
        for (int seconds = 40; seconds < 60; ++seconds) {
            {
                NGraph::TEvGraph::TEvSendMetrics* event = new NGraph::TEvGraph::TEvSendMetrics();
                NKikimrGraph::TMetric* metric = event->Record.AddMetrics();
                metric->SetName("test.metric0");
                metric->SetValue(seconds);
                event->Record.SetTime(seconds);
                runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            }
        }

        Ctest << "Checking..." << Endl;
        {
            NGraph::TEvGraph::TEvGetMetrics* event = new NGraph::TEvGraph::TEvGetMetrics();
            event->Record.AddMetrics("test.metric0");
            event->Record.SetTimeFrom(0);
            event->Record.SetTimeTo(59);
            event->Record.SetSkipBorders(false); // it supposed to be like that by default
            event->Record.SetMaxPoints(60);
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            TAutoPtr<IEventHandle> handle;
            NGraph::TEvGraph::TEvMetricsResult* response = runtime.GrabEdgeEventRethrow<NGraph::TEvGraph::TEvMetricsResult>(handle);
            Ctest << "Received result: " << response->Record.ShortDebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(response->Record.TimeSize(), 21);
        }

        Ctest << "Filling..." << Endl;
        for (int seconds = 60; seconds < 120; ++seconds) {
            {
                NGraph::TEvGraph::TEvSendMetrics* event = new NGraph::TEvGraph::TEvSendMetrics();
                NKikimrGraph::TMetric* metric = event->Record.AddMetrics();
                metric->SetName("test.metric0");
                metric->SetValue(seconds);
                event->Record.SetTime(seconds);
                runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            }
        }

        Ctest << "Checking..." << Endl;
        {
            NGraph::TEvGraph::TEvGetMetrics* event = new NGraph::TEvGraph::TEvGetMetrics();
            event->Record.AddMetrics("test.metric0");
            event->Record.SetTimeFrom(0);
            event->Record.SetTimeTo(119);
            event->Record.SetSkipBorders(false); // it supposed to be like that by default
            event->Record.SetMaxPoints(60);
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            TAutoPtr<IEventHandle> handle;
            NGraph::TEvGraph::TEvMetricsResult* response = runtime.GrabEdgeEventRethrow<NGraph::TEvGraph::TEvMetricsResult>(handle);
            Ctest << "Received result: " << response->Record.ShortDebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(response->Record.TimeSize(), 60);
        }
    }

    Y_UNIT_TEST(LocalBordersOnGet) {
        TTestBasicRuntime runtime;

        runtime.SetLogPriority(NKikimrServices::GRAPH, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::HIVE, NLog::PRI_TRACE);

        TTestEnv::ENABLE_SCHEMESHARD_LOG = false;
        TTestEnv env(runtime, {
            .GraphBackendType_ = "Local"
        });
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "db1"
        )");

        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "db1"
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            StoragePools {
                Name: "pool-1"
                Kind: "hdd"
            }
            ExternalSchemeShard: true
            ExternalHive: true
            GraphShard: true
        )");

        env.TestWaitNotification(runtime, txId);

        NKikimrScheme::TEvDescribeSchemeResult result = DescribePath(runtime, "/MyRoot/db1");
        UNIT_ASSERT(result.GetPathDescription().GetDomainDescription().GetProcessingParams().GetGraphShard() != 0);

        IActor* service = NGraph::CreateGraphService("/MyRoot/db1");
        TActorId serviceId = runtime.Register(service);
        runtime.RegisterService(NGraph::MakeGraphServiceId(), serviceId);
        TActorId sender = runtime.AllocateEdgeActor();

        Ctest << "Preparing..." << Endl;
        // this call is needed to wait for establishing of pipe connection
        {
            NGraph::TEvGraph::TEvGetMetrics* event = new NGraph::TEvGraph::TEvGetMetrics();
            event->Record.AddMetrics("test.metric0");
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            TAutoPtr<IEventHandle> handle;
            NGraph::TEvGraph::TEvMetricsResult* response = runtime.GrabEdgeEventRethrow<NGraph::TEvGraph::TEvMetricsResult>(handle);
            Ctest << "Received result: " << response->Record.ShortDebugString() << Endl;
        }

        runtime.SimulateSleep(TDuration::Seconds(1));

        Ctest << "Filling..." << Endl;
        for (int seconds = 40; seconds < 60; ++seconds) {
            {
                NGraph::TEvGraph::TEvSendMetrics* event = new NGraph::TEvGraph::TEvSendMetrics();
                NKikimrGraph::TMetric* metric = event->Record.AddMetrics();
                metric->SetName("test.metric0");
                metric->SetValue(seconds);
                event->Record.SetTime(seconds);
                runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            }
        }

        Ctest << "Checking..." << Endl;
        {
            NGraph::TEvGraph::TEvGetMetrics* event = new NGraph::TEvGraph::TEvGetMetrics();
            event->Record.AddMetrics("test.metric0");
            event->Record.SetTimeFrom(0);
            event->Record.SetTimeTo(59);
            event->Record.SetSkipBorders(false); // it supposed to be like that by default
            event->Record.SetMaxPoints(60);
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            TAutoPtr<IEventHandle> handle;
            NGraph::TEvGraph::TEvMetricsResult* response = runtime.GrabEdgeEventRethrow<NGraph::TEvGraph::TEvMetricsResult>(handle);
            Ctest << "Received result: " << response->Record.ShortDebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(response->Record.TimeSize(), 21);
        }

        Ctest << "Filling..." << Endl;
        for (int seconds = 60; seconds < 120; ++seconds) {
            {
                NGraph::TEvGraph::TEvSendMetrics* event = new NGraph::TEvGraph::TEvSendMetrics();
                NKikimrGraph::TMetric* metric = event->Record.AddMetrics();
                metric->SetName("test.metric0");
                metric->SetValue(seconds);
                event->Record.SetTime(seconds);
                runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            }
        }

        Ctest << "Checking..." << Endl;
        {
            NGraph::TEvGraph::TEvGetMetrics* event = new NGraph::TEvGraph::TEvGetMetrics();
            event->Record.AddMetrics("test.metric0");
            event->Record.SetTimeFrom(0);
            event->Record.SetTimeTo(119);
            event->Record.SetSkipBorders(false); // it supposed to be like that by default
            event->Record.SetMaxPoints(60);
            runtime.Send(NGraph::MakeGraphServiceId(), sender, event);
            TAutoPtr<IEventHandle> handle;
            NGraph::TEvGraph::TEvMetricsResult* response = runtime.GrabEdgeEventRethrow<NGraph::TEvGraph::TEvMetricsResult>(handle);
            Ctest << "Received result: " << response->Record.ShortDebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(response->Record.TimeSize(), 60);
        }
    }
}

} // NKikimr
