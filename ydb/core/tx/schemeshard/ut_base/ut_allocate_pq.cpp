#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_utils.h>

#include <ydb/core/base/compile_time_flags.h>

#include <util/generic/size_literals.h>
#include <util/string/cast.h>
#include <ydb/library/dbgtrace/debug_trace.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardAllocatePQTest) {
    Y_UNIT_TEST(Boot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
    }

    Y_UNIT_TEST(AllocatePQ) { //+
        DBGTRACE("TSchemeShardAllocatePQTest::AllocatePQ");
        TTestBasicRuntime runtime;
        runtime.SetScheduledLimit(25'000'000);
        TTestEnv env(runtime);
        ui64 txId = 1000;

        DBGTRACE_LOG("");
        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        DBGTRACE_LOG("");
        TestCreatePQGroup(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"PQGroup\""
                        "TotalGroupCount: 9 "
                        "PartitionPerTablet: 4 "
                        "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}");
        env.TestWaitNotification(runtime, {txId-1, txId});

        DBGTRACE_LOG("");
        TestAlterPQGroup(runtime, ++txId, "/MyRoot/DirA",
                         "Name: \"PQGroup\""
                         "TotalGroupCount: 10 "
                         "PartitionPerTablet: 4 "
                         "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}");

        env.TestWaitNotification(runtime, {txId-1, txId});

        DBGTRACE_LOG("");
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::Finished,
                            NLs::PathsInsideDomain(2),
                            NLs::ShardsInsideDomain(4)});

        DBGTRACE_LOG("");
        TestDescribeResult(
                DescribePath(runtime, "/MyRoot/DirA/PQGroup", true),
                {NLs::CheckPartCount("PQGroup", 10, 4, 3, 10, NKikimrSchemeOp::EPathState::EPathStateNoChanges),
                 NLs::CheckPQAlterVersion("PQGroup", 2)}
        );

        DBGTRACE_LOG("");
        {
            auto balancerDescr = GetDescribeFromPQBalancer(runtime, 9437197);
            TString expected = R"(TopicName: "PQGroup" Version: 2 Config { PartitionConfig { LifetimeSeconds: 10 } YdbDatabasePath: "/MyRoot" } PartitionPerTablet: 4 Partitions { Partition: 0 TabletId: 9437194 } Partitions { Partition: 1 TabletId: 9437194 } Partitions { Partition: 2 TabletId: 9437195 } Partitions { Partition: 3 TabletId: 9437194 } Partitions { Partition: 4 TabletId: 9437196 } Partitions { Partition: 5 TabletId: 9437195 } Partitions { Partition: 6 TabletId: 9437194 } Partitions { Partition: 7 TabletId: 9437195 } Partitions { Partition: 8 TabletId: 9437195 } Partitions { Partition: 9 TabletId: 9437196 } SchemeShardId: 8751008 BalancerTabletId: 9437197 SecurityObject: "\022\000")";
            UNIT_ASSERT_NO_DIFF(expected, balancerDescr.ShortUtf8DebugString());
        }

        DBGTRACE_LOG("");
        {
            TAtomic unused;
            runtime.GetAppData().Icb->SetValue("SchemeShard_FillAllocatePQ", true, unused);
        }

        DBGTRACE_LOG("");
        NKikimrScheme::TEvDescribeSchemeResult descr = DescribePath(runtime, "/MyRoot/DirA/PQGroup", true);
        Y_ASSERT(descr.GetPathDescription().GetPersQueueGroup().HasAllocate());

        DBGTRACE_LOG("");
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                               "Name: \"Database\"");

        DBGTRACE_LOG("");
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"Database\" "
                              "ExternalSchemeShard: true "
                              "PlanResolution: 50 "
                              "Coordinators: 1 "
                              "Mediators: 1 "
                              "TimeCastBucketsPerMediator: 2 "
                              "StoragePools { "
                              "  Name: \"pool-1\" "
                              "  Kind: \"pool-kind-1\" "
                              "} ");
        env.TestWaitNotification(runtime, {txId-1, txId});


        DBGTRACE_LOG("");
        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Database"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("Database"),
                            NLs::PathVersionEqual(4),
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(3),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});
        DBGTRACE_LOG("tenantSchemeShard=" << tenantSchemeShard);
        Y_ASSERT(tenantSchemeShard != 0);

        DBGTRACE_LOG("");
        auto allocateDesc = descr.GetPathDescription().GetPersQueueGroup().GetAllocate();
        DBGTRACE_LOG("");
        TestAllocatePQ(runtime, tenantSchemeShard, ++txId, "/MyRoot/Database", allocateDesc.DebugString());
        DBGTRACE_LOG("");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        DBGTRACE_LOG("");
        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/Database/PQGroup", true),
                           {NLs::CheckPartCount("PQGroup", 10, 4, 3, 10)});

        DBGTRACE_LOG("");
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/PQGroup", true),
                           {NLs::CheckPartCount("PQGroup", 10, 4, 3, 10)});

        DBGTRACE_LOG("");
        TestDeallocatePQ(runtime, ++txId, "/MyRoot/DirA", "Name: \"PQGroup\"");
        env.TestWaitNotification(runtime, txId);
        // there are nothing to wait: operation DeallocatePersQueueGroup does not delete any tablets/shards
        //env.TestWaitShardDeletion(runtime, {1, 2, 3, 4});

        DBGTRACE_LOG("");
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/PQGroup", true),
                           {NLs::PathNotExist});

        DBGTRACE_LOG("");
        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/Database/PQGroup", true),
                           {NLs::CheckPartCount("PQGroup", 10, 4, 3, 10)});

        DBGTRACE_LOG("");
        {
            auto balancerDescr = GetDescribeFromPQBalancer(runtime, 9437197);
            TString expected = R"(TopicName: "PQGroup" Version: 3 Config { PartitionConfig { LifetimeSeconds: 10 } YdbDatabasePath: "/MyRoot/Database" } PartitionPerTablet: 4 Partitions { Partition: 0 TabletId: 9437194 } Partitions { Partition: 1 TabletId: 9437194 } Partitions { Partition: 2 TabletId: 9437195 } Partitions { Partition: 3 TabletId: 9437194 } Partitions { Partition: 4 TabletId: 9437196 } Partitions { Partition: 5 TabletId: 9437195 } Partitions { Partition: 6 TabletId: 9437194 } Partitions { Partition: 7 TabletId: 9437195 } Partitions { Partition: 8 TabletId: 9437195 } Partitions { Partition: 9 TabletId: 9437196 } SchemeShardId: 9437198 BalancerTabletId: 9437197 SecurityObject: "\022\000")";
            UNIT_ASSERT_NO_DIFF(expected, balancerDescr.ShortUtf8DebugString());
        }

        DBGTRACE_LOG("");
        TestAlterPQGroup(runtime, tenantSchemeShard, ++txId, "/MyRoot/Database",
                        "Name: \"PQGroup\""
                        "TotalGroupCount: 25 "
                        "PartitionPerTablet: 5 "
                        "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        DBGTRACE_LOG("");
        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/Database/PQGroup", true),
                           {NLs::CheckPartCount("PQGroup", 25, 5, 5, 25)});

        DBGTRACE_LOG("");
        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/Database"),
                           {NLs::PathExist,
                            NLs::PathsInsideDomain(1),
                            NLs::ShardsInsideDomain(9)});
        TestDropPQGroup(runtime, tenantSchemeShard, ++txId, "/MyRoot/Database", "PQGroup");
        env.TestWaitNotification(runtime, txId);

        DBGTRACE_LOG("");
        env.TestWaitTabletDeletion(runtime, {9437194, 9437195, 9437196, 9437197, 9437201, 9437202});
    }
}
