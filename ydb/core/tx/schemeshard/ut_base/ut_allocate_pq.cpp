#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_utils.h>

#include <ydb/core/base/compile_time_flags.h>

#include <util/generic/size_literals.h>
#include <util/string/cast.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardAllocatePQTest) {
    Y_UNIT_TEST(Boot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
    }

    Y_UNIT_TEST(AllocatePQ) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 1000;

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        TestCreatePQGroup(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"PQGroup\""
                        "TotalGroupCount: 9 "
                        "PartitionPerTablet: 4 "
                        "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}");
        env.TestWaitNotification(runtime, {txId-1, txId});

        TestAlterPQGroup(runtime, ++txId, "/MyRoot/DirA",
                         "Name: \"PQGroup\""
                         "TotalGroupCount: 10 "
                         "PartitionPerTablet: 4 "
                         "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}");

        env.TestWaitNotification(runtime, {txId-1, txId});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::Finished,
                            NLs::PathsInsideDomain(2),
                            NLs::ShardsInsideDomain(4)});

        TestDescribeResult(
                DescribePath(runtime, "/MyRoot/DirA/PQGroup", true),
                {NLs::CheckPartCount("PQGroup", 10, 4, 3, 10, NKikimrSchemeOp::EPathState::EPathStateNoChanges),
                 NLs::CheckPQAlterVersion("PQGroup", 2)}
        );

        {
            auto balancerDescr = GetDescribeFromPQBalancer(runtime, 72075186233409549);
            TString expected = R"(TopicName: "PQGroup" Version: 2 Config { PartitionConfig { LifetimeSeconds: 10 } YdbDatabasePath: "/MyRoot" } PartitionPerTablet: 4 Partitions { Partition: 0 TabletId: 72075186233409546 } Partitions { Partition: 1 TabletId: 72075186233409546 } Partitions { Partition: 2 TabletId: 72075186233409547 } Partitions { Partition: 3 TabletId: 72075186233409546 } Partitions { Partition: 4 TabletId: 72075186233409548 } Partitions { Partition: 5 TabletId: 72075186233409547 } Partitions { Partition: 6 TabletId: 72075186233409546 } Partitions { Partition: 7 TabletId: 72075186233409547 } Partitions { Partition: 8 TabletId: 72075186233409547 } Partitions { Partition: 9 TabletId: 72075186233409548 } SchemeShardId: 72057594046678944 BalancerTabletId: 72075186233409549 SecurityObject: "\022\000")";
            UNIT_ASSERT_NO_DIFF(expected, balancerDescr.ShortUtf8DebugString());
        }

        {
            TAtomic unused;
            runtime.GetAppData().Icb->SetValue("SchemeShard_FillAllocatePQ", true, unused);
        }

        NKikimrScheme::TEvDescribeSchemeResult descr = DescribePath(runtime, "/MyRoot/DirA/PQGroup", true);
        Y_ASSERT(descr.GetPathDescription().GetPersQueueGroup().HasAllocate());

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                               "Name: \"Database\"");

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


        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Database"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("Database"),
                            NLs::PathVersionEqual(4),
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(3),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});
        Y_ASSERT(tenantSchemeShard != 0);

        auto allocateDesc = descr.GetPathDescription().GetPersQueueGroup().GetAllocate();
        TestAllocatePQ(runtime, tenantSchemeShard, ++txId, "/MyRoot/Database", allocateDesc.DebugString());
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/Database/PQGroup", true),
                           {NLs::CheckPartCount("PQGroup", 10, 4, 3, 10)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/PQGroup", true),
                           {NLs::CheckPartCount("PQGroup", 10, 4, 3, 10)});

        TestDeallocatePQ(runtime, ++txId, "/MyRoot/DirA", "Name: \"PQGroup\"");
        env.TestWaitNotification(runtime, txId);
        // there are nothing to wait: operation DeallocatePersQueueGroup does not delete any tablets/shards
        //env.TestWaitShardDeletion(runtime, {1, 2, 3, 4});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/PQGroup", true),
                           {NLs::PathNotExist});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/Database/PQGroup", true),
                           {NLs::CheckPartCount("PQGroup", 10, 4, 3, 10)});

        {
            auto balancerDescr = GetDescribeFromPQBalancer(runtime, 72075186233409549);
            TString expected = R"(TopicName: "PQGroup" Version: 3 Config { PartitionConfig { LifetimeSeconds: 10 } YdbDatabasePath: "/MyRoot/Database" } PartitionPerTablet: 4 Partitions { Partition: 0 TabletId: 72075186233409546 } Partitions { Partition: 1 TabletId: 72075186233409546 } Partitions { Partition: 2 TabletId: 72075186233409547 } Partitions { Partition: 3 TabletId: 72075186233409546 } Partitions { Partition: 4 TabletId: 72075186233409548 } Partitions { Partition: 5 TabletId: 72075186233409547 } Partitions { Partition: 6 TabletId: 72075186233409546 } Partitions { Partition: 7 TabletId: 72075186233409547 } Partitions { Partition: 8 TabletId: 72075186233409547 } Partitions { Partition: 9 TabletId: 72075186233409548 } SchemeShardId: 72075186233409550 BalancerTabletId: 72075186233409549 SecurityObject: "\022\000")";
            UNIT_ASSERT_NO_DIFF(expected, balancerDescr.ShortUtf8DebugString());
        }

        TestAlterPQGroup(runtime, tenantSchemeShard, ++txId, "/MyRoot/Database",
                        "Name: \"PQGroup\""
                        "TotalGroupCount: 25 "
                        "PartitionPerTablet: 5 "
                        "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/Database/PQGroup", true),
                           {NLs::CheckPartCount("PQGroup", 25, 5, 5, 25)});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/Database"),
                           {NLs::PathExist,
                            NLs::PathsInsideDomain(1),
                            NLs::ShardsInsideDomain(9)});
        TestDropPQGroup(runtime, tenantSchemeShard, ++txId, "/MyRoot/Database", "PQGroup");
        env.TestWaitNotification(runtime, txId);

        env.TestWaitTabletDeletion(runtime, {72075186233409546, 72075186233409547, 72075186233409548, 72075186233409549, 72075186233409553, 72075186233409554});
    }
}
