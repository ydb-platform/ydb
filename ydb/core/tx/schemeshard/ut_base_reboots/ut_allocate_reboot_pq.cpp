#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_utils.h>

#include <ydb/core/base/compile_time_flags.h>

#include <util/generic/size_literals.h>
#include <util/string/cast.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;


Y_UNIT_TEST_SUITE(TSchemeShardAllocatePQRebootTest) {
    Y_UNIT_TEST(AllocateWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            NKikimrSchemeOp::TPersQueueGroupAllocate allocateDesc;
            ui64 tenantSchemeShard = 0;

            {
                TInactiveZone inactive(activeZone);
                TestCreatePQGroup(runtime, ++t.TxId, "/MyRoot/DirA",
                                "Name: \"PQGroup\""
                                "TotalGroupCount: 10 "
                                "PartitionPerTablet: 10 "
                                "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}"
                                );

                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/PQGroup", true),
                                           {NLs::Finished,
                                            NLs::CheckPartCount("PQGroup", 10, 10, 1, 10),
                                            NLs::PQPartitionsInsideDomain(10),
                                            NLs::PathVersionEqual(2)});

                TestCreateExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                       "Name: \"Database\"");

                TestAlterExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
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
                t.TestEnv->TestWaitNotification(runtime, {++t.TxId-1, ++t.TxId});


                TestDescribeResult(DescribePath(runtime, "/MyRoot/Database"),
                                   {NLs::PathExist,
                                    NLs::IsExternalSubDomain("Database"),
                                    NLs::PathVersionEqual(4),
                                    NLs::PathsInsideDomain(0),
                                    NLs::ShardsInsideDomain(3),
                                    NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});
                Y_ASSERT(tenantSchemeShard != 0);

                {
                    TAtomic unused;
                    runtime.GetAppData().Icb->SetValue("SchemeShard_FillAllocatePQ", true, unused);
                }

                NKikimrScheme::TEvDescribeSchemeResult descr = DescribePath(runtime, "/MyRoot/DirA/PQGroup", true);
                Y_ASSERT(descr.GetPathDescription().GetPersQueueGroup().HasAllocate());

                allocateDesc = descr.GetPathDescription().GetPersQueueGroup().GetAllocate();

            }

            TestAllocatePQ(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/Database", allocateDesc.DebugString());
            t.TestEnv->TestWaitNotification(runtime, t.TxId, tenantSchemeShard);

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/Database/PQGroup", true),
                               {NLs::CheckPartCount("PQGroup", 10, 10, 1, 10)});

            TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/PQGroup", true),
                               {NLs::CheckPartCount("PQGroup", 10, 10, 1, 10)});


            TestDeallocatePQ(runtime, ++t.TxId, "/MyRoot/DirA", "Name: \"PQGroup\"");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/PQGroup", true),
                               {NLs::PathNotExist});

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/Database/PQGroup", true),
                               {NLs::CheckPartCount("PQGroup", 10, 10, 1, 10)});

            {
                TInactiveZone inactive(activeZone);

                auto balancerDescr = GetDescribeFromPQBalancer(runtime, 72075186233409547);
                Cerr << balancerDescr.ShortUtf8DebugString();
                TString expected = R"(TopicName: "PQGroup" Version: 2 Config { PartitionConfig { LifetimeSeconds: 10 } YdbDatabasePath: "/MyRoot/Database" } PartitionPerTablet: 10 Partitions { Partition: 0 TabletId: 72075186233409546 } Partitions { Partition: 1 TabletId: 72075186233409546 } Partitions { Partition: 2 TabletId: 72075186233409546 } Partitions { Partition: 3 TabletId: 72075186233409546 } Partitions { Partition: 4 TabletId: 72075186233409546 } Partitions { Partition: 5 TabletId: 72075186233409546 } Partitions { Partition: 6 TabletId: 72075186233409546 } Partitions { Partition: 7 TabletId: 72075186233409546 } Partitions { Partition: 8 TabletId: 72075186233409546 } Partitions { Partition: 9 TabletId: 72075186233409546 } SchemeShardId: 72075186233409548 BalancerTabletId: 72075186233409547 SecurityObject: "\022\000")";
                UNIT_ASSERT_NO_DIFF(expected, balancerDescr.ShortUtf8DebugString());

                // there are nothing to wait: operation DeallocatePersQueueGroup does not delete any tablets/shards
                // t.TestEnv->TestWaitShardDeletion(runtime, {1});
            }
        });
    }
}
