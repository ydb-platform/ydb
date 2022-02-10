#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <google/protobuf/text_format.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardTestExtSubdomainReboots) {
    Y_UNIT_TEST(Fake) {
    }

    Y_UNIT_TEST(CreateExternalSubdomain) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {

            TestCreateExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                   "Name: \"USER_0\"");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                   {NLs::PathExist,
                                    NLs::IsExternalSubDomain("USER_0"),
                                    NLs::DomainKey(3, TTestTxConfig::SchemeShard), 
                                    NLs::DomainCoordinators({}),
                                    NLs::DomainMediators({}),
                                    NLs::DomainSchemeshard(0)
                                    });
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::ChildrenCount(2)});
                UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 3)); 
                UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 3)); 
            }

            TestAlterExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                  "StoragePools { "
                                  "  Name: \"tenant-1:hdd\" "
                                  "  Kind: \"hdd\" "
                                  "} "
                                  "PlanResolution: 50 "
                                  "Coordinators: 3 "
                                  "Mediators: 2 "
                                  "TimeCastBucketsPerMediator: 2 "
                                  "ExternalSchemeShard: true "
                                  "Name: \"USER_0\"");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                   {NLs::PathExist,
                                    NLs::IsExternalSubDomain("USER_0"),
                                    NLs::DomainKey(3, TTestTxConfig::SchemeShard), 
                                    NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1, TTestTxConfig::FakeHiveTablets+2}), 
                                    NLs::DomainMediators({TTestTxConfig::FakeHiveTablets+3, TTestTxConfig::FakeHiveTablets+4}), 
                                    NLs::DomainSchemeshard(TTestTxConfig::FakeHiveTablets+5) 
                                   });

                TestDescribeResult(DescribePath(runtime, TTestTxConfig::FakeHiveTablets+5, "/MyRoot/USER_0"), 
                                   {NLs::PathExist,
                                    NLs::IsSubDomain("MyRoot/USER_0"),
                                    NLs::DomainKey(3, TTestTxConfig::SchemeShard), 
                                    NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1, TTestTxConfig::FakeHiveTablets+2}), 
                                    NLs::DomainMediators({TTestTxConfig::FakeHiveTablets+3, TTestTxConfig::FakeHiveTablets+4}), 
                                    NLs::DomainSchemeshard(TTestTxConfig::FakeHiveTablets+5) 
                                   });
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::ChildrenCount(2)});
            }

        });
    }

    Y_UNIT_TEST(CreateForceDrop) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {

            AsyncCreateExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                   "Name: \"USER_0\"");
            t.TestEnv->ReliablePropose(runtime, ForceDropExtSubDomainRequest(++t.TxId, "/MyRoot", "USER_0"),
                                       {NKikimrScheme::StatusAccepted});
            t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId-1});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                   {NLs::PathNotExist});
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::ChildrenCount(1)});
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 3)); 
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 3)); 
            }
        });
    }

    Y_UNIT_TEST(AlterForceDrop) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                    "Name: \"USER_0\"");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId-1});
            }

            AsyncAlterExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                  "StoragePools { "
                                  "  Name: \"tenant-1:hdd\" "
                                  "  Kind: \"hdd\" "
                                  "} "
                                  "PlanResolution: 50 "
                                  "Coordinators: 3 "
                                  "Mediators: 2 "
                                  "TimeCastBucketsPerMediator: 2 "
                                  "ExternalSchemeShard: true "
                                  "Name: \"USER_0\"");
            t.TestEnv->ReliablePropose(runtime, ForceDropExtSubDomainRequest(++t.TxId, "/MyRoot", "USER_0"),
                                       {NKikimrScheme::StatusAccepted});

            t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId-1});
            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+6)); 

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                   {NLs::PathNotExist});
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::ChildrenCount(1)});
                t.TestEnv->TestWaitShardDeletion(runtime, {1, 2, 3, 4, 5, 6});
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 3)); 
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 3)); 
            }
        });
    }


    Y_UNIT_TEST(SchemeLimits) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TSchemeLimits limits;
            limits.MaxDepth = 2;
            limits.MaxShards = 3;
            limits.MaxPaths = 2;

            {
                TInactiveZone inactive(activeZone);

                SetSchemeshardSchemaLimits(runtime, limits);

                TestCreateExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                       "Name: \"USER_0\"");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestAlterExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                   "StoragePools { "
                                   "  Name: \"tenant-1:hdd\" "
                                   "  Kind: \"hdd\" "
                                   "} "
                                   "PlanResolution: 50 "
                                   "Coordinators: 1 "
                                   "Mediators: 1 "
                                   "TimeCastBucketsPerMediator: 2 "
                                   "ExternalSchemeShard: true "
                                   "Name: \"USER_0\"");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                   {NLs::PathExist,
                                    NLs::IsExternalSubDomain("USER_0"),
                                    NLs::DomainLimitsIs(limits.MaxPaths, limits.MaxShards)});

                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::ChildrenCount(2),
                                    NLs::DomainLimitsIs(limits.MaxPaths, limits.MaxShards)});

                ui64 extSchemeSahrd = TTestTxConfig::FakeHiveTablets+2; 

                TestDescribeResult(DescribePath(runtime, TTestTxConfig::FakeHiveTablets+2, "/MyRoot/USER_0"), 
                                   {NLs::PathExist,
                                    NLs::IsSubDomain("MyRoot/USER_0"),
                                    NLs::DomainKey(3, TTestTxConfig::SchemeShard), 
                                    NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}), 
                                    NLs::DomainMediators({TTestTxConfig::FakeHiveTablets+1}), 
                                    NLs::DomainSchemeshard(extSchemeSahrd),
                                    NLs::DomainLimitsIs(limits.MaxPaths, limits.MaxShards),
                                    NLs::ShardsInsideDomain(3),
                                    NLs::PathsInsideDomain(0)
                                   });

                TestCreateTable(runtime, extSchemeSahrd, ++t.TxId, "/MyRoot/USER_0", R"(
                            Name: "Table"
                            Columns { Name: "Id" Type: "Uint32" }
                            KeyColumnNames: ["Id"]
                        )", {NKikimrScheme::StatusResourceExhausted});

                TestMkDir(runtime, extSchemeSahrd, ++t.TxId, "/MyRoot/USER_0", "A");
                TestMkDir(runtime, extSchemeSahrd, ++t.TxId, "/MyRoot/USER_0", "B");
                TestMkDir(runtime, extSchemeSahrd, ++t.TxId, "/MyRoot/USER_0", "C", {NKikimrScheme::StatusResourceExhausted});
            }
        });
    }
}
