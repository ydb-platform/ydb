#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/testlib/actors/wait_events.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <google/protobuf/text_format.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardTestExtSubdomainReboots) {
    Y_UNIT_TEST(Fake) {
    }

    Y_UNIT_TEST_FLAG(CreateExternalSubdomain, AlterDatabaseCreateHiveFirst) {
        TTestWithReboots t;
        t.GetTestEnvOptions()
            .EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst)
            .EnableRealSystemViewPaths(false);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {

            TestCreateExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                R"(Name: "USER_0")"
            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                   {NLs::PathExist,
                                    NLs::IsExternalSubDomain("USER_0"),
                                    NLs::DomainKey(3, TTestTxConfig::SchemeShard),
                                    NLs::DomainCoordinators({}),
                                    NLs::DomainMediators({}),
                                    NLs::DomainSchemeshard(0),
                                    NLs::DomainHive(0)
                                    });
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::ChildrenCount(2)});
                UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 3));
                UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 3));
            }

            TestAlterExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                R"(
                    Name: "USER_0"

                    StoragePools {
                        Name: "tenant-1:hdd"
                        Kind: "hdd"
                    }
                    PlanResolution: 50
                    Coordinators: 3
                    Mediators: 2
                    TimeCastBucketsPerMediator: 2

                    ExternalHive: true
                    ExternalSchemeShard: true
                )"
            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                //NOTE: AlterDatabaseCreateHiveFirst create system tablets in a child hive, AlterDatabaseGen1 create system tablets in the root hive
                ui64 subdomainHiveTablets = TTestTxConfig::FakeHiveTablets + (AlterDatabaseCreateHiveFirst ? TFakeHiveState::TABLETS_PER_CHILD_HIVE : 1);
                ui64 subdomainSchemeshard = subdomainHiveTablets;

                // check scheme from root schemeshard
                TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                   {NLs::PathExist,
                                    NLs::IsExternalSubDomain("USER_0"),
                                    NLs::DomainKey(3, TTestTxConfig::SchemeShard),
                                    NLs::ShardsInsideDomain(7),
                                    // internal knowledge of shard declaration sequence is used here
                                    NLs::DomainHive(TTestTxConfig::FakeHiveTablets),
                                    NLs::DomainSchemeshard(subdomainSchemeshard),
                                    NLs::DomainCoordinators({subdomainHiveTablets+1, subdomainHiveTablets+2, subdomainHiveTablets+3}),
                                    NLs::DomainMediators({subdomainHiveTablets+4, subdomainHiveTablets+5}),
                                   });

                // check scheme from extsubdomain schemeshard
                TestDescribeResult(DescribePath(runtime, subdomainSchemeshard, "/MyRoot/USER_0"),
                                   {NLs::PathExist,
                                    NLs::IsSubDomain("MyRoot/USER_0"),
                                    NLs::DomainKey(3, TTestTxConfig::SchemeShard),
                                    NLs::ShardsInsideDomain(7),
                                    // internal knowledge of shard declaration sequence is used here
                                    NLs::DomainHive(TTestTxConfig::FakeHiveTablets),
                                    NLs::DomainSchemeshard(subdomainSchemeshard),
                                    NLs::DomainCoordinators({subdomainHiveTablets+1, subdomainHiveTablets+2, subdomainHiveTablets+3}),
                                    NLs::DomainMediators({subdomainHiveTablets+4, subdomainHiveTablets+5}),
                                   });
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::ChildrenCount(2)});
            }

        });
    }

    Y_UNIT_TEST_FLAG(CreateExternalSubdomainWithoutHive, AlterDatabaseCreateHiveFirst) {
        TTestWithReboots t;
        t.GetTestEnvOptions()
            .EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst)
            .EnableRealSystemViewPaths(false);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {

            TestCreateExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                R"(Name: "USER_0")"
            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                   {NLs::PathExist,
                                    NLs::IsExternalSubDomain("USER_0"),
                                    NLs::DomainKey(3, TTestTxConfig::SchemeShard),
                                    NLs::DomainCoordinators({}),
                                    NLs::DomainMediators({}),
                                    NLs::DomainSchemeshard(0),
                                    NLs::DomainHive(0)
                                    });
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::ChildrenCount(2)});
                UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 3));
                UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 3));
            }

            TestAlterExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                R"(
                    StoragePools {
                        Name: "tenant-1:hdd"
                        Kind: "hdd"
                    }
                    PlanResolution: 50
                    Coordinators: 3
                    Mediators: 2
                    TimeCastBucketsPerMediator: 2
                    ExternalSchemeShard: true
                    Name: "USER_0"
                )"
            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                // check scheme from root schemeshard
                TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                   {NLs::PathExist,
                                    NLs::IsExternalSubDomain("USER_0"),
                                    NLs::DomainKey(3, TTestTxConfig::SchemeShard),
                                    NLs::ShardsInsideDomain(6),
                                    // internal knowledge of shard declaration sequence is used here
                                    NLs::DomainSchemeshard(TTestTxConfig::FakeHiveTablets),
                                    NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets+1, TTestTxConfig::FakeHiveTablets+2, TTestTxConfig::FakeHiveTablets+3}),
                                    NLs::DomainMediators({TTestTxConfig::FakeHiveTablets+4, TTestTxConfig::FakeHiveTablets+5}),
                                   });

                // check scheme from extsubdomain schemeshard
                TestDescribeResult(DescribePath(runtime, TTestTxConfig::FakeHiveTablets, "/MyRoot/USER_0"),
                                   {NLs::PathExist,
                                    NLs::IsSubDomain("MyRoot/USER_0"),
                                    NLs::DomainKey(3, TTestTxConfig::SchemeShard),
                                    NLs::ShardsInsideDomain(6),
                                    // internal knowledge of shard declaration sequence is used here
                                    NLs::DomainSchemeshard(TTestTxConfig::FakeHiveTablets),
                                    NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets+1, TTestTxConfig::FakeHiveTablets+2, TTestTxConfig::FakeHiveTablets+3}),
                                    NLs::DomainMediators({TTestTxConfig::FakeHiveTablets+4, TTestTxConfig::FakeHiveTablets+5}),
                                   });
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::ChildrenCount(2)});
            }

        });
    }

    Y_UNIT_TEST_FLAG(CreateForceDrop, AlterDatabaseCreateHiveFirst) {
        TTestWithReboots t;
        t.GetTestEnvOptions()
            .EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst)
            .EnableRealSystemViewPaths(false);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {

            AsyncCreateExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                R"(Name: "USER_0")"
            );
            t.TestEnv->ReliablePropose(runtime, ForceDropExtSubDomainRequest(++t.TxId, "/MyRoot", "USER_0"),
                                       {NKikimrScheme::StatusAccepted});
            t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId-1});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                   {NLs::PathNotExist});
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::ChildrenCount(1, NKikimrSchemeOp::EPathState::EPathStateNoChanges)});
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 3));
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 3));
            }
        });
    }

    Y_UNIT_TEST_FLAG(AlterForceDrop, AlterDatabaseCreateHiveFirst) {
        TTestWithReboots t;
        t.GetTestEnvOptions()
            .EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst)
            .EnableRealSystemViewPaths(false);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                    R"(Name: "USER_0")"
                );
                t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId-1});
            }

            AsyncAlterExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                R"(
                    StoragePools {
                        Name: "tenant-1:hdd"
                        Kind: "hdd"
                    }
                    PlanResolution: 50
                    Coordinators: 3
                    Mediators: 2
                    TimeCastBucketsPerMediator: 2
                    ExternalSchemeShard: true
                    Name: "USER_0"
                )"
            );
            t.TestEnv->ReliablePropose(runtime, ForceDropExtSubDomainRequest(++t.TxId, "/MyRoot", "USER_0"),
                                       {NKikimrScheme::StatusAccepted});

            t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId-1});
            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+6));

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                   {NLs::PathNotExist});
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::ChildrenCount(1, NKikimrSchemeOp::EPathState::EPathStateNoChanges)});
                t.TestEnv->TestWaitShardDeletion(runtime, {1, 2, 3, 4, 5, 6});
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 3));
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 3));
            }
        });
    }


    Y_UNIT_TEST_FLAG(SchemeLimits, AlterDatabaseCreateHiveFirst) {
        TTestWithReboots t;
        t.GetTestEnvOptions()
            .EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst)
            .EnableRealSystemViewPaths(false);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TSchemeLimits limits;
            limits.MaxDepth = 2;
            limits.MaxShards = 3;
            limits.MaxPaths = 2;

            {
                TInactiveZone inactive(activeZone);

                SetSchemeshardSchemaLimits(runtime, limits);

                TestCreateExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                    R"(Name: "USER_0")"
                );
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestAlterExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                R"(
                    StoragePools {
                        Name: "tenant-1:hdd"
                        Kind: "hdd"
                    }
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                    ExternalSchemeShard: true
                    Name: "USER_0"
                )"
            );
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

                ui64 subdomainSchemeshard = TTestTxConfig::FakeHiveTablets;

                TestDescribeResult(DescribePath(runtime, subdomainSchemeshard, "/MyRoot/USER_0"),
                                   {NLs::PathExist,
                                    NLs::IsSubDomain("MyRoot/USER_0"),
                                    NLs::DomainKey(3, TTestTxConfig::SchemeShard),
                                    // internal knowledge of shard declaration sequence is used here
                                    NLs::DomainSchemeshard(subdomainSchemeshard),
                                    NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets+1}),
                                    NLs::DomainMediators({TTestTxConfig::FakeHiveTablets+2}),
                                    NLs::DomainLimitsIs(limits.MaxPaths, limits.MaxShards),
                                    NLs::ShardsInsideDomain(3),
                                    NLs::PathsInsideDomain(0)
                                   });

                TestCreateTable(runtime, subdomainSchemeshard, ++t.TxId, "/MyRoot/USER_0", R"(
                            Name: "Table"
                            Columns { Name: "Id" Type: "Uint32" }
                            KeyColumnNames: ["Id"]
                        )", {NKikimrScheme::StatusResourceExhausted});

                TestMkDir(runtime, subdomainSchemeshard, ++t.TxId, "/MyRoot/USER_0", "A");
                TestMkDir(runtime, subdomainSchemeshard, ++t.TxId, "/MyRoot/USER_0", "B");
                TestMkDir(runtime, subdomainSchemeshard, ++t.TxId, "/MyRoot/USER_0", "C", {NKikimrScheme::StatusResourceExhausted});
            }
        });
    }

    Y_UNIT_TEST(AlterSchemeLimits) {
        TTestWithReboots t;
        t.GetTestEnvOptions().EnableRealSystemViewPaths(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TSchemeLimits limits;
            limits.MaxShards = 7;
            limits.MaxShardsInPath = 3;
            limits.MaxPaths = 5;
            limits.MaxChildrenInDir = 3;

            {
                TInactiveZone inactive(activeZone);
                TestCreateExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                    R"(Name: "Alice")"
                );
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestAlterExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                    R"(
                        StoragePools {
                            Name: "tenant-1:hdd"
                            Kind: "hdd"
                        }
                        PlanResolution: 50
                        Coordinators: 1
                        Mediators: 1
                        TimeCastBucketsPerMediator: 2
                        ExternalSchemeShard: true
                        Name: "Alice"
                    )"
                );
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            activeZone = false;
            using TEvSync = TEvSchemeShard::TEvUpdateTenantSchemeShard;
            TWaitForFirstEvent<TEvSync> syncWaiter(runtime, [](const TEvSync::TPtr& ev) {
                return ev.Get()->Get()->Record.HasSchemeLimits();
            });
            activeZone = true;

            TestAlterExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                Sprintf(R"(
                    Name: "Alice"
                    SchemeLimits {
                        MaxShards: %lu
                        MaxShardsInPath: %lu
                        MaxPaths: %lu
                        MaxChildrenInDir: %lu
                    }
                )", limits.MaxShards, limits.MaxShardsInPath, limits.MaxPaths, limits.MaxChildrenInDir
            ));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                const auto tenantSchemeShard = TTestTxConfig::FakeHiveTablets;
                // test what the parent knows about the subdomain
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Alice"), {
                    NLs::PathExist,
                    NLs::IsExternalSubDomain("Alice"),
                    NLs::SchemeLimits(limits.AsProto()),
                    NLs::ShardsInsideDomain(3),
                    NLs::PathsInsideDomain(0)
                });

                syncWaiter.Wait();
                syncWaiter.Stop();
                // test what the subdomain knows about itself
                TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/Alice"), {
                    NLs::PathExist,
                    NLs::SchemeLimits(limits.AsProto()),
                    NLs::ShardsInsideDomain(3),
                    NLs::PathsInsideDomain(0)
                });

                const auto defaultLimits = TSchemeLimits().AsProto();
                TestDescribeResult(DescribePath(runtime, "/MyRoot"), {
                    NLs::ChildrenCount(2),
                    NLs::SchemeLimits(defaultLimits)
                });

                TestCreateTable(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/Alice", R"(
                        Name: "TableA"
                        Columns { Name: "Id" Type: "Uint32" }
                        KeyColumnNames: ["Id"]
                        UniformPartitionsCount: 4
                    )", { NKikimrScheme::StatusResourceExhausted } // blocked by the max shards in path limit
                );
                TestCreateTable(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/Alice", R"(
                        Name: "TableA"
                        Columns { Name: "Id" Type: "Uint32" }
                        KeyColumnNames: ["Id"]
                        UniformPartitionsCount: 3
                    )"
                );
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestCreateTable(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/Alice", R"(
                        Name: "TableB"
                        Columns { Name: "Id" Type: "Uint32" }
                        KeyColumnNames: ["Id"]
                        UniformPartitionsCount: 2
                    )", { NKikimrScheme::StatusResourceExhausted } // blocked by the max shards limit
                );
                TestCreateTable(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/Alice", R"(
                        Name: "TableB"
                        Columns { Name: "Id" Type: "Uint32" }
                        KeyColumnNames: ["Id"]
                    )"
                );
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestMkDir(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/Alice", "A");
                TestMkDir(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/Alice", "B", {NKikimrScheme::StatusResourceExhausted}); // blocked by the max children in dir limit
                TestMkDir(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/Alice/A", "A");
                TestMkDir(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/Alice/A", "B");
                TestMkDir(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/Alice/A", "C", {NKikimrScheme::StatusResourceExhausted}); // blocked by the max paths limit
            }
        });
    }
}
