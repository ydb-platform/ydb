#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/testlib/actors/wait_events.h>

#include <ydb/public/lib/value/value.h>


using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardTestExtSubdomainReboots) {
    Y_UNIT_TEST(Fake) {
    }

    Y_UNIT_TEST_FLAG(CreateExtSubdomainWithHive, AlterDatabaseCreateHiveFirst) {
        TTestWithReboots t;
        t.GetTestEnvOptions()
            .EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TLocalPathId subDomainPathId;
            {
                TInactiveZone inactive(activeZone);
                subDomainPathId = GetNextLocalPathId(runtime, t.TxId);
            }

            TestCreateExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                R"(Name: "USER_0")"
            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                   {NLs::PathExist,
                                    NLs::IsExternalSubDomain("USER_0"),
                                    NLs::DomainKey(subDomainPathId, TTestTxConfig::SchemeShard),
                                    NLs::DomainCoordinators({}),
                                    NLs::DomainMediators({}),
                                    NLs::DomainSchemeshard(0),
                                    NLs::DomainHive(0)
                                    });
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::ChildrenCount(3)});
                UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", subDomainPathId));
                UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", subDomainPathId));
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
                                    NLs::DomainKey(subDomainPathId, TTestTxConfig::SchemeShard),
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
                                    NLs::DomainKey(subDomainPathId, TTestTxConfig::SchemeShard),
                                    NLs::ShardsInsideDomain(7),
                                    // internal knowledge of shard declaration sequence is used here
                                    NLs::DomainHive(TTestTxConfig::FakeHiveTablets),
                                    NLs::DomainSchemeshard(subdomainSchemeshard),
                                    NLs::DomainCoordinators({subdomainHiveTablets+1, subdomainHiveTablets+2, subdomainHiveTablets+3}),
                                    NLs::DomainMediators({subdomainHiveTablets+4, subdomainHiveTablets+5}),
                                   });
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::ChildrenCount(3)});
            }

        });
    }

    Y_UNIT_TEST_FLAGS(DropExtSubdomain, AlterDatabaseCreateHiveFirst, ExternalHive) {
        TTestWithReboots t;
        t.GetTestEnvOptions()
            .EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            ui64 expectedDomainPaths;
            TLocalPathId expectedSubDomainPathId;
            {
                TInactiveZone inactive(activeZone);
                auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
                expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();
                expectedSubDomainPathId = GetNextLocalPathId(runtime, t.TxId);
            }

            TestCreateExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                R"(Name: "USER_0")"
            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            expectedDomainPaths += 1;

            TPathId subdomainPathId;
            {
                TInactiveZone inactive(activeZone);

                auto describe = DescribePath(runtime, "/MyRoot/USER_0");
                TestDescribeResult(describe, {
                    NLs::PathExist,
                    NLs::IsExternalSubDomain("USER_0"),
                    NLs::DomainCoordinators({}),
                    NLs::DomainMediators({}),
                    NLs::DomainSchemeshard(0),
                    NLs::DomainHive(0)
                });
                const auto& domainKey = describe.GetPathDescription().GetDomainDescription().GetDomainKey();
                subdomainPathId = TPathId::FromDomainKey(domainKey);
                UNIT_ASSERT_VALUES_EQUAL(subdomainPathId.LocalPathId, expectedSubDomainPathId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot"), {
                    NLs::ChildrenCount(3)
                });

                UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", subdomainPathId.LocalPathId));
                UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", subdomainPathId.LocalPathId));

                // Register observer for future extsubdomain cleanup notification
                t.TestEnv->AddExtSubdomainCleanupObserver(runtime, subdomainPathId);
            }

            TestAlterExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                Sprintf(R"(
                        Name: "USER_0"

                        StoragePools {
                            Name: "tenant-1:hdd"
                            Kind: "hdd"
                        }
                        PlanResolution: 50
                        Coordinators: 1
                        Mediators: 1
                        TimeCastBucketsPerMediator: 2

                        ExternalHive: %s
                        ExternalSchemeShard: true
                    )",
                    ToString(ExternalHive).c_str()
                )
            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            ui64 tenantHiveId = 0;
            {
                TInactiveZone inactive(activeZone);

                auto describe = DescribePath(runtime, "/MyRoot/USER_0");
                TestDescribeResult(describe, {
                    NLs::PathExist,
                    NLs::IsExternalSubDomain("USER_0"),
                    NLs::ExtractDomainHive(&tenantHiveId),
                });

                if (ExternalHive) {
                    // extsubdomain drop should be independent of tenant hive's state.
                    // It must correctly remove database whether tenant nodes and tablets are alive or not.
                    //
                    // Make tenant hive inaccessible by stopping its tablet.
                    // In real life that could be, for example, due to absence of tenant nodes.
                    //
                    // Tenant hive is controlled by the root hive (running at node 0).
                    HiveStopTablet(runtime, TTestTxConfig::Hive, tenantHiveId, 0);
                }
            }

            // drop extsubdomain
            TestForceDropExtSubDomain(runtime, ++t.TxId, "/MyRoot", "USER_0");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            expectedDomainPaths -= 1;

            {
                TInactiveZone inactive(activeZone);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"), {
                    NLs::PathNotExist
                });

                TestDescribeResult(DescribePath(runtime, "/MyRoot"), {
                    NLs::PathExist,
                    NLs::ShardsInsideDomain(0)
                });

                // wait for it to be really cleaned up
                t.TestEnv->WaitForExtSubdomainCleanup(runtime, subdomainPathId);

                // check that extsubdomain's system tablets are deleted from the root hive
                // and not-working state of the tenant hive was unable to hinder that
                {
                    const auto tablets = HiveGetSubdomainTablets(runtime, TTestTxConfig::Hive, subdomainPathId);
                    UNIT_ASSERT_C(tablets.size() == 0, TStringBuilder()
                        << "-- existing subdomain's system tablets in the root hive: expected 0, got " << tablets.size()
                    );
                }

                // check that extsubdomain's path is really erased from the root schemeshard

                TestDescribeResult(DescribePath(runtime, "/MyRoot"), {
                    NLs::PathExist,
                    NLs::PathsInsideDomain(expectedDomainPaths)  // infamous /MyRoot/DirA, created in TTestWithReboots::Prepare()
                });

                {
                    const auto result = ReadLocalTableRecords(runtime, TTestTxConfig::SchemeShard, "SystemShardsToDelete", "ShardIdx");
                    const auto records = NKikimr::NClient::TValue::Create(result)[0]["List"];
                    //DEBUG: Cerr << "TEST: SystemShardsToDelete: " << records.GetValueText<NKikimr::NClient::TFormatJSON>() << Endl;
                    //DEBUG: Cerr << "TEST: " << records.DumpToString() << Endl;
                    UNIT_ASSERT_VALUES_EQUAL(records.Size(), 0);
                }

                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", subdomainPathId.LocalPathId));
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", subdomainPathId.LocalPathId));
            }

        });
    }

    Y_UNIT_TEST_FLAG(CreateExtSubdomainNoHive, AlterDatabaseCreateHiveFirst) {
        TTestWithReboots t;
        t.GetTestEnvOptions()
            .EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TLocalPathId subDomainPathId;
            {
                TInactiveZone inactive(activeZone);
                subDomainPathId = GetNextLocalPathId(runtime, t.TxId);
            }

            TestCreateExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                R"(Name: "USER_0")"
            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                   {NLs::PathExist,
                                    NLs::IsExternalSubDomain("USER_0"),
                                    NLs::DomainKey(subDomainPathId, TTestTxConfig::SchemeShard),
                                    NLs::DomainCoordinators({}),
                                    NLs::DomainMediators({}),
                                    NLs::DomainSchemeshard(0),
                                    NLs::DomainHive(0)
                                    });
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::ChildrenCount(3)});
                UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", subDomainPathId));
                UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", subDomainPathId));
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
                                    NLs::DomainKey(subDomainPathId, TTestTxConfig::SchemeShard),
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
                                    NLs::DomainKey(subDomainPathId, TTestTxConfig::SchemeShard),
                                    NLs::ShardsInsideDomain(6),
                                    // internal knowledge of shard declaration sequence is used here
                                    NLs::DomainSchemeshard(TTestTxConfig::FakeHiveTablets),
                                    NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets+1, TTestTxConfig::FakeHiveTablets+2, TTestTxConfig::FakeHiveTablets+3}),
                                    NLs::DomainMediators({TTestTxConfig::FakeHiveTablets+4, TTestTxConfig::FakeHiveTablets+5}),
                                   });
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::ChildrenCount(3)});
            }

        });
    }

    Y_UNIT_TEST_FLAG(CreateForceDrop, AlterDatabaseCreateHiveFirst) {
        TTestWithReboots t;
        t.GetTestEnvOptions()
            .EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TLocalPathId subDomainPathId;
            {
                TInactiveZone inactive(activeZone);
                subDomainPathId = GetNextLocalPathId(runtime, t.TxId);
            }

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
                                   {NLs::ChildrenCount(2, NKikimrSchemeOp::EPathState::EPathStateNoChanges)});
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", subDomainPathId));
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", subDomainPathId));
            }
        });
    }

    Y_UNIT_TEST_FLAGS(AlterForceDrop, AlterDatabaseCreateHiveFirst, ExternalHive) {
        TTestWithReboots t;
        t.GetTestEnvOptions()
            .EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TLocalPathId subDomainPathId;
            {
                TInactiveZone inactive(activeZone);
                subDomainPathId = GetNextLocalPathId(runtime, t.TxId);
                TestCreateExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                    R"(Name: "USER_0")"
                );
                t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId-1});
            }

            AsyncAlterExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                Sprintf(R"(
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

                        ExternalHive: %s
                    )",
                    ToString(ExternalHive).c_str()
                )
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
                                   {NLs::ChildrenCount(2, NKikimrSchemeOp::EPathState::EPathStateNoChanges)});
                t.TestEnv->TestWaitShardDeletion(runtime, {1, 2, 3, 4, 5, 6});
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", subDomainPathId));
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", subDomainPathId));
            }
        });
    }

    Y_UNIT_TEST_FLAGS(SchemeLimits, AlterDatabaseCreateHiveFirst, ExternalHive) {
        TTestWithReboots t;
        t.GetTestEnvOptions()
            .EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst)
        ;

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TSchemeLimits limits;
            limits.MaxDepth = 2;
            limits.MaxPaths = 2;
            limits.MaxShards = 3 + (ExternalHive ? 1 : 0);

            {
                TInactiveZone inactive(activeZone);

                SetSchemeshardSchemaLimits(runtime, limits);

                TestCreateExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                    R"(Name: "USER_0")"
                );
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestAlterExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                Sprintf(R"(
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

                        ExternalHive: %s
                    )",
                    ToString(ExternalHive).c_str()
                )
            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                ui64 subdomainSchemeshard;
                TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                   {NLs::PathExist,
                                    NLs::IsExternalSubDomain("USER_0"),
                                    NLs::ExtractTenantSchemeshard(&subdomainSchemeshard),
                                    NLs::ShardsInsideDomain(limits.MaxShards),
                                    NLs::DomainLimitsIs(limits.MaxPaths, limits.MaxShards)});

                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::ChildrenCount(3),
                                    NLs::DomainLimitsIs(limits.MaxPaths, limits.MaxShards)});

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
        //INFO: Temporarily this test will not run when EnableAlterDatabase is not set
        t.GetTestEnvOptions().EnableAlterDatabase(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TSchemeLimits limits;
            limits.MaxShards = 7;
            limits.MaxShardsInPath = 3;
            limits.MaxPaths = 5;
            limits.MaxChildrenInDir = 4;

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

                ui64 tenantSchemeShard = 0;
                // test what the parent knows about the subdomain
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Alice"), {
                    NLs::PathExist,
                    NLs::IsExternalSubDomain("Alice"),
                    NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
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
                    NLs::ShardsInsideDomain(3)
                });

                const auto defaultLimits = TSchemeLimits().AsProto();
                TestDescribeResult(DescribePath(runtime, "/MyRoot"), {
                    NLs::ChildrenCount(3),
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
