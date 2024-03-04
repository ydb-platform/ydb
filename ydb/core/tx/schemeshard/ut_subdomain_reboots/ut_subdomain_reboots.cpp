#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <google/protobuf/text_format.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(SubDomainWithReboots) {
    Y_UNIT_TEST(Fake) {
    }

    Y_UNIT_TEST(Create) { //+
        TTestWithReboots t(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TVector<TString> userAttrsKeys{"AttrA1", "AttrA2"};
            TUserAttrs userAttrs{{"AttrA1", "ValA1"}, {"AttrA2", "ValA2"}};

            AsyncCreateSubDomain(runtime, ++t.TxId, "/MyRoot/DirA",
                                 "PlanResolution: 50 "
                                 "Coordinators: 1 "
                                 "Mediators: 1 "
                                 "TimeCastBucketsPerMediator: 2 "
                                 "Name: \"USER_0\"",
                                 AlterUserAttrs(userAttrs));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::Finished,
                                    NLs::IsSubDomain("USER_0"),
                                    NLs::PathVersionEqual(3),
                                    NLs::PathsInsideDomain(0),
                                    NLs::ShardsInsideDomain(2),
                                    NLs::UserAttrsEqual(userAttrs)});

                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                   {NLs::PathExist,
                                    NLs::PathVersionEqual(5),
                                    NLs::PathsInsideDomain(2),
                                    NLs::ShardsInsideDomain(0)});

                UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 3));
                UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 3));
            }
        });
    }

    Y_UNIT_TEST(DeclareAndDefine) { //+
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TVector<TString> userAttrsKeys{"AttrA1", "AttrA2"};
            TUserAttrs userAttrs{{"AttrA1", "ValA1"}, {"AttrA2", "ValA2"}};

            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, ++t.TxId,
                                    "/MyRoot/DirA",
                                    "Name: \"USER_0\"",
                                    AlterUserAttrs(userAttrs));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestAlterSubDomain(runtime, ++t.TxId,  "/MyRoot/DirA",
                               "PlanResolution: 50 "
                               "Coordinators: 1 "
                               "Mediators: 2 "
                               "TimeCastBucketsPerMediator: 2 "
                               "Name: \"USER_0\"");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone guard(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::Finished,
                                    NLs::IsSubDomain("USER_0"),
                                    NLs::PathVersionEqual(4),
                                    NLs::PathsInsideDomain(0),
                                    NLs::ShardsInsideDomain(3),
                                    NLs::UserAttrsEqual(userAttrs)});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                   {NLs::PathVersionEqual(5),
                                    NLs::PathsInsideDomain(2),
                                    NLs::ShardsInsideDomain(0)});
            }
        });
    }

    Y_UNIT_TEST(CreateWithStoragePools) { //+
        TTestWithReboots t(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            AsyncCreateSubDomain(runtime, ++t.TxId, "/MyRoot/DirA",
                                 "PlanResolution: 50 "
                                 "Coordinators: 1 "
                                 "Mediators: 1 "
                                 "TimeCastBucketsPerMediator: 2 "
                                 "Name: \"USER_0\" "
                                 "StoragePools { "
                                 "  Name: \"name_USER_0_kind_hdd-1\" "
                                 "  Kind: \"hdd-1\" "
                                 "} "
                                 "StoragePools { "
                                 "  Name: \"name_USER_0_kind_hdd-2\" "
                                 "  Kind: \"hdd-2\" "
                                 "}");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::Finished,
                                    NLs::SubdomainWithNoEmptyStoragePools,
                                    NLs::PathVersionEqual(3),
                                    NLs::PathsInsideDomain(0),
                                    NLs::ShardsInsideDomain(2)});

                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                   {NLs::PathVersionEqual(5),
                                    NLs::PathsInsideDomain(2),
                                    NLs::ShardsInsideDomain(0)});
            }
        });
    }

    Y_UNIT_TEST(RootWithStoragePools) { //+
        TTestWithReboots t(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::PathExist,
                                    NLs::StoragePoolsEqual({"pool-1", "pool-2"}),
                                    NLs::DomainCoordinators(TVector<ui64>{TTestTxConfig::Coordinator})});
            }

            AsyncAlterSubDomain(runtime, ++t.TxId,  "/",
                                "StoragePools { "
                                "  Name: \"pool-1\" "
                                "  Kind: \"pool-kind-1\" "
                                "} "
                                "StoragePools { "
                                "  Name: \"pool-2\" "
                                "  Kind: \"pool-kind-2\" "
                                "} "
                                "StoragePools { "
                                "  Name: \"name_USER_0_kind_hdd-1\" "
                                "  Kind: \"hdd-1\" "
                                "} "
                                "StoragePools { "
                                "  Name: \"name_USER_0_kind_hdd-2\" "
                                "  Kind: \"hdd-2\" "
                                "} "
                                "Name: \"MyRoot\"");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::PathExist,
                                    NLs::StoragePoolsEqual({"pool-1", "pool-2", "name_USER_0_kind_hdd-1", "name_USER_0_kind_hdd-2"})});
            }
        });
    }

    Y_UNIT_TEST(RootWithStoragePoolsAndTable) { //+
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestAlterSubDomain(runtime, ++t.TxId,  "/",
                                   "StoragePools { "
                                   "  Name: \"pool-1\" "
                                   "  Kind: \"pool-kind-1\" "
                                   "} "
                                   "StoragePools { "
                                   "  Name: \"pool-2\" "
                                   "  Kind: \"pool-kind-2\" "
                                   "} "
                                   "StoragePools { "
                                   "  Name: \"name_USER_0_kind_hdd-1\" "
                                   "  Kind: \"hdd-1\" "
                                   "} "
                                   "StoragePools { "
                                   "  Name: \"name_USER_0_kind_hdd-2\" "
                                   "  Kind: \"hdd-2\" "
                                   "} "
                                   "Name: \"MyRoot\"");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::PathExist,
                                    NLs::StoragePoolsEqual({"pool-1", "pool-2", "name_USER_0_kind_hdd-1", "name_USER_0_kind_hdd-2"})});
            }


            TestCreateTable(runtime, ++t.TxId, "/MyRoot",
                            "Name: \"table_0\""
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::PathExist,
                                    NLs::StoragePoolsEqual({"pool-1", "pool-2", "name_USER_0_kind_hdd-1", "name_USER_0_kind_hdd-2"})});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/table_0"),
                                   {NLs::PathExist,
                                    NLs::PathVersionEqual(3)});
            }

        });
    }

    Y_UNIT_TEST(Delete) { //+
        TTestWithReboots t(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, ++t.TxId, "/MyRoot/DirA",
                                    "PlanResolution: 50 "
                                    "Coordinators: 1 "
                                    "Mediators: 1 "
                                    "TimeCastBucketsPerMediator: 2 "
                                    "Name: \"USER_0\"");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            AsyncDropSubDomain(runtime, ++t.TxId,  "/MyRoot/DirA", "USER_0");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            t.TestEnv->TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::PathNotExist});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                   {NLs::PathVersionEqual(7),
                                    NLs::PathsInsideDomain(1),
                                    NLs::ShardsInsideDomain(0)});
                t.TestEnv->TestWaitShardDeletion(runtime, {1, 2});
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 3));
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 3));
            }
        });
    }

    Y_UNIT_TEST(DeleteWithStoragePools) { //+
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, ++t.TxId, "/MyRoot/DirA",
                                    "PlanResolution: 50 "
                                    "Coordinators: 1 "
                                    "Mediators: 1 "
                                    "TimeCastBucketsPerMediator: 2 "
                                    "Name: \"USER_0\""
                                    "StoragePools {"
                                    "  Name: \"name_USER_0_kind_hdd-1\""
                                    "  Kind: \"hdd-1\""
                                    "}"
                                    "StoragePools {"
                                    "  Name: \"name_USER_0_kind_hdd-2\""
                                    "  Kind: \"hdd-2\""
                                    "}");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropSubDomain(runtime, ++t.TxId,  "/MyRoot/DirA", "USER_0");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            t.TestEnv->TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::PathNotExist});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                   {NLs::PathVersionEqual(7),
                                    NLs::PathsInsideDomain(1),
                                    NLs::ShardsInsideDomain(0)});
                t.TestEnv->TestWaitShardDeletion(runtime, {1, 2});
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 3));
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 3));
            }
        });
    }

    Y_UNIT_TEST(DropSplittedTabletInsideWithStoragePools) { //+
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, ++t.TxId, "/MyRoot/DirA",
                                    "PlanResolution: 50 "
                                    "Coordinators: 1 "
                                    "Mediators: 1 "
                                    "TimeCastBucketsPerMediator: 2 "
                                    "Name: \"USER_0\""
                                    "StoragePools {"
                                    "  Name: \"name_USER_0_kind_hdd-1\""
                                    "  Kind: \"storage-pool-number-1\""
                                    "}"
                                    "StoragePools {"
                                    "  Name: \"name_USER_0_kind_hdd-2\""
                                    "  Kind: \"storage-pool-number-2\""
                                    "}");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                   {NLs::PathVersionEqual(5),
                                    NLs::PathsInsideDomain(2),
                                    NLs::ShardsInsideDomain(0)});

                TestCreateTable(runtime, ++t.TxId, "/MyRoot/DirA/USER_0",
                                "Name: \"table_0\""
                                "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                "KeyColumnNames: [\"RowId\"]"
                                );
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                   {NLs::PathVersionEqual(5),
                                    NLs::PathsInsideDomain(2),
                                    NLs::ShardsInsideDomain(0)});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::PathVersionEqual(5),
                                    NLs::PathsInsideDomain(1),
                                    NLs::ShardsInsideDomain(3)});

                TestSplitTable(runtime, ++t.TxId, "/MyRoot/DirA/USER_0/table_0",
                               R"(
                                    SourceTabletId: 72075186233409548
                                    SplitBoundary {
                                        KeyPrefix {
                                            Tuple { Optional { Uint64: 300 } }
                                        }
                                    }
                                    SplitBoundary {
                                        KeyPrefix {
                                            Tuple { Optional { Uint64: 600 } }
                                        }
                                    }
                                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                t.TestEnv->TestWaitTabletDeletion(runtime, TTestTxConfig::FakeHiveTablets+2); //delete src

                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::PathVersionEqual(5),
                                    NLs::PathsInsideDomain(1),
                                    NLs::ShardsInsideDomain(5)});

            }

            TestDropTable(runtime, ++t.TxId, "/MyRoot/DirA/USER_0", "table_0");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestForceDropSubDomain(runtime, ++t.TxId,  "/MyRoot/DirA", "USER_0");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 5));

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::PathNotExist});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                   {NLs::PathVersionEqual(7),
                                    NLs::PathsInsideDomain(1),
                                    NLs::ShardsInsideDomain(0)});
                // FIXME: DropTable breaks down during ForceDropSubDomain and leaves table half-dropped
                //        an additional reboot "fixes" the half-dropped table
                RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
                t.TestEnv->TestWaitShardDeletion(runtime, {1, 2, 3, 4, 5});
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 3));
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 3));
            }
        });
    }

    Y_UNIT_TEST(SplitTabletInsideWithStoragePools) { //+
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, ++t.TxId, "/MyRoot/DirA",
                                    "PlanResolution: 50 "
                                    "Coordinators: 1 "
                                    "Mediators: 1 "
                                    "TimeCastBucketsPerMediator: 2 "
                                    "Name: \"USER_0\""
                                    "StoragePools {"
                                    "  Name: \"name_USER_0_kind_hdd-1\""
                                    "  Kind: \"storage-pool-number-1\""
                                    "}"
                                    "StoragePools {"
                                    "  Name: \"name_USER_0_kind_hdd-2\""
                                    "  Kind: \"storage-pool-number-2\""
                                    "}");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                   {NLs::PathVersionEqual(5),
                                    NLs::PathsInsideDomain(2),
                                    NLs::ShardsInsideDomain(0)});

                TestCreateTable(runtime, ++t.TxId, "/MyRoot/DirA/USER_0",
                                "Name: \"table_0\""
                                "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                "KeyColumnNames: [\"RowId\"]"
                                );
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                   {NLs::PathVersionEqual(5),
                                    NLs::PathsInsideDomain(2),
                                    NLs::ShardsInsideDomain(0)});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::PathVersionEqual(5),
                                    NLs::PathsInsideDomain(1),
                                    NLs::ShardsInsideDomain(3)});
            }

            TestSplitTable(runtime, ++t.TxId, "/MyRoot/DirA/USER_0/table_0",
                           R"(
                                SourceTabletId: 72075186233409548
                                SplitBoundary {
                                    KeyPrefix {
                                        Tuple { Optional { Uint64: 300 } }
                                    }
                                }
                                SplitBoundary {
                                    KeyPrefix {
                                        Tuple { Optional { Uint64: 600 } }
                                    }
                                }
                            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::PathVersionEqual(5),
                                    NLs::PathsInsideDomain(1),
                                    NLs::ShardsInsideDomainOneOf({5, 6})});
                t.TestEnv->TestWaitTabletDeletion(runtime, TTestTxConfig::FakeHiveTablets+2); //delete src
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::PathVersionEqual(5),
                                    NLs::PathsInsideDomain(1),
                                    NLs::ShardsInsideDomain(5)});
            }
        });
    }

    Y_UNIT_TEST(CreateTabletInsideWithStoragePools) { //+
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, ++t.TxId, "/MyRoot/DirA",
                                    "PlanResolution: 50 "
                                    "Coordinators: 1 "
                                    "Mediators: 1 "
                                    "TimeCastBucketsPerMediator: 2 "
                                    "Name: \"USER_0\""
                                    "StoragePools {"
                                    "  Name: \"name_USER_0_kind_hdd-1\""
                                    "  Kind: \"storage-pool-number-1\""
                                    "}"
                                    "StoragePools {"
                                    "  Name: \"name_USER_0_kind_hdd-2\""
                                    "  Kind: \"storage-pool-number-2\""
                                    "}");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                   {NLs::PathVersionEqual(5),
                                    NLs::PathsInsideDomain(2),
                                    NLs::ShardsInsideDomain(0)});
            }

            TestCreateTable(runtime, ++t.TxId, "/MyRoot/DirA/USER_0",
                            "Name: \"table_0\""
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]"
                            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                   {NLs::PathVersionEqual(5),
                                    NLs::PathsInsideDomain(2),
                                    NLs::ShardsInsideDomain(0)});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::PathVersionEqual(5),
                                    NLs::PathsInsideDomain(1),
                                    NLs::ShardsInsideDomain(3)});
            }
        });
    }

}

Y_UNIT_TEST_SUITE(ForceDropWithReboots) {
    Y_UNIT_TEST(Fake) {
    }

    Y_UNIT_TEST(ForceDelete) { //+
        TTestWithReboots t(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, t.TxId, "/MyRoot/DirA",
                                    "PlanResolution: 50 "
                                    "Coordinators: 1 "
                                    "Mediators: 1 "
                                    "TimeCastBucketsPerMediator: 2 "
                                    "Name: \"USER_0\"");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 3));
                UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 3));
            }

            AsyncForceDropSubDomain(runtime, ++t.TxId,  "/MyRoot/DirA", "USER_0");

            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            t.TestEnv->TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::PathNotExist});
                t.TestEnv->TestWaitShardDeletion(runtime, {1, 2});
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 3));
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 3));
            }
        });
    }

    Y_UNIT_TEST(ForceDeleteCreateSubdomainInfly) { //+
        TTestWithReboots t(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {

            AsyncCreateSubDomain(runtime, ++t.TxId, "/MyRoot/DirA",
                                 "PlanResolution: 50 "
                                 "Coordinators: 1 "
                                 "Mediators: 1 "
                                 "TimeCastBucketsPerMediator: 2 "
                                 "Name: \"USER_0\"");
            AsyncForceDropSubDomain(runtime, ++t.TxId,  "/MyRoot/DirA", "USER_0");

            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId} );
            t.TestEnv->TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::PathNotExist});
                t.TestEnv->TestWaitShardDeletion(runtime, {1, 2});
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 3));
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 3));
            }
        });
    }

    Y_UNIT_TEST(ForceDeleteCreateTableInFlyWithRebootAtCommit) { //+
        TTestWithReboots t(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            AsyncCreateSubDomain(runtime, ++t.TxId, "/MyRoot/DirA",
                                 "PlanResolution: 50 "
                                 "Coordinators: 1 "
                                 "Mediators: 1 "
                                 "TimeCastBucketsPerMediator: 2 "
                                 "Name: \"USER_0\"");
            AsyncMkDir(runtime, ++t.TxId, "/MyRoot/DirA/USER_0", "dir");
            AsyncCreateTable(runtime, ++t.TxId, "/MyRoot/DirA/USER_0/dir",
                             "Name: \"table_0\""
                             "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                             "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                             "KeyColumnNames: [\"RowId\"]");

            AsyncForceDropSubDomain(runtime, ++t.TxId,  "/MyRoot/DirA", "USER_0");

            Cerr << Endl << "Wait notification " << Endl;
            t.TestEnv->TestWaitNotification(runtime, {t.TxId-3, t.TxId-2, t.TxId-1, t.TxId} );
            Cerr << Endl << "Ok notification " << Endl;

            t.TestEnv->TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1, TTestTxConfig::FakeHiveTablets+2});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::PathNotExist});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                   {NLs::PathVersionOneOf({5, 6}),
                                    NLs::PathsInsideDomain(1),
                                    NLs::ShardsInsideDomain(0)});
                t.TestEnv->TestWaitShardDeletion(runtime, {1, 2, 3});
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 3));
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 3));
            }
        });
    }

    Y_UNIT_TEST(ForceDeleteCreateTableInFly) { //+
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TestCreateSubDomain(runtime, ++t.TxId, "/MyRoot/DirA",
                                "PlanResolution: 50 "
                                "Coordinators: 1 "
                                "Mediators: 1 "
                                "TimeCastBucketsPerMediator: 2 "
                                "Name: \"USER_0\"");
            TestMkDir(runtime, ++t.TxId, "/MyRoot/DirA/USER_0", "dir");
            TestCreateTable(runtime, ++t.TxId, "/MyRoot/DirA/USER_0/dir",
                            "Name: \"table_0\""
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]");

            AsyncForceDropSubDomain(runtime, ++t.TxId,  "/MyRoot/DirA", "USER_0");

            Cerr << Endl << "Wait notification " << Endl;
            t.TestEnv->TestWaitNotification(runtime, {t.TxId-3, t.TxId-2, t.TxId-1, t.TxId} );
            Cerr << Endl << "Ok notification " << Endl;

            t.TestEnv->TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1, TTestTxConfig::FakeHiveTablets+2});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::PathNotExist});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                   {NLs::PathVersionOneOf({6, 7}),
                                    NLs::PathsInsideDomain(1),
                                    NLs::ShardsInsideDomain(0)});
                t.TestEnv->TestWaitShardDeletion(runtime, {1, 2, 3});
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 3));
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 3));
            }
        });
    }

    Y_UNIT_TEST(ForceDeleteSplitInFly) { //+
        TTestWithReboots t(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, ++t.TxId, "/MyRoot/DirA",
                                    "PlanResolution: 50 "
                                    "Coordinators: 1 "
                                    "Mediators: 1 "
                                    "TimeCastBucketsPerMediator: 2 "
                                    "Name: \"USER_0\"");
                TestMkDir(runtime, ++t.TxId, "/MyRoot/DirA/USER_0", "dir");
                TestCreateTable(runtime, ++t.TxId, "/MyRoot/DirA/USER_0/dir",
                                "Name: \"table_0\""
                                "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                "KeyColumnNames: [\"RowId\"]");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId-2, t.TxId-1, t.TxId} );

                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::PathVersionEqual(5),
                                    NLs::PathsInsideDomain(2),
                                    NLs::ShardsInsideDomain(3)});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                   {NLs::PathVersionEqual(5),
                                    NLs::PathsInsideDomain(2),
                                    NLs::ShardsInsideDomain(0)});
            }

            AsyncSplitTable(runtime, ++t.TxId, "/MyRoot/DirA/USER_0/dir/table_0", R"(
                                        SourceTabletId: 72075186233409548
                                        SplitBoundary {
                                            KeyPrefix {
                                                Tuple { Optional { Uint64: 3000000000 } }
                                            }
                                        })");

            AsyncForceDropSubDomain(runtime, ++t.TxId,  "/MyRoot/DirA", "USER_0");

            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});
            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 5));

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::PathNotExist});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                   {NLs::PathVersionEqual(7),
                                    NLs::PathsInsideDomain(1),
                                    NLs::ShardsInsideDomain(0)});
                t.TestEnv->TestWaitShardDeletion(runtime, {1, 2, 3, 4, 5});
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 3));
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 3));
            }
        });
    }

    Y_UNIT_TEST(ForceDropDeleteInFly) { //+
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, ++t.TxId, "/MyRoot/DirA",
                                    "PlanResolution: 50 "
                                    "Coordinators: 1 "
                                    "Mediators: 1 "
                                    "TimeCastBucketsPerMediator: 2 "
                                    "Name: \"USER_0\"");

                TestCreateTable(runtime, ++t.TxId, "/MyRoot/DirA/USER_0",
                                "Name: \"table_0\""
                                "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                "KeyColumnNames: [\"RowId\"]"
                                );
                t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});

                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::PathVersionEqual(5),
                                    NLs::PathsInsideDomain(1),
                                    NLs::ShardsInsideDomain(3)});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                   {NLs::PathVersionEqual(5),
                                    NLs::PathsInsideDomain(2),
                                    NLs::ShardsInsideDomain(0)});
            }

            TestDropTable(runtime, ++t.TxId, "/MyRoot/DirA/USER_0", "table_0");
            AsyncForceDropSubDomain(runtime, ++t.TxId,  "/MyRoot/DirA", "USER_0");
            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});
            t.TestEnv->TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1, TTestTxConfig::FakeHiveTablets+2});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::PathNotExist});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                   {NLs::PathVersionEqual(7),
                                    NLs::PathsInsideDomain(1),
                                    NLs::ShardsInsideDomain(0)});
                // FIXME: DropTable breaks down during ForceDropSubDomain and leaves table half-dropped
                //        an additional reboot "fixes" the half-dropped table
                RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
                t.TestEnv->TestWaitShardDeletion(runtime, {1, 2, 3});
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 3));
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 3));
            }
        });
    }

    Y_UNIT_TEST(DoNotLostDeletedTablets) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TPathVersion pathVersion;
            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, ++t.TxId, "/MyRoot",
                                    "PlanResolution: 50 "
                                    "Coordinators: 1 "
                                    "Mediators: 1 "
                                    "TimeCastBucketsPerMediator: 2 "
                                    "Name: \"USER_0\"");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                pathVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                                 {NLs::PathExist});

                TestCreateTable(runtime, ++t.TxId, "/MyRoot/USER_0",
                                "Name: \"Table1\""
                                "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                                "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                                "Columns { Name: \"key3\"       Type: \"Uint64\"}"
                                "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                "KeyColumnNames: [\"key1\", \"key2\", \"key3\"]"
                                "UniformPartitionsCount: 2"
                                );
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                // Write some data to the user table
                auto fnWriteRow = [&] (ui64 tabletId) {
                    TString writeQuery = R"(
                        (
                            (let key '( '('key1 (Uint32 '0)) '('key2 (Utf8 'aaaa)) '('key3 (Uint64 '0)) ) )
                            (let value '('('Value (Utf8 '281474980010683)) ) )
                            (return (AsList (UpdateRow '__user__Table1 key value) ))
                        )
                    )";
                    NKikimrMiniKQL::TResult result;
                    TString err;
                    NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
                    UNIT_ASSERT_VALUES_EQUAL(err, "");
                    UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
                };

                fnWriteRow(TTestTxConfig::FakeHiveTablets+2);
                SetAllowLogBatching(runtime, TTestTxConfig::FakeHiveTablets+2, false);

                fnWriteRow(TTestTxConfig::FakeHiveTablets+3);
                SetAllowLogBatching(runtime, TTestTxConfig::FakeHiveTablets+3, true);

                TestCopyTable(runtime, ++t.TxId, "/MyRoot/USER_0", "Table2", "/MyRoot/USER_0/Table1");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropTable(runtime, ++t.TxId, "/MyRoot/USER_0", "Table1");

                TDispatchOptions opts;
                opts.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvDataShard::EvSchemaChangedResult, 2));
                runtime.DispatchEvents(opts);

                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            // Drop the last table to trigger parts return
            TestForceDropUnsafe(runtime, ++t.TxId, pathVersion.PathId.LocalPathId);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 6));

        });
    }

    Y_UNIT_TEST(PathsAndShardsCountersSimultaneousAlterSubDomain) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, ++t.TxId, "/MyRoot",
                                    "PlanResolution: 50 "
                                    "Coordinators: 1 "
                                    "Mediators: 1 "
                                    "TimeCastBucketsPerMediator: 2 "
                                    "Name: \"USER_0\"");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot/USER_0",
                                 "Name: \"Table1\""
                                 "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                                 "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                                 "Columns { Name: \"key3\"       Type: \"Uint64\"}"
                                 "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                 "KeyColumnNames: [\"key1\", \"key2\", \"key3\"]"
                                 "UniformPartitionsCount: 1"
                                 );
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            AsyncMkDir(runtime, ++t.TxId, "/MyRoot/USER_0", "dir");
            AsyncCreateTable(runtime, ++t.TxId, "/MyRoot/USER_0",
                            "Name: \"Table2\""
                            "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                            "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                            "Columns { Name: \"key3\"       Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"key1\", \"key2\", \"key3\"]"
                            "UniformPartitionsCount: 1"
                            );
            t.TestEnv->ReliablePropose(runtime, AlterSubDomainRequest(++t.TxId,  "/",
                               "StoragePools { "
                               "  Name: \"pool-1\" "
                               "  Kind: \"pool-kind-1\" "
                               "} "
                               "StoragePools { "
                               "  Name: \"pool-2\" "
                               "  Kind: \"pool-kind-2\" "
                               "} "
                               "StoragePools { "
                               "  Name: \"name_USER_0_kind_hdd-1\" "
                               "  Kind: \"hdd-1\" "
                               "} "
                               "StoragePools { "
                               "  Name: \"name_USER_0_kind_hdd-2\" "
                               "  Kind: \"hdd-2\" "
                               "} "
                               "Name: \"MyRoot\""),
                               {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications});

            t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId - 1, t.TxId - 2});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                   {NLs::PathsInsideDomain(3),
                                    NLs::ShardsInsideDomain(4)});
            }
        });
    }
}
