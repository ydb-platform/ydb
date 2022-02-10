#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <util/system/env.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardUpgradeSubDomainWithOutDesicion) {
    Y_UNIT_TEST(Fake) {
    }

    Y_UNIT_TEST(Common) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TPathId pathId;

            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                "Name: \"USER_0\"");
                TestAlterSubDomain(runtime, ++t.TxId,  "/MyRoot",
                               "Name: \"USER_0\" "
                               "PlanResolution: 50 "
                               "Coordinators: 1 "
                               "Mediators: 1 "
                               "TimeCastBucketsPerMediator: 2");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId - 1});
                UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 3));
                UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 3));

                TestMkDir(runtime, ++t.TxId, "/MyRoot/USER_0", "DirB",
                          {NKikimrScheme::StatusAccepted}, AlterUserAttrs({{"AttrA1", "ValA1"}}));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot/USER_0/DirB", R"(
                                Name: "Table"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "A" } }}}
                                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                pathId = TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                            {NLs::IsSubDomain("USER_0")}).PathId;
            }

            TestUpgradeSubDomain(runtime, ++t.TxId,  "/MyRoot", "USER_0");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            ui64 tenantSchemeShard = 0;
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::IsExternalSubDomain("USER_0"),
                                NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
                                NLs::DomainKey(pathId)});

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirB"),
                               {NLs::PathRedirected});

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathIdEqual(TPathId(tenantSchemeShard, 1)),
                                NLs::IsSubDomain("MyRoot/USER_0"),
                                NLs::DomainKey(pathId),
                                NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                                NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1})});

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirB"),
                               {NLs::PathExist});
        });
    }

    Y_UNIT_TEST(WaitCreationDirAndTable) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TPathId pathId;

            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                    "Name: \"USER_0\"");
                TestAlterSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                   "Name: \"USER_0\" "
                                   "PlanResolution: 50 "
                                   "Coordinators: 1 "
                                   "Mediators: 1 "
                                   "TimeCastBucketsPerMediator: 2");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId - 1});

                TestMkDir(runtime, ++t.TxId, "/MyRoot/USER_0", "DirA",
                          {NKikimrScheme::StatusAccepted}, AlterUserAttrs({{"AttrA1", "ValA1"}}));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                pathId = TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                            {NLs::IsSubDomain("USER_0")}).PathId;

                TestCreateTable(runtime, ++t.TxId, "/MyRoot/USER_0/DirA", R"(
                                Name: "Table"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                )");
                TestMkDir(runtime, ++t.TxId, "/MyRoot/USER_0", "DirB");
                // No wait finish /MyRoot/USER_0/DirB /MyRoot/USER_0/DirA/Table
            }

            TestUpgradeSubDomain(runtime, ++t.TxId,  "/MyRoot", "USER_0");

            TestMkDir(runtime, ++t.TxId, "/MyRoot/USER_0/DirB", "DirC",
                      {NKikimrScheme::StatusMultipleModifications}); //creation doesn't chain, they see upgrade operation
            TestCreateTable(runtime, ++t.TxId, "/MyRoot/USER_0/DirB", R"(
                                Name: "Table"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                )",
                            {NKikimrScheme::StatusMultipleModifications});

            t.TestEnv->TestWaitNotification(runtime, {t.TxId-2, t.TxId-1, t.TxId});

            t.TestEnv->TestWaitNotification(runtime, {t.TxId-4, t.TxId-3});

            ui64 tenantSchemeShard = 0;
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::IsExternalSubDomain("USER_0"),
                                NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
                                NLs::DomainKey(pathId)});

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathIdEqual(TPathId(tenantSchemeShard, 1)),
                                NLs::IsSubDomain("MyRoot/USER_0"),
                                NLs::DomainKey(pathId),
                                NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                                NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1})});
        });
    }

    Y_UNIT_TEST(UpgradeRejectNewAlters) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TPathId pathId;

            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                    "Name: \"USER_0\"");
                TestAlterSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                   "Name: \"USER_0\" "
                                   "PlanResolution: 50 "
                                   "Coordinators: 1 "
                                   "Mediators: 1 "
                                   "TimeCastBucketsPerMediator: 2");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId - 1});

                pathId = TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                            {NLs::IsSubDomain("USER_0")}).PathId;

                TestMkDir(runtime, ++t.TxId, "/MyRoot/USER_0", "DirA");
                TestCreateTable(runtime, ++t.TxId, "/MyRoot/USER_0", R"(
                                Name: "Table"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "A" } }}}
                                )");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});
            }

            TestUpgradeSubDomain(runtime, ++t.TxId,  "/MyRoot", "USER_0"); //1008

            TestAlterTable(runtime, ++t.TxId, "/MyRoot/USER_0", R"(
                                Name: "Table"
                                Columns { Name: "addColumn2"  Type: "Uint64"}
                           )",
                           {TEvSchemeShard::EStatus::StatusMultipleModifications});
            TestUserAttrs(runtime, ++t.TxId, "/MyRoot/USER_0", "DirA",
                          {TEvSchemeShard::EStatus::StatusMultipleModifications},
                          AlterUserAttrs({{"AttrA2", "ValA2"}}));

            t.TestEnv->TestWaitNotification(runtime, {t.TxId-2, t.TxId-1, t.TxId});

            ui64 tenantSchemeShard = 0;
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::IsExternalSubDomain("USER_0"),
                                NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
                                NLs::DomainKey(pathId)});

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathIdEqual(TPathId(tenantSchemeShard, 1)),
                                NLs::IsSubDomain("MyRoot/USER_0"),
                                NLs::DomainKey(pathId),
                                NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                                NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1})});
        });
    }

    Y_UNIT_TEST(UpgradeWaitOldAltersAndRejectNewAlters) {
        TTestWithReboots t;

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TPathId pathId;

            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                    "Name: \"USER_0\"");
                TestAlterSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                   "Name: \"USER_0\" "
                                   "PlanResolution: 50 "
                                   "Coordinators: 1 "
                                   "Mediators: 1 "
                                   "TimeCastBucketsPerMediator: 2");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId - 1});

                pathId = TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                            {NLs::IsSubDomain("USER_0")}).PathId;

                TestMkDir(runtime, ++t.TxId, "/MyRoot/USER_0", "DirA");
                TestCreateTable(runtime, ++t.TxId, "/MyRoot/USER_0", R"(
                                Name: "Table"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                )");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});

                TestAlterTable(runtime, ++t.TxId, "/MyRoot/USER_0", R"(
                                Name: "Table"
                                Columns { Name: "addColumn1"  Type: "Uint64"}
                           )"); //1006
                TestUserAttrs(runtime, ++t.TxId, "/MyRoot/USER_0", "DirA", AlterUserAttrs({{"AttrA1", "ValA1"}})); //1007
            }

            TestUpgradeSubDomain(runtime, ++t.TxId,  "/MyRoot", "USER_0"); //1008

            t.TestEnv->TestWaitNotification(runtime, {t.TxId-2, t.TxId-1});

            TestAlterTable(runtime, ++t.TxId, "/MyRoot/USER_0", R"(
                                Name: "Table"
                                Columns { Name: "addColumn2"  Type: "Uint64"}
                           )",
                           {TEvSchemeShard::EStatus::StatusMultipleModifications});
            TestUserAttrs(runtime, ++t.TxId, "/MyRoot/USER_0", "DirA",
                          {TEvSchemeShard::EStatus::StatusMultipleModifications},
                          AlterUserAttrs({{"AttrA2", "ValA2"}}));

            t.TestEnv->TestWaitNotification(runtime, {t.TxId-2, t.TxId-1, t.TxId});


            t.TestEnv->TestWaitNotification(runtime, {t.TxId-4, t.TxId-3});

            ui64 tenantSchemeShard = 0;
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::IsExternalSubDomain("USER_0"),
                                NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
                                NLs::DomainKey(pathId)});

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathIdEqual(TPathId(tenantSchemeShard, 1)),
                                NLs::IsSubDomain("MyRoot/USER_0"),
                                NLs::DomainKey(pathId),
                                NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                                NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1})});
        });
    }

    Y_UNIT_TEST(UpgradeWaitOldSplitAndRejectNewSplit) {
        TTestWithReboots t;

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TPathId pathId;

            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                    "Name: \"USER_0\"");
                TestAlterSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                   "Name: \"USER_0\" "
                                   "PlanResolution: 50 "
                                   "Coordinators: 1 "
                                   "Mediators: 1 "
                                   "TimeCastBucketsPerMediator: 2");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId - 1});

                pathId = TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                            {NLs::IsSubDomain("USER_0")}).PathId;

                TestCreateTable(runtime, ++t.TxId, "/MyRoot/USER_0", R"(
                                Name: "Table"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "C" } }}}
                                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestSplitTable(runtime, ++t.TxId, "/MyRoot/USER_0/Table", R"(
                                SourceTabletId: 9437196
                                SplitBoundary {
                                    KeyPrefix {
                                        Tuple { Optional { Text: "B" } }
                                    }
                                }
                                )");
            }

            TestUpgradeSubDomain(runtime, ++t.TxId,  "/MyRoot", "USER_0"); //1008

            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);
            TestSplitTable(runtime, ++t.TxId, "/MyRoot/USER_0/Table", R"(
                                SourceTabletId: 9437197
                                SplitBoundary {
                                    KeyPrefix {
                                        Tuple { Optional { Text: "D" } }
                                    }
                                }
                            )",
                       {TEvSchemeShard::EStatus::StatusMultipleModifications});

            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});

            ui64 tenantSchemeShard = 0;
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::IsExternalSubDomain("USER_0"),
                                NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
                                NLs::DomainKey(pathId)});

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathIdEqual(TPathId(tenantSchemeShard, 1)),
                                NLs::IsSubDomain("MyRoot/USER_0"),
                                NLs::DomainKey(pathId),
                                NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                                NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                                NLs::ShardsInsideDomain(6)});
        });
    }
}

Y_UNIT_TEST_SUITE(TSchemeShardUpgradeSubDomainUndo) {
    Y_UNIT_TEST(Fake) {
    }

    Y_UNIT_TEST(Common) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TPathId pathId;

            ui64 tenantSchemeShard = 0;

            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                    "Name: \"USER_0\"");
                TestAlterSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                   "Name: \"USER_0\" "
                                   "PlanResolution: 50 "
                                   "Coordinators: 1 "
                                   "Mediators: 1 "
                                   "TimeCastBucketsPerMediator: 2");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId - 1});

                TestMkDir(runtime, ++t.TxId, "/MyRoot/USER_0", "DirB",
                          {NKikimrScheme::StatusAccepted}, AlterUserAttrs({{"AttrA1", "ValA1"}}));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot/USER_0/DirB", R"(
                                    Name: "Table"
                                    Columns { Name: "key1"       Type: "Utf8"}
                                    Columns { Name: "key2"       Type: "Uint32"}
                                    Columns { Name: "Value"      Type: "Utf8"}
                                    KeyColumnNames: ["key1", "key2"]
                                    SplitBoundary { KeyPrefix { Tuple { Optional { Text: "A" } }}}
                                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                pathId = TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                            {NLs::IsSubDomain("USER_0")}).PathId;

                TestUpgradeSubDomain(runtime, ++t.TxId,  "/MyRoot", "USER_0");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                   {NLs::PathExist,
                                    NLs::IsExternalSubDomain("USER_0"),
                                    NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
                                    NLs::DomainKey(pathId)});

                UNIT_ASSERT_UNEQUAL(tenantSchemeShard, 0);
            }


            TestUpgradeSubDomainDecision(runtime, ++t.TxId,  "/MyRoot", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Undo);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::IsSubDomain("USER_0"),
                                NLs::DomainKey(pathId)});

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirB"),
                               {NLs::PathExist});

            t.TestEnv->TestWaitTabletDeletion(runtime, tenantSchemeShard);
        });
    }
}


Y_UNIT_TEST_SUITE(TSchemeShardUpgradeSubDomainCommit) {
    Y_UNIT_TEST(Kesus) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TPathId pathId;

            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                    "Name: \"USER_0\"");
                TestAlterSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                   "Name: \"USER_0\" "
                                   "PlanResolution: 50 "
                                   "Coordinators: 1 "
                                   "Mediators: 1 "
                                   "TimeCastBucketsPerMediator: 2");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId - 1});

                TestMkDir(runtime, ++t.TxId, "/MyRoot/USER_0", "DirB",
                          {NKikimrScheme::StatusAccepted}, AlterUserAttrs({{"AttrA1", "ValA1"}}));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateKesus(runtime, ++t.TxId, "/MyRoot/USER_0",
                                "Name: \"Kesus1\" "
                                "Config: { self_check_period_millis: 1234 session_grace_period_millis: 5678 }");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/Kesus1"),
                                   {NLs::Finished, NLs::KesusConfigIs(1234, 5678)});

                pathId = TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                            {NLs::IsSubDomain("USER_0")}).PathId;
            }

            TestUpgradeSubDomain(runtime, ++t.TxId,  "/MyRoot", "USER_0");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            ui64 tenantSchemeShard = 0;
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::IsExternalSubDomain("USER_0"),
                                NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
                                NLs::DomainKey(pathId)});

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/Kesus1"),
                               {NLs::PathRedirected});

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/Kesus1"),
                               {NLs::Finished, NLs::KesusConfigIs(1234, 5678)});

            TestUpgradeSubDomainDecision(runtime, ++t.TxId,  "/MyRoot", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Commit);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirB"),
                               {NLs::PathRedirected});

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathIdEqual(TPathId(tenantSchemeShard, 1)),
                                NLs::IsSubDomain("MyRoot/USER_0"),
                                NLs::DomainKey(pathId),
                                NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                                NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1})});

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirB"),
                               {NLs::PathExist});

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/Kesus1"),
                               {NLs::Finished, NLs::KesusConfigIs(1234, 5678)});

            TestAlterKesus(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/USER_0",
                           "Name: \"Kesus1\" "
                           "Config: { self_check_period_millis: 2345 }");
            t.TestEnv-> TestWaitNotification(runtime, t.TxId, tenantSchemeShard);

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/Kesus1"),
                               {NLs::Finished, NLs::KesusConfigIs(2345, 5678)});

            TestDropKesus(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/USER_0", "Kesus1");
            t.TestEnv-> TestWaitNotification(runtime, t.TxId, tenantSchemeShard);

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/Kesus1"),
                               {NLs::PathNotExist});
        });
    }

    Y_UNIT_TEST(DeleteAfter) {
        TTestWithReboots t;

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, ++t.TxId,  "/MyRoot", "Name: \"USER_0\"");
                TestAlterSubDomain(runtime, ++t.TxId,  "/MyRoot", R"(
                                Name: "USER_0"
                                PlanResolution: 50
                                Coordinators: 1
                                Mediators: 1
                                TimeCastBucketsPerMediator: 2
                                )");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId - 1});

                TestMkDir(runtime, ++t.TxId, "/MyRoot/USER_0", "DirA");
                TestCreateTable(runtime, ++t.TxId, "/MyRoot/USER_0/DirA",R"(
                                Name: "Table1"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "C" } }}}
                                )");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId -1, t.TxId});

                TestUpgradeSubDomain(runtime, ++t.TxId,  "/MyRoot", "USER_0");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestUpgradeSubDomainDecision(runtime, ++t.TxId,  "/MyRoot", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Commit);
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                ui64 tenantSchemeShard = 0;
                TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                   {NLs::PathExist,
                                    NLs::IsExternalSubDomain("USER_0"),
                                    NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});
                TestCreateTable(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/USER_0",R"(
                                Name: "Table3"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId, tenantSchemeShard);

                RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor()); // one more reboot before propose deletion
            }

            TestForceDropExtSubDomain(runtime, ++t.TxId,  "/MyRoot", "USER_0"); //110
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                               {NLs::PathExist,
                                NLs::ChildrenCount(1),
                                NLs::PathsInsideDomain(1),
                                NLs::ShardsInsideDomain(0)});

            t.TestEnv->TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+6));
            {
                TInactiveZone inactive(activeZone);
                t.TestEnv->TestWaitShardDeletion(runtime, {1, 2, 3, 4, 5, 6});
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 3));
                UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 3));
            }
        });
    }

    Y_UNIT_TEST(Common) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TPathId pathId;

            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                    "Name: \"USER_0\"");
                TestAlterSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                   "Name: \"USER_0\" "
                                   "PlanResolution: 50 "
                                   "Coordinators: 1 "
                                   "Mediators: 1 "
                                   "TimeCastBucketsPerMediator: 2");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId - 1});

                TestMkDir(runtime, ++t.TxId, "/MyRoot/USER_0", "DirB",
                          {NKikimrScheme::StatusAccepted}, AlterUserAttrs({{"AttrA1", "ValA1"}}));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot/USER_0/DirB", R"(
                                Name: "Table"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "A" } }}}
                                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                pathId = TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                                            {NLs::IsSubDomain("USER_0")}).PathId;

                TestUpgradeSubDomain(runtime, ++t.TxId,  "/MyRoot", "USER_0");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestUpgradeSubDomainDecision(runtime, ++t.TxId,  "/MyRoot", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Commit);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            ui64 tenantSchemeShard = 0;
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::IsExternalSubDomain("USER_0"),
                                NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
                                NLs::DomainKey(pathId)});

            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirB"),
                               {NLs::PathRedirected});

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::PathIdEqual(TPathId(tenantSchemeShard, 1)),
                                NLs::IsSubDomain("MyRoot/USER_0"),
                                NLs::DomainKey(pathId),
                                NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                                NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1})});

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirB"),
                               {NLs::PathExist});
        });
    }

    Y_UNIT_TEST(ChangeACLRmDir) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestCreateSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                    "Name: \"USER_0\"");
                TestAlterSubDomain(runtime, ++t.TxId,  "/MyRoot",R"(
                                        Name: "USER_0"
                                        PlanResolution: 50
                                        Coordinators: 1
                                        Mediators: 1
                                        TimeCastBucketsPerMediator: 2
                                        )");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId - 1});

                TestMkDir(runtime, ++t.TxId, "/MyRoot/USER_0", "DirA");
                TestMkDir(runtime, ++t.TxId, "/MyRoot/USER_0/DirA", "DirB");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId - 1, t.TxId});

                TestUpgradeSubDomain(runtime, ++t.TxId,  "/MyRoot", "USER_0");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestUpgradeSubDomainDecision(runtime, ++t.TxId,  "/MyRoot", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Commit);
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            ui64 tenantSchemeShard = 0;
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::IsExternalSubDomain("USER_0"),
                                NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

            TestModifyACL(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/USER_0", "DirA", "", "svc@staff");
            TestModifyACL(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/USER_0/DirA", "DirB", "", "svc@staff");
            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId}, tenantSchemeShard);

            TestRmDir(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/USER_0/DirA", "DirB");
            t.TestEnv->TestWaitNotification(runtime, t.TxId, tenantSchemeShard);

            TestRmDir(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/USER_0", "DirA");
            t.TestEnv->TestWaitNotification(runtime, t.TxId, tenantSchemeShard);

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA"),
                               {NLs::PathNotExist});
        });

    }

    Y_UNIT_TEST(DropTable) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestCreateSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                    "Name: \"USER_0\"");
                TestAlterSubDomain(runtime, ++t.TxId,  "/MyRoot",R"(
                                        Name: "USER_0"
                                        PlanResolution: 50
                                        Coordinators: 1
                                        Mediators: 1
                                        TimeCastBucketsPerMediator: 2
                                        )");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId - 1});

                TestMkDir(runtime, ++t.TxId, "/MyRoot/USER_0", "DirA");
                TestCreateTable(runtime, ++t.TxId, "/MyRoot/USER_0/DirA",R"(
                                        Name: "Table"
                                        Columns { Name: "key1"       Type: "Utf8"}
                                        Columns { Name: "key2"       Type: "Uint32"}
                                        Columns { Name: "Value"      Type: "Utf8"}
                                        KeyColumnNames: ["key1", "key2"]
                                        SplitBoundary { KeyPrefix { Tuple { Optional { Text: "C" } }}}
                                        )");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId - 1, t.TxId});

                TestUpgradeSubDomain(runtime, ++t.TxId,  "/MyRoot", "USER_0"); //106
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestUpgradeSubDomainDecision(runtime, ++t.TxId,  "/MyRoot", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Commit); //107
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            ui64 tenantSchemeShard = 0;
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::IsExternalSubDomain("USER_0"),
                                NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA/Table"),
                               {NLs::PathExist,
                                NLs::IsTable});

            TestDropTable(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/USER_0/DirA", "Table");
            t.TestEnv->TestWaitNotification(runtime, t.TxId, tenantSchemeShard);

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA/Table"),
                               {NLs::PathNotExist});
            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA"),
                               {NLs::PathExist,
                                NLs::ChildrenCount(0)});

            TestRmDir(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/USER_0", "DirA");
            t.TestEnv->TestWaitNotification(runtime, t.TxId, tenantSchemeShard);

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA"),
                               {NLs::PathNotExist});
        });

    }

    Y_UNIT_TEST(CreateAndDropInsideMigrated) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestCreateSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                    "Name: \"USER_0\"");
                TestAlterSubDomain(runtime, ++t.TxId,  "/MyRoot",R"(
                                        Name: "USER_0"
                                        PlanResolution: 50
                                        Coordinators: 1
                                        Mediators: 1
                                        TimeCastBucketsPerMediator: 2
                                        )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestMkDir(runtime, ++t.TxId, "/MyRoot/USER_0", "DirA");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId - 1, t.TxId});

                TestUpgradeSubDomain(runtime, ++t.TxId,  "/MyRoot", "USER_0"); //106
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestUpgradeSubDomainDecision(runtime, ++t.TxId,  "/MyRoot", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Commit); //107
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            ui64 tenantSchemeShard = 0;
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::IsExternalSubDomain("USER_0"),
                                NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

            TestCreateTable(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/USER_0/DirA",R"(
                                        Name: "Table"
                                        Columns { Name: "key1"       Type: "Utf8"}
                                        Columns { Name: "key2"       Type: "Uint32"}
                                        Columns { Name: "Value"      Type: "Utf8"}
                                        KeyColumnNames: ["key1", "key2"]
                                        )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId, tenantSchemeShard);

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA/Table"),
                               {NLs::PathExist,
                                NLs::IsTable});

            TestDropTable(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/USER_0/DirA", "Table");
            t.TestEnv->TestWaitNotification(runtime, t.TxId, tenantSchemeShard);

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA/Table"),
                               {NLs::PathNotExist});
            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA"),
                               {NLs::PathExist,
                                NLs::ChildrenCount(0)});

            TestRmDir(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/USER_0", "DirA");
            t.TestEnv->TestWaitNotification(runtime, t.TxId, tenantSchemeShard);

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA"),
                               {NLs::PathNotExist});
        });

    }

    Y_UNIT_TEST(ChangeUserAttrs) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TUserAttrs attrs1({{"Name1", "Val1"}});
            TUserAttrs attrs2({{"Name2", "Val2"}});

            {
                TInactiveZone inactive(activeZone);

                TestCreateSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                    "Name: \"USER_0\"");
                TestAlterSubDomain(runtime, ++t.TxId,  "/MyRoot",R"(
                                        Name: "USER_0"
                                        PlanResolution: 50
                                        Coordinators: 1
                                        Mediators: 1
                                        TimeCastBucketsPerMediator: 2
                                        )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestMkDir(runtime, ++t.TxId, "/MyRoot/USER_0", "DirA", {NKikimrScheme::StatusAccepted}, AlterUserAttrs(attrs1));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestUpgradeSubDomain(runtime, ++t.TxId,  "/MyRoot", "USER_0");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestUpgradeSubDomainDecision(runtime, ++t.TxId,  "/MyRoot", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Commit);
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            ui64 tenantSchemeShard = 0;
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::IsExternalSubDomain("USER_0"),
                                NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

            TestUserAttrs(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/USER_0", "DirA",
                {NKikimrScheme::StatusAccepted}, AlterUserAttrs(attrs2, {"Name1"}));
            t.TestEnv->TestWaitNotification(runtime, t.TxId, tenantSchemeShard);

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA"),
                               {NLs::PathExist,
                                NLs::UserAttrsEqual(attrs2)});

            TestRmDir(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/USER_0", "DirA");
            t.TestEnv->TestWaitNotification(runtime, t.TxId, tenantSchemeShard);

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA"),
                               {NLs::PathNotExist});
        });

    }

    Y_UNIT_TEST(CommitSplit) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestCreateSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                    "Name: \"USER_0\"");
                TestAlterSubDomain(runtime, ++t.TxId,  "/MyRoot",R"(
                                        Name: "USER_0"
                                        PlanResolution: 50
                                        Coordinators: 1
                                        Mediators: 1
                                        TimeCastBucketsPerMediator: 2
                                        )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestMkDir(runtime, ++t.TxId, "/MyRoot/USER_0", "DirA");
                TestCreateTable(runtime, ++t.TxId, "/MyRoot/USER_0/DirA",R"(
                                        Name: "Table"
                                        Columns { Name: "key1"       Type: "Utf8"}
                                        Columns { Name: "key2"       Type: "Uint32"}
                                        Columns { Name: "Value"      Type: "Utf8"}
                                        KeyColumnNames: ["key1", "key2"]
                                        )");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId - 1, t.TxId});

                TestUpgradeSubDomain(runtime, ++t.TxId,  "/MyRoot", "USER_0");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestUpgradeSubDomainDecision(runtime, ++t.TxId,  "/MyRoot", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Commit);
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            ui64 tenantSchemeShard = 0;
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::IsExternalSubDomain("USER_0"),
                                NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA/Table", true),
                               {NLs::PathExist,
                                NLs::PartitionCount(1)});

            TestSplitTable(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/USER_0/DirA/Table", R"(
                                SourceTabletId: 9437196
                                SplitBoundary  {
                                    KeyPrefix {
                                        Tuple { Optional { Text: "Marla" } }
                                    }
                                }
                            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId, tenantSchemeShard);

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA/Table", true),
                               {NLs::PathExist,
                                NLs::PartitionCount(2)});

            t.TestEnv->TestWaitTabletDeletion(runtime, ui64{9437196});

            {
                TInactiveZone inactive(activeZone);

                TestDropTable(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/USER_0/DirA", "Table");
                t.TestEnv->TestWaitNotification(runtime, t.TxId, tenantSchemeShard);
            }

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA/Table"),
                               {NLs::PathNotExist});

            t.TestEnv->TestWaitTabletDeletion(runtime, {9437198, 9437199});

        });

    }

    Y_UNIT_TEST(AlterMigratedTable) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestCreateSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                    "Name: \"USER_0\"");
                TestAlterSubDomain(runtime, ++t.TxId,  "/MyRoot",R"(
                                        Name: "USER_0"
                                        PlanResolution: 50
                                        Coordinators: 1
                                        Mediators: 1
                                        TimeCastBucketsPerMediator: 2
                                        )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestMkDir(runtime, ++t.TxId, "/MyRoot/USER_0", "DirA");
                TestCreateTable(runtime, ++t.TxId, "/MyRoot/USER_0/DirA",R"(
                                        Name: "Table"
                                        Columns { Name: "key1"       Type: "Utf8"}
                                        Columns { Name: "val1"      Type: "Utf8"}
                                        KeyColumnNames: ["key1"]
                                        )");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId - 1, t.TxId});

                TestUpgradeSubDomain(runtime, ++t.TxId,  "/MyRoot", "USER_0");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestUpgradeSubDomainDecision(runtime, ++t.TxId,  "/MyRoot", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Commit);
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            ui64 tenantSchemeShard = 0;
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::IsExternalSubDomain("USER_0"),
                                NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA/Table", true),
                               {NLs::PathExist,
                                NLs::PartitionCount(1)});

            TestAlterTable(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/USER_0/DirA", R"(
                                Name: "Table"
                                Columns { Name: "add"  Type: "Uint64"}
                           )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId, tenantSchemeShard);

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA/Table"),
                               {NLs::PathExist,
                                NLs::CheckColumns("Table", {"key1", "val1", "add"}, {}, {"key1"})});
        });

    }

    Y_UNIT_TEST(CopyMigratedTable) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestCreateSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                    "Name: \"USER_0\"");
                TestAlterSubDomain(runtime, ++t.TxId,  "/MyRoot",R"(
                                        Name: "USER_0"
                                        PlanResolution: 50
                                        Coordinators: 1
                                        Mediators: 1
                                        TimeCastBucketsPerMediator: 2
                                        )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestMkDir(runtime, ++t.TxId, "/MyRoot/USER_0", "DirA");
                TestCreateTable(runtime, ++t.TxId, "/MyRoot/USER_0/DirA",R"(
                                        Name: "Table"
                                        Columns { Name: "key1"       Type: "Utf8"}
                                        Columns { Name: "val1"      Type: "Utf8"}
                                        KeyColumnNames: ["key1"]
                                        )");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId - 1, t.TxId});

                TestUpgradeSubDomain(runtime, ++t.TxId,  "/MyRoot", "USER_0");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestUpgradeSubDomainDecision(runtime, ++t.TxId,  "/MyRoot", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Commit);
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            ui64 tenantSchemeShard = 0;
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                               {NLs::PathExist,
                                NLs::IsExternalSubDomain("USER_0"),
                                NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA/Table", true),
                               {NLs::PathExist,
                                NLs::PartitionCount(1)});

            TestCopyTable(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/USER_0/DirA", "Copy", "/MyRoot/USER_0/DirA/Table");
            t.TestEnv->TestWaitNotification(runtime, t.TxId, tenantSchemeShard);

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA/Copy", true),
                               {NLs::PathExist,
                                NLs::PartitionCount(1)});
        });
    }

    Y_UNIT_TEST(WithIndexes) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestCreateSubDomain(runtime, ++t.TxId,  "/MyRoot",
                                    "Name: \"USER_0\"");
                TestAlterSubDomain(runtime, ++t.TxId,  "/MyRoot",R"(
                                        Name: "USER_0"
                                        PlanResolution: 50
                                        Coordinators: 1
                                        Mediators: 1
                                        TimeCastBucketsPerMediator: 2
                                        )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestMkDir(runtime, ++t.TxId, "/MyRoot/USER_0", "DirA");
                TestCreateIndexedTable(runtime, ++t.TxId, "/MyRoot/USER_0/DirA", R"(
                                            TableDescription {
                                                Name: "Table"
                                                Columns { Name: "key1"       Type: "Utf8"}
                                                Columns { Name: "value0"      Type: "Utf8"}
                                                KeyColumnNames: ["key1"]
                                            }
                                            IndexDescription {
                                              Name: "UserDefinedIndexByValue0"
                                              KeyColumnNames: ["value0"]
                                            }
                                        )");
                t.TestEnv->TestWaitNotification(runtime, {t.TxId - 1, t.TxId});

                TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirA/Table", true),
                                   {NLs::PathExist,
                                    NLs::PartitionCount(1),
                                    NLs::IndexesCount(1)});
            }

            TestUpgradeSubDomain(runtime, ++t.TxId,  "/MyRoot", "USER_0");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestUpgradeSubDomainDecision(runtime, ++t.TxId,  "/MyRoot", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Commit);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            ui64 tenantSchemeShard = 0;
            TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirA/Table"),
                               {NLs::PathRedirected,
                                NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA/Table", true),
                               {NLs::PathExist,
                                NLs::PartitionCount(1),
                                NLs::IndexesCount(1)});

            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA/Table/UserDefinedIndexByValue0/indexImplTable", true, true, true),
                               {NLs::PathExist,
                                NLs::IsTable,
                                NLs::PartitionCount(1)});

            {
                TInactiveZone inactive(activeZone);

                TestDropTable(runtime, tenantSchemeShard, ++t.TxId, "/MyRoot/USER_0/DirA", "Table");
                t.TestEnv->TestWaitNotification(runtime, t.TxId, tenantSchemeShard);

                t.TestEnv->TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets + 2, TTestTxConfig::FakeHiveTablets + 3});
            }
        });
    }
}
