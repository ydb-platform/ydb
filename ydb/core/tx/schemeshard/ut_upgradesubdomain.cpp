#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <util/system/env.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardUpgradeSubDomainTest) {
    Y_UNIT_TEST(Fake) {
    }

    Y_UNIT_TEST(UpgradeEmptyDomainCommit) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, ++txId,  "/MyRoot",
                            "Name: \"USER_0\"");
        TestAlterSubDomain(runtime, ++txId,  "/MyRoot",
                           "Name: \"USER_0\" "
                           "PlanResolution: 50 "
                           "Coordinators: 1 "
                           "Mediators: 1 "
                           "TimeCastBucketsPerMediator: 2");
        env.TestWaitNotification(runtime, {txId, txId - 1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsSubDomain("USER_0"),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::PathVersionEqual(4)});

        TestUpgradeSubDomain(runtime, ++txId,  "/MyRoot", "USER_0");
        env.TestWaitNotification(runtime, txId);

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::PathVersionEqual(5)});

        UNIT_ASSERT(tenantSchemeShard == TTestTxConfig::FakeHiveTablets + 2);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::PathIdEqual(1),
                            NLs::IsSubDomain("MyRoot/USER_0"),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1})});

        TestMkDir(runtime, ++txId, "/MyRoot/USER_0", "DirA", {NKikimrScheme::StatusMultipleModifications});
        TestMkDir(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0", "DirA", {NKikimrScheme::StatusReadOnly});


        TestUpgradeSubDomainDecision(runtime, ++txId,  "/MyRoot", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Commit);
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::PathVersionEqual(6)});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::PathIdEqual(1),
                            NLs::IsSubDomain("MyRoot/USER_0"),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1})});

        TestMkDir(runtime, ++txId, "/MyRoot/USER_0", "DirA", {NKikimrScheme::StatusRedirectDomain});
        TestMkDir(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0", "DirA");
        env.TestWaitNotification(runtime, txId);

    }

    Y_UNIT_TEST(UpgradeEmptyDomainUndo) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, ++txId,  "/MyRoot",
                            "Name: \"USER_0\"");
        TestAlterSubDomain(runtime, ++txId,  "/MyRoot",
                           "Name: \"USER_0\" "
                           "PlanResolution: 50 "
                           "Coordinators: 1 "
                           "Mediators: 1 "
                           "TimeCastBucketsPerMediator: 2");
        env.TestWaitNotification(runtime, {txId, txId - 1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsSubDomain("USER_0"),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::PathVersionEqual(4)});

        TestUpgradeSubDomain(runtime, ++txId,  "/MyRoot", "USER_0");
        env.TestWaitNotification(runtime, txId);

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::DomainSchemeshard(TTestTxConfig::FakeHiveTablets + 2),
                            NLs::PathVersionEqual(5)});

        UNIT_ASSERT(tenantSchemeShard == TTestTxConfig::FakeHiveTablets + 2);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::PathIdEqual(1),
                            NLs::IsSubDomain("MyRoot/USER_0"),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1})});

        TestUpgradeSubDomainDecision(runtime, ++txId,  "/MyRoot", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Undo); // 104
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsSubDomain("USER_0"),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::DomainSchemeshard(0),
                            NLs::PathVersionEqual(105)});

        TestMkDir(runtime, ++txId, "/MyRoot/USER_0", "DirA");
        env.TestWaitNotification(runtime, txId);

        env.TestWaitTabletDeletion(runtime, TTestTxConfig::FakeHiveTablets + 2);

    }

    Y_UNIT_TEST(UpgradeDomainWithDirsCommit) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, ++txId,  "/MyRoot",
                            "Name: \"USER_0\"");
        TestAlterSubDomain(runtime, ++txId,  "/MyRoot",
                           "Name: \"USER_0\" "
                           "PlanResolution: 50 "
                           "Coordinators: 1 "
                           "Mediators: 1 "
                           "TimeCastBucketsPerMediator: 2");
        env.TestWaitNotification(runtime, {txId, txId - 1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsSubDomain("USER_0"),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::PathVersionEqual(4),
                            NLs::NoChildren});

        TestMkDir(runtime, ++txId, "/MyRoot/USER_0", "DirA");
        env.TestWaitNotification(runtime, txId);
        TUserAttrs userAttrs({{"ATTR1", "VALUE1"}, {"ATTR2", "VALUE2"}});
        TestMkDir(runtime, ++txId, "/MyRoot/USER_0", "DirB", {NKikimrScheme::StatusAccepted}, AlterUserAttrs(userAttrs));
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot/USER_0", "DirA/DirC");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirA"),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::PathIdEqual(NKikimr::TPathId(TTestTxConfig::SchemeShard, 3)),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::ChildrenCount(1)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirB"),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::PathIdEqual(NKikimr::TPathId(TTestTxConfig::SchemeShard, 4)),
                            NLs::NoChildren});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirA/DirC"),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::PathIdEqual(NKikimr::TPathId(TTestTxConfig::SchemeShard, 5)),
                            NLs::NoChildren});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsSubDomain("USER_0"),
                            NLs::PathVersionEqual(8),
                            NLs::ChildrenCount(2)});

        TestUpgradeSubDomain(runtime, ++txId,  "/MyRoot", "USER_0");
        env.TestWaitNotification(runtime, txId);

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::DomainSchemeshard(TTestTxConfig::FakeHiveTablets + 2),
                            NLs::PathVersionEqual(9)});
        UNIT_ASSERT(tenantSchemeShard == TTestTxConfig::FakeHiveTablets + 2);
        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::PathIdEqual(1),
                            NLs::IsSubDomain("MyRoot/USER_0"),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1})});

        TestUpgradeSubDomainDecision(runtime, ++txId,  "/MyRoot", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Commit);
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::DomainSchemeshard(tenantSchemeShard),
                            NLs::PathVersionEqual(10),
                            NLs::NoChildren});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirA"),
                           {NLs::PathRedirected});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::PathIdEqual(NKikimr::TPathId(tenantSchemeShard, 1)),
                            NLs::IsSubDomain("MyRoot/USER_0"),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::ChildrenCount(2)});
        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA"),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::PathIdEqual(NKikimr::TPathId(TTestTxConfig::SchemeShard, 3)),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::ChildrenCount(1)});
        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirB"),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::PathIdEqual(NKikimr::TPathId(TTestTxConfig::SchemeShard, 4)),
                            NLs::NoChildren,
                            NLs::UserAttrsEqual(userAttrs)});
        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA/DirC"),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::PathIdEqual(NKikimr::TPathId(TTestTxConfig::SchemeShard, 5)),
                            NLs::NoChildren});
    }

    Y_UNIT_TEST(UpgradeDomainWithDirsUndo) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, ++txId,  "/MyRoot",
                            "Name: \"USER_0\"");
        TestAlterSubDomain(runtime, ++txId,  "/MyRoot",
                           "Name: \"USER_0\" "
                           "PlanResolution: 50 "
                           "Coordinators: 1 "
                           "Mediators: 1 "
                           "TimeCastBucketsPerMediator: 2");
        env.TestWaitNotification(runtime, {txId, txId - 1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsSubDomain("USER_0"),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::PathVersionEqual(4),
                            NLs::NoChildren});

        TestMkDir(runtime, ++txId, "/MyRoot/USER_0", "DirA");
        env.TestWaitNotification(runtime, txId);
        TUserAttrs userAttrs({{"ATTR1", "VALUE1"}, {"ATTR2", "VALUE2"}});
        TestMkDir(runtime, ++txId, "/MyRoot/USER_0", "DirB", {NKikimrScheme::StatusAccepted}, AlterUserAttrs(userAttrs));
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot/USER_0", "DirA/DirC");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirA"),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::PathIdEqual(NKikimr::TPathId(TTestTxConfig::SchemeShard, 3)),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::ChildrenCount(1)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirB"),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::PathIdEqual(NKikimr::TPathId(TTestTxConfig::SchemeShard, 4)),
                            NLs::NoChildren});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirA/DirC"),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::PathIdEqual(NKikimr::TPathId(TTestTxConfig::SchemeShard, 5)),
                            NLs::NoChildren});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsSubDomain("USER_0"),
                            NLs::PathVersionEqual(8),
                            NLs::ChildrenCount(2),
                            NLs::PathsInsideDomain(3)});

        TestUpgradeSubDomain(runtime, ++txId,  "/MyRoot", "USER_0");
        env.TestWaitNotification(runtime, txId);

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::DomainSchemeshard(TTestTxConfig::FakeHiveTablets + 2),
                            NLs::PathVersionEqual(9)});
        UNIT_ASSERT(tenantSchemeShard == TTestTxConfig::FakeHiveTablets + 2);
        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::PathIdEqual(1),
                            NLs::IsSubDomain("MyRoot/USER_0"),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1})});

        TestUpgradeSubDomainDecision(runtime, ++txId,  "/MyRoot", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Undo);
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsSubDomain("USER_0"),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::DomainSchemeshard(0),
                            NLs::PathVersionEqual(109),
                            NLs::ChildrenCount(2),
                            NLs::PathsInsideDomain(3)});

        env.TestWaitTabletDeletion(runtime, TTestTxConfig::FakeHiveTablets + 2);
    }

    Y_UNIT_TEST(UpgradeDomainWithTablesCommit) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, ++txId,  "/MyRoot",
                            "Name: \"USER_0\"");
        TestAlterSubDomain(runtime, ++txId,  "/MyRoot",
                           "Name: \"USER_0\" "
                           "PlanResolution: 50 "
                           "Coordinators: 1 "
                           "Mediators: 1 "
                           "TimeCastBucketsPerMediator: 2");
        env.TestWaitNotification(runtime, {txId, txId - 1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsSubDomain("USER_0"),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::PathVersionEqual(4),
                            NLs::NoChildren,
                            NLs::ShardsInsideDomain(2)});

        TestMkDir(runtime, ++txId, "/MyRoot/USER_0", "DirA");
        TUserAttrs userAttrs({{"ATTR1", "VALUE1"}, {"ATTR2", "VALUE2"}});
        TestMkDir(runtime, ++txId, "/MyRoot/USER_0", "DirB", {NKikimrScheme::StatusAccepted}, AlterUserAttrs(userAttrs));
        TestMkDir(runtime, ++txId, "/MyRoot/USER_0", "DirA/DirC");

        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0/DirA",
                        "Name: \"table_1\""
                        "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                        "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                        "KeyColumnNames: [\"RowId\"]");
        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0/DirB",
                        "Name: \"table_2\""
                        "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                        "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                        "KeyColumnNames: [\"RowId\"]");
        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0/DirA/DirC",
                        "Name: \"table_3\""
                        "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                        "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                        "KeyColumnNames: [\"RowId\"]");
        env.TestWaitNotification(runtime, {txId-5, txId-4, txId-3, txId-2, txId -1, txId});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirA"),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::ChildrenCount(2),
                            NLs::PathsInsideDomain(6),
                            NLs::ShardsInsideDomain(5)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirA/table_1"),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::IsTable,
                            NLs::PathIdEqual(NKikimr::TPathId(TTestTxConfig::SchemeShard, 6)),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1})});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirB/table_2"),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::IsTable,
                            NLs::PathIdEqual(NKikimr::TPathId(TTestTxConfig::SchemeShard, 7)),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1})});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirA/DirC"),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::ChildrenCount(1)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirA/DirC/table_3"),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::IsTable,
                            NLs::PathIdEqual(NKikimr::TPathId(TTestTxConfig::SchemeShard, 8)),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1})});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsSubDomain("USER_0"),
                            NLs::PathVersionEqual(8),
                            NLs::ChildrenCount(2)});

        TestUpgradeSubDomain(runtime, ++txId,  "/MyRoot", "USER_0");
        env.TestWaitNotification(runtime, txId);

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::DomainSchemeshard(TTestTxConfig::FakeHiveTablets + 5),
                            NLs::NoChildren,
                            NLs::PathVersionEqual(9)});

        UNIT_ASSERT(tenantSchemeShard == TTestTxConfig::FakeHiveTablets + 5);
        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::PathIdEqual(1),
                            NLs::IsSubDomain("MyRoot/USER_0"),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1})});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirA"),
                           {NLs::PathRedirected,
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::DomainSchemeshard(TTestTxConfig::FakeHiveTablets + 5)});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA"),
                           {NLs::PathExist,
                            NLs::PathIdEqual(TPathId(TTestTxConfig::SchemeShard, 3)),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1})});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA/table_1"),
                           {NLs::PathExist,
                            NLs::IsTable,
                            NLs::PathIdEqual(TPathId(TTestTxConfig::SchemeShard, 6)),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1})});

        TestUpgradeSubDomainDecision(runtime, ++txId,  "/MyRoot", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Commit);
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::DomainSchemeshard(tenantSchemeShard),
                            NLs::PathVersionEqual(10),
                            NLs::NoChildren});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::PathIdEqual(NKikimr::TPathId(tenantSchemeShard, 1)),
                            NLs::IsSubDomain("MyRoot/USER_0"),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::ChildrenCount(2),
                            NLs::PathsInsideDomain(6),
                            NLs::ShardsInsideDomain(6)});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA"),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::PathIdEqual(NKikimr::TPathId(TTestTxConfig::SchemeShard, 3)),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::ChildrenCount(2)});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA/table_1"),
                           {NLs::PathExist,
                            NLs::IsTable,
                            NLs::PathIdEqual(TPathId(TTestTxConfig::SchemeShard, 6)),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1})});
    }

    Y_UNIT_TEST(UpgradeDomainCommitDelete) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, ++txId,  "/MyRoot",
                            "Name: \"USER_0\"");
        TestAlterSubDomain(runtime, ++txId,  "/MyRoot",R"(
                                Name: "USER_0"
                                PlanResolution: 50
                                Coordinators: 1
                                Mediators: 1
                                TimeCastBucketsPerMediator: 2
                                )");
        env.TestWaitNotification(runtime, {txId, txId - 1});
        UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 2));
        UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 2));

        TestMkDir(runtime, ++txId, "/MyRoot/USER_0", "DirA");
        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0/DirA",R"(
                                Name: "Table1"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "C" } }}}
                                )");
        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0/",R"(
                                Name: "Table2"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "C" } }}}
                                )");
        env.TestWaitNotification(runtime, {txId-2, txId -1, txId});

        TestUpgradeSubDomain(runtime, ++txId,  "/MyRoot", "USER_0"); //106
        env.TestWaitNotification(runtime, txId);

        TestUpgradeSubDomainDecision(runtime, ++txId,  "/MyRoot", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Commit); //107
        env.TestWaitNotification(runtime, txId);

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});
        UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 2));
        UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 2));
        TestMkDir(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0", "DirB");
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0/DirB",R"(
                                Name: "Table3"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "C" } }}}
                                )");
        env.TestWaitNotification(runtime, {txId-1, txId}, tenantSchemeShard);

        TestForceDropExtSubDomain(runtime, ++txId,  "/MyRoot", "USER_0"); //110
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist,
                            NLs::NoChildren,
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(0)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathNotExist});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/Table2"),
                           {NLs::PathNotExist});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirA"),
                           {NLs::PathNotExist});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirA/Table1"),
                           {NLs::PathNotExist});

        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+9));
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 2));
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 2));
    }

    Y_UNIT_TEST(UpgradeDomainCommitRecreateShard) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, ++txId,  "/MyRoot",
                            "Name: \"USER_0\"");
        TestAlterSubDomain(runtime, ++txId,  "/MyRoot",R"(
                                Name: "USER_0"
                                PlanResolution: 50
                                Coordinators: 1
                                Mediators: 1
                                TimeCastBucketsPerMediator: 2
                                )");
        env.TestWaitNotification(runtime, {txId, txId - 1});

        TestMkDir(runtime, ++txId, "/MyRoot/USER_0", "DirA");
        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0/DirA", R"(
                                Name: "Table1"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "key2"       Type: "Uint32"}
                                Columns { Name: "Value"      Type: "Utf8"}
                                KeyColumnNames: ["key1", "key2"]
                                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "C" } }}}
                                )");

        env.TestWaitNotification(runtime, {txId -1, txId});

        TestUpgradeSubDomain(runtime, ++txId,  "/MyRoot", "USER_0"); //106
        env.TestWaitNotification(runtime, txId);

        TestUpgradeSubDomainDecision(runtime, ++txId,  "/MyRoot", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Commit); //107
        env.TestWaitNotification(runtime, txId);

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

        TestAlterTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0/DirA",R"(
                Name: "Table1"
                PartitionConfig {
                    CrossDataCenterFollowerCount: 1
                }
            )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA/Table1"),
                           {NLs::PathExist,
                            NLs::IsTable});
    }

    Y_UNIT_TEST(UpgradeDomainWithInactiveShards) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, ++txId,  "/MyRoot",
                            "Name: \"USER_0\"");
        TestAlterSubDomain(runtime, ++txId,  "/MyRoot",
                           "Name: \"USER_0\" "
                           "PlanResolution: 50 "
                           "Coordinators: 1 "
                           "Mediators: 1 "
                           "TimeCastBucketsPerMediator: 2");
        env.TestWaitNotification(runtime, {txId, txId - 1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsSubDomain("USER_0"),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::PathVersionEqual(4),
                            NLs::NoChildren,
                            NLs::ShardsInsideDomain(2)});

        TestMkDir(runtime, ++txId, "/MyRoot/USER_0", "DirA");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0/DirA",
                        "Name: \"table_1\""
                        "Columns { Name: \"RowId\"      Type: \"Utf8\"}"
                        "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                        "KeyColumnNames: [\"RowId\"]");
        env.TestWaitNotification(runtime, txId);

        {
            // Write some data to the user table
            auto fnWriteRow = [&] (ui64 tabletId, TString key) {
                TString writeQuery = Sprintf( R"(
                            (
                                (let key '( '('RowId (Utf8 '%s)) ) )
                                (let value '('('Value (Utf8 '281474980010683)) ) )
                                (return (AsList (UpdateRow '__user__table_1 key value) ))
                            )
                        )", key.c_str());
                NKikimrMiniKQL::TResult result;
                TString err;
                NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
                UNIT_ASSERT_VALUES_EQUAL(err, "");
                UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
            };

            fnWriteRow(TTestTxConfig::FakeHiveTablets + 2, "AAA");
            fnWriteRow(TTestTxConfig::FakeHiveTablets + 2, "ZZZ");
        }

        TestSplitTable(runtime, ++txId, "/MyRoot/USER_0/DirA/table_1", R"(
                            SourceTabletId: 9437196
                            SplitBoundary {
                                KeyPrefix {
                                    Tuple { Optional { Text: "BBB" } }
                                }
                            })");
        env.TestWaitNotification(runtime, txId);

        for (ui32 index = 2; index <= 4; ++index) {
            auto subDomainPathId = TestFindTabletSubDomainPathId(runtime, TTestTxConfig::FakeHiveTablets + 2);
            UNIT_ASSERT_VALUES_EQUAL(subDomainPathId, TPathId(TTestTxConfig::SchemeShard, 2));
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirA"),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::ChildrenCount(1),
                            NLs::PathsInsideDomain(2),
                            NLs::ShardsInsideDomain(5)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/DirA/table_1"),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::IsTable,
                            NLs::PathIdEqual(NKikimr::TPathId(TTestTxConfig::SchemeShard, 4)),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1})});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsSubDomain("USER_0"),
                            NLs::PathVersionEqual(6),
                            NLs::ChildrenCount(1)});

        TestUpgradeSubDomain(runtime, ++txId,  "/MyRoot", "USER_0");
        env.TestWaitNotification(runtime, txId);

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::DomainSchemeshard(TTestTxConfig::FakeHiveTablets + 5),
                            NLs::NoChildren,
                            NLs::PathVersionEqual(7)});

        TestUpgradeSubDomainDecision(runtime, ++txId,  "/MyRoot", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Commit);
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::DomainSchemeshard(tenantSchemeShard),
                            NLs::PathVersionEqual(8),
                            NLs::NoChildren});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::PathIdEqual(NKikimr::TPathId(tenantSchemeShard, 1)),
                            NLs::IsSubDomain("MyRoot/USER_0"),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::ChildrenCount(1),
                            NLs::PathsInsideDomain(2),
                            NLs::ShardsInsideDomain(5)});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA"),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::PathIdEqual(NKikimr::TPathId(TTestTxConfig::SchemeShard, 3)),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::ChildrenCount(1)});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA/table_1"),
                           {NLs::PathExist,
                            NLs::IsTable,
                            NLs::PathIdEqual(TPathId(TTestTxConfig::SchemeShard, 4)),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1})});

        // Inactive shard is not migrated
        TestFindTabletSubDomainPathId(runtime, tenantSchemeShard, TTestTxConfig::FakeHiveTablets + 2,
            NKikimrScheme::TEvFindTabletSubDomainPathIdResult::SHARD_NOT_FOUND);

        for (ui32 index = 3; index <= 4; ++index) {
            auto subDomainPathId = TestFindTabletSubDomainPathId(runtime, tenantSchemeShard, TTestTxConfig::FakeHiveTablets + index);
            UNIT_ASSERT_VALUES_EQUAL(subDomainPathId, TPathId(tenantSchemeShard, 1));
        }

        for (ui32 index = 3; index <= 4; ++index) {
            NKikimrMiniKQL::TResult result;
            TString err;
            ui32 status = LocalMiniKQL(
                runtime, TTestTxConfig::FakeHiveTablets + index, R"(
                    (
                        (let range '('('Id (Uint64 '33) (Uint64 '34))))
                        (let select '('Id 'Uint64))
                        (let options '())
                        (let result (SelectRange 'Sys range select options))
                        (return (AsList (SetResult 'Data result) ))
                    )
                )", result, err);
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            // Cerr << result << Endl;
            // { Type { Kind: Struct Struct { Member { Name: "Data" Type { Kind: Optional Optional { Item { Kind: Struct Struct { Member { Name: "List" Type { Kind: List List { Item { Kind: Struct Struct { Member { Name: "Id" Type { Kind: Optional Optional { Item { Kind: Data Data { Scheme: 4 } } } } } Member { Name: "Uint64" Type { Kind: Optional Optional { Item { Kind: Data Data { Scheme: 4 } } } } } } } } } } Member { Name: "Truncated" Type { Kind: Data Data { Scheme: 6 } } } } } } } } } } Value { Struct { Optional { Struct { List { Struct { Optional { Uint64: 33 } } Struct { Optional { Uint64: 9437199 } } } List { Struct { Optional { Uint64: 34 } } Struct { Optional { Uint64: 1 } } } } Struct { Bool: false } } } } }
            const auto& list = result.GetValue().GetStruct(0).GetOptional().GetStruct(0);

            const auto& item0 = list.GetList(0);
            const ui64 id0 = item0.GetStruct(0).GetOptional().GetUint64();
            UNIT_ASSERT_VALUES_EQUAL(id0, 33);
            const ui64 value0 = item0.GetStruct(1).GetOptional().GetUint64();
            UNIT_ASSERT_VALUES_EQUAL(value0, tenantSchemeShard);

            const auto& item1 = list.GetList(1);
            const ui64 id1 = item1.GetStruct(0).GetOptional().GetUint64();
            UNIT_ASSERT_VALUES_EQUAL(id1, 34);
            const ui64 value1 = item1.GetStruct(1).GetOptional().GetUint64();
            UNIT_ASSERT_VALUES_EQUAL(value1, 1);
        }

        TestDropTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0/DirA", "table_1");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA/table_1"),
                           {NLs::PathNotExist});

        env.TestWaitTabletDeletion(runtime, { TTestTxConfig::FakeHiveTablets + 2, TTestTxConfig::FakeHiveTablets + 3, TTestTxConfig::FakeHiveTablets + 4});

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
                            NLs::DomainKey(2, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({TTestTxConfig::FakeHiveTablets}),
                            NLs::DomainMediators({TTestTxConfig::FakeHiveTablets + 1}),
                            NLs::DomainSchemeshard(TTestTxConfig::FakeHiveTablets + 5),
                            NLs::NoChildren,
                            NLs::PathVersionEqual(8)});

        TestForceDropExtSubDomain(runtime, ++txId,  "/MyRoot", "USER_0");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(UpgradeDomainCommitAlterTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, ++txId,  "/MyRoot",
                            "Name: \"USER_0\"");
        TestAlterSubDomain(runtime, ++txId,  "/MyRoot",R"(
                                Name: "USER_0"
                                PlanResolution: 50
                                Coordinators: 1
                                Mediators: 1
                                TimeCastBucketsPerMediator: 2
                                )");
        env.TestWaitNotification(runtime, {txId, txId - 1});

        TestMkDir(runtime, ++txId, "/MyRoot/USER_0", "DirA");
        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0/DirA", R"(
                                Name: "Table1"
                                Columns { Name: "key1"       Type: "Utf8"}
                                Columns { Name: "Value1"     Type: "Utf8"}
                                KeyColumnNames: ["key1"]
                                PartitionConfig {
                                    ColumnFamilies {
                                        Id: 0
                                        StorageConfig {
                                            SysLog {
                                                PreferredPoolKind: "hdd"
                                            }
                                            Log {
                                                PreferredPoolKind: "hdd"
                                            }
                                            Data {
                                                PreferredPoolKind: "hdd"
                                            }
                                        }
                                    }
                                }
                                )");

        env.TestWaitNotification(runtime, {txId -1, txId});

        TestUpgradeSubDomain(runtime, ++txId,  "/MyRoot", "USER_0"); //106
        env.TestWaitNotification(runtime, txId);

        TestUpgradeSubDomainDecision(runtime, ++txId,  "/MyRoot", "USER_0", NKikimrSchemeOp::TUpgradeSubDomain::Commit); //107
        env.TestWaitNotification(runtime, txId);

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

        TestAlterTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0/DirA",R"(
                Name: "Table1"
                Columns { Name: "Value2"  Type: "Utf8"}
            )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/DirA/Table1"),
                           {NLs::PathExist,
                            NLs::IsTable});
    }
}
