#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardExtSubDomainTest) {
    Y_UNIT_TEST(Fake) {
    }

    Y_UNIT_TEST(Create) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER_0"
                PlanResolution: 50
                Coordinators: 3
                Mediators: 3
                TimeCastBucketsPerMediator: 2
            )",
            {{NKikimrScheme::StatusInvalidParameter, "only declaration at creation is allowed"}}
        );

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER_0"
                StoragePools {
                    Name: "/dc-1/users/tenant-1:hdd"
                    Kind: "hdd"
                }
                StoragePools {
                  Name: "/dc-1/users/tenant-1:hdd-1"
                  Kind: "hdd-1"
                }
            )",
            {{NKikimrScheme::StatusInvalidParameter, "only declaration at creation is allowed"}}
        );

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(Name: "USER_0")"
        );

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, "MyRoot/USER_0"),
                           {NLs::PathExist});
    }

    Y_UNIT_TEST(CreateAndWait) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        AsyncMkDir(runtime, ++txId, "MyRoot", "dir");
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot/dir",
            R"(Name: "USER_0")"
        );

        env.TestWaitNotification(runtime, {txId, txId - 1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/dir/USER_0"),
                           {NLs::PathExist,
                            NLs::PathVersionEqual(3),
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(0)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/dir"),
                           {NLs::PathExist,
                            NLs::PathVersionEqual(5),
                            NLs::PathsInsideDomain(2),
                            NLs::ShardsInsideDomain(0)});
    }

    Y_UNIT_TEST(CreateItemsInsideExtSubdomainAtGSSwithoutTSS) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(Name: "USER_0")"
        );

        TestMkDir(runtime, ++txId, "/MyRoot/USER_0", "dir_0",
                  {{NKikimrScheme::StatusRedirectDomain}});

        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0/dir_0",
            R"(
                Name: "table_1"
                Columns { Name: "RowId"      Type: "Uint64"}
                Columns { Name: "Value"      Type: "Utf8"}
                KeyColumnNames: ["RowId"]
            )",
            {{NKikimrScheme::StatusRedirectDomain}}
        );

        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0",
            R"(
                Name: "table_0"
                Columns { Name: "RowId"      Type: "Uint64"}
                Columns { Name: "Value"      Type: "Utf8"}
                KeyColumnNames: ["RowId"]
            )",
            {{NKikimrScheme::StatusRedirectDomain}}
        );

        env.TestWaitNotification(runtime, {txId, txId - 1, txId - 2, txId -3});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::IsExternalSubDomain("USER_0"),
                            NLs::PathVersionEqual(3),
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(0)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/table_0"),
                           {NLs::InExternalSubdomain});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/dir_0"),
                           {NLs::InExternalSubdomain});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/dir_0/table_1"),
                           {NLs::InExternalSubdomain});
    }

    Y_UNIT_TEST_FLAGS(CreateAndAlterWithoutEnablingTx, AlterDatabaseCreateHiveFirst, ExternalHive) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "dir");
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot/dir",
            R"(Name: "USER_0")"
        );
        TestDescribeResult(DescribePath(runtime, "/MyRoot/dir/USER_0"),
                           {NLs::PathExist,
                            NLs::DomainKey(3, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({}),
                            NLs::DomainMediators({}),
                            NLs::DomainSchemeshard(0)});
        env.TestWaitNotification(runtime, {txId, txId - 1});

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot/dir",
            Sprintf(R"(
                    Name: "USER_0"
                    ExternalSchemeShard: true

                    ExternalHive: %s
                )",
                ToString(ExternalHive).c_str()
            ),
            {{NKikimrScheme::StatusInvalidParameter, "ExtSubDomain without coordinators/mediators"}}
        );
    }

    Y_UNIT_TEST_FLAGS(CreateAndAlter, AlterDatabaseCreateHiveFirst, ExternalHive) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;


        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(Name: "USER_0")"
        );

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            Sprintf(R"(
                    Name: "USER_0"
                    PlanResolution: 50
                    Coordinators: 3
                    Mediators: 3
                    TimeCastBucketsPerMediator: 2

                    ExternalHive: %s
                )",
                ToString(ExternalHive).c_str()
            ),
            {{NKikimrScheme::StatusInvalidParameter, "ExtSubDomain without ExternalSchemeShard"}}
        );

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            Sprintf(R"(
                    StoragePools {
                        Name: "pool-1"
                        Kind: "pool-kind-1"
                    }
                    StoragePools {
                        Name: "pool-2"
                        Kind: "pool-kind-2"
                    }
                    StoragePools {
                        Name: "/dc-1/users/tenant-1:hdd"
                        Kind: "hdd"
                    }
                    StoragePools {
                        Name: "/dc-1/users/tenant-1:hdd-1"
                        Kind: "hdd-1"
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

        env.TestWaitNotification(runtime, {txId, txId - 1, txId - 2});

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

        UNIT_ASSERT(tenantSchemeShard != 0
                    && tenantSchemeShard != (ui64)-1
                    && tenantSchemeShard != TTestTxConfig::SchemeShard);

        ui64 tenantSchemeShard2 = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/table"),
                           {NLs::InExternalSubdomain,
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard2)});

        UNIT_ASSERT(tenantSchemeShard == tenantSchemeShard2);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                           {NLs::PathExist});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/dir"),
                           {NLs::PathNotExist});

        TestMkDir(runtime, tenantSchemeShard, ++txId,  "/MyRoot/USER_0", "dir");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot/USER_0/dir"),
                           {NLs::PathRedirected});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/dir"),
                           {NLs::PathExist,
                            NLs::Finished});

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0/dir",
            R"(
                Name: "table_1"
                Columns { Name: "RowId"      Type: "Uint64"}
                Columns { Name: "Value"      Type: "Utf8"}
                KeyColumnNames: ["RowId"]
            )"
        );

        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/dir/table_1"),
                           {NLs::PathRedirected});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/dir/table_1"),
                           {NLs::PathExist,
                            NLs::Finished});
    }

    Y_UNIT_TEST_FLAGS(CreateAndSameAlterTwice, AlterDatabaseCreateHiveFirst, ExternalHive) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;


        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(Name: "USER_0")"
        );

        const TString alterText = Sprintf(R"(
            Name: "USER_0"
            ExternalSchemeShard: true
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            StoragePools {
                Name: "pool-1"
                Kind: "hdd"
            }

            ExternalHive: %s
            )",
            ToString(ExternalHive).c_str()
        );

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", alterText);
        env.TestWaitNotification(runtime, {txId, txId - 1});

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", alterText);
        env.TestWaitNotification(runtime, txId);

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"), {
            NLs::PathExist,
            NLs::IsExternalSubDomain("USER_0"),
            NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
        });

        UNIT_ASSERT(tenantSchemeShard != 0
            && tenantSchemeShard != (ui64)-1
            && tenantSchemeShard != TTestTxConfig::SchemeShard
        );

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"), {
            NLs::PathExist,
            NLs::IsExternalSubDomain("USER_0"),
            NLs::StoragePoolsEqual({"pool-1"}),
        });

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"), {
            NLs::PathExist,
            NLs::StoragePoolsEqual({"pool-1"})
        });
    }

    Y_UNIT_TEST_FLAGS(CreateAndAlterAlterAddStoragePool, AlterDatabaseCreateHiveFirst, ExternalHive) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;


        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(Name: "USER_0")"
        );

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            Sprintf(R"(
                    Name: "USER_0"
                    ExternalSchemeShard: true
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                    StoragePools {
                        Name: "pool-1"
                        Kind: "hdd"
                    }

                    ExternalHive: %s
                )",
                ToString(ExternalHive).c_str()
            )
        );

        env.TestWaitNotification(runtime, {txId, txId - 1});

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            Sprintf(R"(
                    Name: "USER_0"
                    StoragePools {
                        Name: "pool-1"
                        Kind: "hdd"
                    }
                    StoragePools {
                        Name: "pool-2"
                        Kind: "hdd-1"
                    }

                    ExternalHive: %s
                )",
                ToString(ExternalHive).c_str()
            )
        );
        env.TestWaitNotification(runtime, txId);


        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

        UNIT_ASSERT(tenantSchemeShard != 0
                    && tenantSchemeShard != (ui64)-1
                    && tenantSchemeShard != TTestTxConfig::SchemeShard);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::StoragePoolsEqual({"pool-1", "pool-2"})});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::StoragePoolsEqual({"pool-1", "pool-2"})});

        TestUserAttrs(runtime, ++txId,  "/MyRoot", "USER_0", AlterUserAttrs({{"user__attr_1", "value"}}));
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::UserAttrsEqual({{"user__attr_1", "value"}})});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::UserAttrsEqual({{"user__attr_1", "value"}})});
    }

    Y_UNIT_TEST_FLAGS(CreateAndAlterAlterSameStoragePools, AlterDatabaseCreateHiveFirst, ExternalHive) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;


        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(Name: "USER_0")"
        );

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            Sprintf(R"(
                    Name: "USER_0"
                    ExternalSchemeShard: true
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                    StoragePools {
                        Name: "pool-1"
                        Kind: "hdd"
                    }

                    ExternalHive: %s
                )",
                ToString(ExternalHive).c_str()
            )
        );

        env.TestWaitNotification(runtime, {txId, txId - 1});

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            Sprintf(R"(
                    Name: "USER_0"
                    ExternalSchemeShard: true
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                    StoragePools {
                        Name: "pool-1"
                        Kind: "hdd"
                    }
                    DatabaseQuotas {
                        data_size_hard_quota: 1288490188800
                        data_size_soft_quota: 1224065679360
                    }

                    ExternalHive: %s
                )",
                ToString(ExternalHive).c_str()
            )
        );
        env.TestWaitNotification(runtime, txId);


        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"), {
            NLs::PathExist,
            NLs::IsExternalSubDomain("USER_0"),
            NLs::ExtractTenantSchemeshard(&tenantSchemeShard)
        });

        UNIT_ASSERT(tenantSchemeShard != 0
            && tenantSchemeShard != (ui64)-1
            && tenantSchemeShard != TTestTxConfig::SchemeShard
        );

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"), {
            NLs::PathExist,
            NLs::IsExternalSubDomain("USER_0"),
            NLs::StoragePoolsEqual({"pool-1"})
        });

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"), {
            NLs::PathExist,
            NLs::StoragePoolsEqual({"pool-1"})
        });
    }

    Y_UNIT_TEST_FLAGS(AlterWithPlainAlterSubdomain, AlterDatabaseCreateHiveFirst, ExternalHive) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        // Create extsubdomain

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(Name: "USER_0")"
        );
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            Sprintf(R"(
                    Name: "USER_0"
                    ExternalSchemeShard: true
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                    StoragePools {
                        Name: "pool-1"
                        Kind: "hdd"
                    }

                    ExternalHive: %s
                )",
                ToString(ExternalHive).c_str()
            )
        );
        env.TestWaitNotification(runtime, {txId, txId - 1});

        // Altering extsubdomain but with plain altersubdomain should succeed
        // (post tenant migration compatibility)

        //NOTE: SubDomain and not ExtSubdomain
        TestAlterSubDomain(runtime, ++txId,  "/MyRoot",
            Sprintf(R"(
                    Name: "USER_0"
                    ExternalSchemeShard: true
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                    StoragePools {
                        Name: "pool-1"
                        Kind: "hdd"
                    }

                    ExternalHive: %s
                )",
                ToString(ExternalHive).c_str()
            )
        );
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST_FLAGS(AlterTwiceAndWithPlainAlterSubdomain, AlterDatabaseCreateHiveFirst, ExternalHive) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(Name: "USER_0")"
        );
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            Sprintf(R"(
                    Name: "USER_0"
                    ExternalSchemeShard: true
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                    StoragePools {
                        Name: "pool-1"
                        Kind: "hdd"
                    }

                    ExternalHive: %s
                )",
                ToString(ExternalHive).c_str()
            )
        );
        env.TestWaitNotification(runtime, {txId, txId - 1});

        AsyncAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            Sprintf(R"(
                    Name: "USER_0"
                    ExternalSchemeShard: true
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                    StoragePools {
                        Name: "pool-1"
                        Kind: "hdd"
                    }

                    ExternalHive: %s
                )",
                ToString(ExternalHive).c_str()
            )
        );
        // TestModificationResults(runtime, txId, {NKikimrScheme::StatusAccepted});
        const auto firstAlterTxId = txId;

        //NOTE: SubDomain vs ExtSubDomain
        TestAlterSubDomain(runtime, ++txId,  "/MyRoot",
            Sprintf(R"(
                    Name: "USER_0"
                    ExternalSchemeShard: true
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                    StoragePools {
                        Name: "pool-1"
                        Kind: "hdd"
                    }

                    ExternalHive: %s
                )",
                ToString(ExternalHive).c_str()
            ),
            {{NKikimrScheme::StatusMultipleModifications}}
        );

        env.TestWaitNotification(runtime, firstAlterTxId);
    }

    Y_UNIT_TEST(CreateWithOnlyDotsNotAllowed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", ".", {NKikimrScheme::StatusSchemeError});
        TestMkDir(runtime, ++txId, "/MyRoot", "..", {NKikimrScheme::StatusSchemeError});
        TestMkDir(runtime, ++txId, "/MyRoot", "...", {NKikimrScheme::StatusSchemeError});
        TestMkDir(runtime, ++txId, "/MyRoot", "................", {NKikimrScheme::StatusSchemeError});
        TestMkDir(runtime, ++txId, "/MyRoot", ".SubDirA");
        TestMkDir(runtime, ++txId, "/MyRoot", "SubDirB.");
        TestMkDir(runtime, ++txId, "/MyRoot", "a.............");
        TestMkDir(runtime, ++txId, "/MyRoot", ".......a......");
        TestMkDir(runtime, ++txId, "/MyRoot", ".............a");

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(Name: ".")", {NKikimrScheme::StatusSchemeError});
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(Name: "..")", {NKikimrScheme::StatusSchemeError});
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(Name: "...")", {NKikimrScheme::StatusSchemeError});
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(Name: "................")", {NKikimrScheme::StatusSchemeError});
    }

    Y_UNIT_TEST_FLAG(CreateWithExtraPathSymbolsAllowed, AlterDatabaseCreateHiveFirst) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        TSchemeLimits lowLimits;
        lowLimits.MaxPathElementLength = 10;
        lowLimits.ExtraPathSymbolsAllowed = ".-";
        SetSchemeshardSchemaLimits(runtime, lowLimits);

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(Name: "USER+0")",
            {{NKikimrScheme::StatusSchemeError}}
        );
        env.TestWaitNotification(runtime, txId);

        lowLimits.ExtraPathSymbolsAllowed = ".-+";
        SetSchemeshardSchemaLimits(runtime, lowLimits);

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(Name: "USER+0")"
        );

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER+0"
                ExternalSchemeShard: true
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                StoragePools {
                    Name: "pool-1"
                    Kind: "hdd"
                }
            )"
        );

        env.TestWaitNotification(runtime, {txId, txId - 1});

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER+0"
                ExternalSchemeShard: true
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                StoragePools {
                  Name: "pool-1"
                  Kind: "hdd"
                }
            )"
        );
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER+0"
                ExternalSchemeShard: false
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                StoragePools {
                    Name: "pool-1"
                    Kind: "hdd"
                }
            )",
            {{NKikimrScheme::StatusInvalidParameter, "ExternalSchemeShard could only be added, not removed"}}
        );
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER+0"
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                StoragePools {
                    Name: "pool-1"
                    Kind: "hdd"
                }
            )"
        );
        env.TestWaitNotification(runtime, txId);

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER+0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER+0"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

        TestMkDir(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER+0", "Dir__!",  {{NKikimrScheme::StatusSchemeError}});
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        lowLimits.ExtraPathSymbolsAllowed = ".-+!";
        SetSchemeshardSchemaLimits(runtime, lowLimits, tenantSchemeShard);

        TestMkDir(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER+0", "Dir__!");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER+0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER+0")});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER+0"),
                           {NLs::PathExist,
                            NLs::ChildrenCount(1)});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER+0/Dir__!"),
                           {NLs::PathExist,
                            NLs::Finished});
    }

    Y_UNIT_TEST_FLAG(CreateAndAlterWithExternalHive, AlterDatabaseCreateHiveFirst) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        // create ext-subdomain point
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(Name: "USER_0")"
        );
        env.TestWaitNotification(runtime, txId);

        ui64 rootHiveId = 0;
        {
            // no shards inside root domain
            TestDescribeResult(DescribePath(runtime, "/MyRoot"), {
                NLs::ShardsInsideDomain(0),
                NLs::ExtractDomainHive(&rootHiveId)
            });

            // no tablets in root hive
            UNIT_ASSERT_EQUAL(env.GetHiveState()->Tablets.size(), 0);
        }

        // construct ext-subdomain at its point, with domain hive
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            // (minimally correct ExtSubDomain settings + ExternalHive)
            R"(
                Name: "USER_0"
                ExternalSchemeShard: true
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                StoragePools {
                  Name: "pool-1"
                  Kind: "hdd"
                }

                ExternalHive: true
            )"
        );
        env.TestWaitNotification(runtime, txId);

        struct ExpectedValues {
            ui64 SubdomainVersion;
            ui64 TabletsInsideRootHive;
            ui64 TabletsInsideExtSubdomainHive;
        };

        ExpectedValues expectedFlagSet;
        expectedFlagSet.SubdomainVersion = 3;
        expectedFlagSet.TabletsInsideRootHive = 1;
        expectedFlagSet.TabletsInsideExtSubdomainHive = 3;

        ExpectedValues expectedFlagUnset;
        expectedFlagUnset.SubdomainVersion = 2;
        expectedFlagUnset.TabletsInsideRootHive = 4;
        expectedFlagUnset.TabletsInsideExtSubdomainHive = 0;

        const auto& expected = AlterDatabaseCreateHiveFirst ? expectedFlagSet : expectedFlagUnset;

        ui64 subhiveId = 0;

        // check that all requested domain innards are created:
        // 1 coordinator, 1 mediator, 1 schemeshard and 1 hive
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"), {
            NLs::PathExist,
            NLs::IsExternalSubDomain("USER_0"),
            NLs::ShardsInsideDomain(4),
            NLs::ExtractDomainHive(&subhiveId),
            NLs::DomainSettings(50, 2),
            NLs::SubDomainVersion(expected.SubdomainVersion)
        });

        // ...and there are no new shards inside root domain
        TestDescribeResult(DescribePath(runtime, "/MyRoot"), {
            NLs::ShardsInsideDomain(0)
        });

        // ...but extsubdomain hive is created
        UNIT_ASSERT(subhiveId != 0
            && subhiveId != (ui64)-1
            && subhiveId != TTestTxConfig::Hive
        );
        UNIT_ASSERT(subhiveId != rootHiveId);

        // ...and global hive holds new subdomain hive but nothing else
        UNIT_ASSERT_VALUES_EQUAL_C(expected.TabletsInsideRootHive, env.GetHiveState()->Tablets.size(), "-- unexpected tablet count in global hive");
        UNIT_ASSERT_VALUES_EQUAL(ETabletType::Hive, env.GetHiveState()->Tablets.begin()->second.Type);

        // ...and extsubdomain hive holds all other tablets (1 coordinator, 1 mediator, 1 schemeshard -- 3 total)
        {
            TActorId senderA = runtime.AllocateEdgeActor();
            runtime.SendToPipe(subhiveId, senderA, new TEvHive::TEvRequestHiveInfo());
            TAutoPtr<IEventHandle> handle;
            TEvHive::TEvResponseHiveInfo* response = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);

            UNIT_ASSERT_VALUES_EQUAL_C(expected.TabletsInsideExtSubdomainHive, response->Record.GetTablets().size(), "-- unexpected tablet count in extsubdomain hive");

            if (AlterDatabaseCreateHiveFirst) {
                std::array<ETabletType::EType, 3> expectedTypes = {
                    ETabletType::SchemeShard,
                    ETabletType::Coordinator,
                    ETabletType::Mediator
                };

                for (const auto& tablet : response->Record.GetTablets()) {
                    Cdbg << "extsubdomain hive tablet, type " << tablet.GetTabletType() << Endl;
                    auto found = std::find(expectedTypes.begin(), expectedTypes.end(), tablet.GetTabletType());
                    UNIT_ASSERT_C(found != expectedTypes.end(), "-- extsubdomain hive holds tablet of unexpected type " << tablet.GetTabletType());
                }
            }
        }
    }

    Y_UNIT_TEST_FLAG(AlterCantChangeExternalSchemeShard, AlterDatabaseCreateHiveFirst) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(Name: "USER_0")");
        env.TestWaitNotification(runtime, txId);

        // Minimally correct ExtSubDomain settings
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER_0"
                ExternalSchemeShard: true
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                StoragePools {
                  Name: "pool-1"
                  Kind: "hdd"
                }
            )"
        );
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER_0"
                ExternalSchemeShard: false
            )",
            {{NKikimrScheme::StatusInvalidParameter, "ExternalSchemeShard could only be added, not removed"}}
        );
    }

    Y_UNIT_TEST_FLAG(AlterCantChangeExternalHive, AlterDatabaseCreateHiveFirst) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(Name: "USER_0")");
        env.TestWaitNotification(runtime, txId);

        // Minimally correct ExtSubDomain settings
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER_0"
                ExternalSchemeShard: true
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                StoragePools {
                  Name: "pool-1"
                  Kind: "hdd"
                }

                ExternalHive: true
            )"
        );
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER_0"
                ExternalHive: false
            )",
            {{NKikimrScheme::StatusInvalidParameter, "ExternalHive could only be added, not removed"}}
        );
    }

    Y_UNIT_TEST_FLAG(AlterCantChangeExternalSysViewProcessor, AlterDatabaseCreateHiveFirst) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(Name: "USER_0")");
        env.TestWaitNotification(runtime, txId);

        // Minimally correct ExtSubDomain settings
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER_0"
                ExternalSchemeShard: true
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                StoragePools {
                  Name: "pool-1"
                  Kind: "hdd"
                }

                ExternalSysViewProcessor: true
            )"
        );
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER_0"
                ExternalSysViewProcessor: false
            )",
            {{NKikimrScheme::StatusInvalidParameter, "ExternalSysViewProcessor could only be added, not removed"}}
        );
    }

    Y_UNIT_TEST_FLAG(AlterCantChangeExternalStatisticsAggregator, AlterDatabaseCreateHiveFirst) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(Name: "USER_0")");
        env.TestWaitNotification(runtime, txId);

        // Minimally correct ExtSubDomain settings
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER_0"
                ExternalSchemeShard: true
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                StoragePools {
                  Name: "pool-1"
                  Kind: "hdd"
                }

                ExternalStatisticsAggregator: true
            )"
        );
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER_0"
                ExternalStatisticsAggregator: false
            )",
            {{NKikimrScheme::StatusInvalidParameter, "ExternalStatisticsAggregator could only be added, not removed"}}
        );
    }

    Y_UNIT_TEST_FLAG(AlterCantChangeSetParams, AlterDatabaseCreateHiveFirst) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        // create and setup extsubdomain (by altering it the first time)
        // then try to change parameters with a second alter

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(Name: "USER_0")");
        env.TestWaitNotification(runtime, txId);

        // Minimally correct ExtSubDomain settings
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER_0"
                ExternalSchemeShard: true
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                StoragePools {
                  Name: "pool-1"
                  Kind: "hdd"
                }
            )"
        );
        env.TestWaitNotification(runtime, txId);

        // Alter treats input TSubDomainSettings structure as a diff to existing state
        // (but most parameters couldn't be changed once set to non default values)

        std::vector<std::tuple<TString, TString, TString, TString>> params = {
            // {param-name, default-value, current-value, next-value}
            {"PlanResolution", "0", "50", "15"},
            {"TimeCastBucketsPerMediator", "0", "2", "3"},
            // {"Coordinators", 1, 2},
            // {"Mediators", 1, 2},
        };

        constexpr char TextTemplateSubDomainSettingsSetSingleParameter[] = R"(
            Name: "USER_0"
            %s: %s
        )";

        auto MakeSubdomainSettings = [=](const TString& name, const TString& value) {
            return Sprintf(TextTemplateSubDomainSettingsSetSingleParameter, name.c_str(), value.c_str());
        };

        for (const auto& i : params) {
            const auto& [name, defaultVal, current, next] = i;

            Cdbg << "==== parameter " << name << Endl;

            {
                // accept current value -- noop
                Cdbg << "==== parameter " << name << ": set current value " << Endl;
                TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                    MakeSubdomainSettings(name, current)
                );
                // reject default value -- can't unset
                Cdbg << "==== parameter " << name << ": set default value" << Endl;
                TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                    MakeSubdomainSettings(name, defaultVal),
                    {{NKikimrScheme::StatusInvalidParameter, "could be set only once"}}
                );
                // reject next value -- can't change
                Cdbg << "==== parameter " << name << ": set next value " << Endl;
                TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                    MakeSubdomainSettings(name, next),
                    {{NKikimrScheme::StatusInvalidParameter, "could be set only once"}}
                );
            }
        }
    }

    Y_UNIT_TEST_FLAG(AlterRequiresParamCombinations, AlterDatabaseCreateHiveFirst) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER_0"
            )"
        );
        env.TestWaitNotification(runtime, txId);

        // PlanResolution
        {
            TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                R"(
                    Name: "USER_0"
                    ExternalSchemeShard: true
                    PlanResolution: 0
                    Coordinators: 1
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                )",
                {{NKikimrScheme::StatusInvalidParameter, "can not create ExtSubDomain with PlanResolution not set"}}
            );
        }
        // Coordinators and Mediators
        {
            TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                R"(
                    Name: "USER_0"
                    ExternalSchemeShard: true
                    PlanResolution: 50
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                )",
                {{NKikimrScheme::StatusInvalidParameter, "can not create ExtSubDomain with zero Coordinators"}}
            );

            TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                R"(
                    Name: "USER_0"
                    ExternalSchemeShard: true
                    PlanResolution: 50
                    Coordinators: 0
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                )",
                {{NKikimrScheme::StatusInvalidParameter, "can not create ExtSubDomain with zero Coordinators"}}
            );

            TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                R"(
                    Name: "USER_0"
                    ExternalSchemeShard: true
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 0
                    TimeCastBucketsPerMediator: 2
                )",
                {{NKikimrScheme::StatusInvalidParameter, "can not create ExtSubDomain with zero Mediators"}}
            );

            TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                R"(
                    Name: "USER_0"
                    ExternalSchemeShard: true
                    PlanResolution: 50
                    Coordinators: 1
                    TimeCastBucketsPerMediator: 2
                )",
                {{NKikimrScheme::StatusInvalidParameter, "can not create ExtSubDomain with zero Mediators"}}
            );
        }

        // TimeCastBucketsPerMediator with coordinators
        {
            TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                R"(
                    Name: "USER_0"
                    ExternalSchemeShard: false
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 1
                )",
                {{NKikimrScheme::StatusInvalidParameter, "can not create ExtSubDomain with TimeCastBucketsPerMediator not set"}}
            );

            TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                R"(
                    Name: "USER_0"
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 1
                )",
                {{NKikimrScheme::StatusInvalidParameter, "can not create ExtSubDomain with TimeCastBucketsPerMediator not set"}}
            );
        }
    }

    Y_UNIT_TEST_FLAG(AlterNameConflicts, AlterDatabaseCreateHiveFirst) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        // alter non existing domain

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER_1"
                ExternalSchemeShard: true
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
            )",
            {NKikimrScheme::StatusPathDoesNotExist}
        );
    }

    Y_UNIT_TEST_FLAG(CreateNameConflicts, AlterDatabaseCreateHiveFirst) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        // can't create extsubdomain at existing directory
        {
            TestMkDir(runtime, ++txId, "/MyRoot", "Dir");
            env.TestWaitNotification(runtime, txId);
            TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                R"(
                    Name: "Dir"
                )",
                {NKikimrScheme::StatusNameConflict}
            );
        }

        // can't create extsubdomain at existing table
        {
            TestCreateTable(runtime, ++txId, "/MyRoot",
                R"(
                    Name: "table"
                    Columns { Name: "RowId"      Type: "Uint64"}
                    Columns { Name: "Value"      Type: "Utf8"}
                    KeyColumnNames: ["RowId"]
                )"
            );
            env.TestWaitNotification(runtime, txId);
            TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                R"(
                    Name: "table"
                )",
                {NKikimrScheme::StatusNameConflict}
            );
        }

        // can't create extsubdomain at existing subdomain
        // and can't alter subdomain as extsubdomain
        {
            TestCreateSubDomain(runtime, ++txId,  "/MyRoot",
                R"(
                    Name: "subdomain"
                )"
            );
            env.TestWaitNotification(runtime, txId);
            TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                R"(
                    Name: "subdomain"
                )",
                {NKikimrScheme::StatusNameConflict}
            );

            TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                R"(
                    Name: "subdomain"
                    ExternalSchemeShard: true
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                )",
                {NKikimrScheme::StatusNameConflict}
            );
        }

        // can't create extsubdomain at existing extsubdomain
        {
            TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                R"(
                    Name: "extSubdomain"
                )"
            );
            env.TestWaitNotification(runtime, txId);
            TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                R"(
                    Name: "extSubdomain"
                )",
                {NKikimrScheme::StatusAlreadyExists}
            );
        }
    }

    Y_UNIT_TEST_FLAG(NothingInsideGSS, AlterDatabaseCreateHiveFirst) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(Name: "USER_0")"
        );
        env.TestWaitNotification(runtime, txId);

        auto testCreations = [&] () {
                //dir
                TestMkDir(runtime, ++txId, "/MyRoot/USER_0", "DirA", {{NKikimrScheme::StatusRedirectDomain}});

                //table
                TestCreateTable(runtime, ++txId, "/MyRoot/USER_0", R"(
                Name: "Table"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )", {{NKikimrScheme::StatusRedirectDomain}});

                //bsv
                TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot/USER_0", R"(
                Name: "BSVolume"
                VolumeConfig: {
                    NumChannels: 1
                    ChannelProfileId: 2
                    BlockSize: 4096
                    Partitions { BlockCount: 16 }
                }
            )", {{NKikimrScheme::StatusRedirectDomain}});

                //extSub
                TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot/USER_0", R"(
                Name: "ExtSubDomain"
            )", {{NKikimrScheme::StatusRedirectDomain}});

                //sub
                TestCreateSubDomain(runtime, ++txId,  "/MyRoot/USER_0", R"(
                Name: "SubDomain"
            )", {{NKikimrScheme::StatusRedirectDomain}});

                //kesus
                TestCreateKesus(runtime, ++txId, "/MyRoot/USER_0", R"(
                Name: "Kesus"
            )", {{NKikimrScheme::StatusRedirectDomain}});

                //rtmr
                TestCreateRtmrVolume(runtime, ++txId,  "/MyRoot/USER_0", R"(
                Name: "rtmr1"
                PartitionsCount: 0
            )", {{NKikimrScheme::StatusRedirectDomain}});

                //solomon
                TestCreateSolomon(runtime, ++txId, "/MyRoot/USER_0", R"(
                Name: "Solomon"
                PartitionCount: 40
            )", {{NKikimrScheme::StatusRedirectDomain}});

                //pq
                TestCreatePQGroup(runtime, ++txId, "/MyRoot/USER_0", R"(
                Name: "PQGroup"
                TotalGroupCount: 2
                PartitionPerTablet: 1
                PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10 } }
            )", {{NKikimrScheme::StatusRedirectDomain}});
        };

        testCreations();

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER_0"
                ExternalSchemeShard: true
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                StoragePools {
                    Name: "pool-1"
                    Kind: "hdd"
                }
            )"
        );
        env.TestWaitNotification(runtime, txId);

        testCreations();
    }

    Y_UNIT_TEST_FLAGS(Drop, AlterDatabaseCreateHiveFirst, ExternalHive) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;


        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(Name: "USER_0")"
        );

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            Sprintf(R"(
                    Name: "USER_0"
                    ExternalSchemeShard: true
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                    StoragePools {
                        Name: "pool-1"
                        Kind: "hdd"
                    }

                    ExternalHive: %s
                )",
                ToString(ExternalHive).c_str()
            )
        );

        env.TestWaitNotification(runtime, {txId, txId - 1});
        UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 2));
        UNIT_ASSERT(CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 2));

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

        UNIT_ASSERT(tenantSchemeShard != 0
                    && tenantSchemeShard != (ui64)-1
                    && tenantSchemeShard != TTestTxConfig::SchemeShard);

        TestMkDir(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0", "dir");
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0/dir",
            R"(
                Name: "table_1"
                Columns { Name: "RowId"      Type: "Uint64"}
                Columns { Name: "Value"      Type: "Utf8"}
                KeyColumnNames: ["RowId"]
                UniformPartitionsCount: 2
            )"
        );

        env.TestWaitNotification(runtime, {txId, txId -1}, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, "/MyRoot" ),
                           {NLs::PathExist,
                            NLs::PathsInsideDomain(1),
                            NLs::ShardsInsideDomain(0)});

        const ui64 AdditionalHiveTablet = (ExternalHive ? 1 : 0);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/dir/table_1"),
                           {NLs::PathRedirected,
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(3 + AdditionalHiveTablet)});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/dir/table_1"),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::PathsInsideDomain(2),
                            NLs::ShardsInsideDomain(5 + AdditionalHiveTablet)});

        TestForceDropExtSubDomain(runtime, ++txId, "/MyRoot", "USER_0");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/dir/table_1"),
                           {NLs::PathNotExist});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathNotExist});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist,
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(0)});

        // env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 5));
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 2));
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 2));
    }

    Y_UNIT_TEST_FLAG(CreateThenDropChangesParent, AlterDatabaseCreateHiveFirst) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(Name: "USER_0")"
        );
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"), {
            NLs::PathExist,
            NLs::IsExternalSubDomain("USER_0"),
        });

        const auto& prevParentVersion = DescribePath(runtime, "/MyRoot")
            .GetPathDescription()
            .GetSelf()
            .GetPathVersion()
        ;

        TestForceDropExtSubDomain(runtime, ++txId, "/MyRoot", "USER_0");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"), {NLs::PathNotExist});

        // Check that drop properly propagated to schemeboard
        {
            auto nav = Navigate(runtime, "/MyRoot", NSchemeCache::TSchemeCacheNavigate::EOp::OpList);
            const auto& entry = nav->ResultSet.at(0);
            UNIT_ASSERT_VALUES_EQUAL(entry.Status, NSchemeCache::TSchemeCacheNavigate::EStatus::Ok);

            UNIT_ASSERT(bool(entry.Self));
            UNIT_ASSERT_VALUES_EQUAL(entry.Self->Info.GetPathVersion(), prevParentVersion + 2);

            UNIT_ASSERT(bool(entry.ListNodeEntry));
            UNIT_ASSERT_VALUES_EQUAL_C(entry.ListNodeEntry->Children.size(), 0, "extsubdomain exist: " << entry.ListNodeEntry->Children.at(0).Name);
        }
    }

    Y_UNIT_TEST_FLAGS(CreateAndAlterThenDropChangesParent, AlterDatabaseCreateHiveFirst, ExternalHive) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(Name: "USER_0")"
        );
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            Sprintf(R"(
                    Name: "USER_0"
                    ExternalSchemeShard: true
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                    StoragePools {
                        Name: "pool-1"
                        Kind: "hdd"
                    }

                    ExternalHive: %s
                )",
                ToString(ExternalHive).c_str()
            )
        );
        env.TestWaitNotification(runtime, {txId, txId - 1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"), {
            NLs::PathExist,
            NLs::IsExternalSubDomain("USER_0"),
        });

        const auto& prevParentVersion = DescribePath(runtime, "/MyRoot")
            .GetPathDescription()
            .GetSelf()
            .GetPathVersion()
        ;

        TestForceDropExtSubDomain(runtime, ++txId, "/MyRoot", "USER_0");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"), {NLs::PathNotExist});

        // Check that drop properly propagated to schemeboard
        {
            auto nav = Navigate(runtime, "/MyRoot", NSchemeCache::TSchemeCacheNavigate::EOp::OpList);
            const auto& entry = nav->ResultSet.at(0);
            UNIT_ASSERT_VALUES_EQUAL(entry.Status, NSchemeCache::TSchemeCacheNavigate::EStatus::Ok);

            UNIT_ASSERT(bool(entry.Self));
            UNIT_ASSERT_VALUES_EQUAL(entry.Self->Info.GetPathVersion(), prevParentVersion + 2);

            UNIT_ASSERT(bool(entry.ListNodeEntry));
            UNIT_ASSERT_VALUES_EQUAL_C(entry.ListNodeEntry->Children.size(), 0, "extsubdomain exist: " << entry.ListNodeEntry->Children.at(0).Name);
        }
    }

    Y_UNIT_TEST_FLAG(SysViewProcessorSync, AlterDatabaseCreateHiveFirst) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        NSchemeShard::TSchemeLimits lowLimits;
        lowLimits.MaxShardsInPath = 3;
        SetSchemeshardSchemaLimits(runtime, lowLimits);

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(Name: "USER_0")"
        );

        // check that limits have a power, try create 4 shards
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER_0"
                PlanResolution: 50
                Coordinators: 2
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                ExternalSchemeShard: true
            )",
            {{NKikimrScheme::StatusResourceExhausted}}
        );

        // create 3 shards
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER_0"
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                ExternalSchemeShard: true
                StoragePools {
                    Name: "/dc-1/users/tenant-1:hdd"
                    Kind: "hdd"
                }
            )"
        );
        env.TestWaitNotification(runtime, {txId, txId - 1});

        lowLimits.MaxShardsInPath = 2;
        SetSchemeshardSchemaLimits(runtime, lowLimits);

        // one more, but for free
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER_0"
                ExternalSysViewProcessor: true
            )"
        );

        env.TestWaitNotification(runtime, txId);

        ui64 tenantSchemeShard = 0;
        ui64 tenantSVP = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
                            NLs::ExtractTenantSysViewProcessor(&tenantSVP)});

        UNIT_ASSERT(tenantSchemeShard != 0
                    && tenantSchemeShard != (ui64)-1
                    && tenantSchemeShard != TTestTxConfig::SchemeShard);

        UNIT_ASSERT(tenantSVP != 0 && tenantSVP != (ui64)-1);

        ui64 tenantSVPOnTSS = 0;
        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::ExtractTenantSysViewProcessor(&tenantSVPOnTSS)});

        UNIT_ASSERT_EQUAL(tenantSVP, tenantSVPOnTSS);

        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0",
            R"(
                Name: "table"
                Columns { Name: "RowId"      Type: "Uint64"}
                Columns { Name: "Value"      Type: "Utf8"}
                KeyColumnNames: ["RowId"]
            )"
        );

        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/table"),
                           {NLs::PathExist});

        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::ExtractTenantSysViewProcessor(&tenantSVPOnTSS)});

        UNIT_ASSERT_EQUAL(tenantSVP, tenantSVPOnTSS);
    }

    Y_UNIT_TEST_FLAG(StatisticsAggregatorSync, AlterDatabaseCreateHiveFirst) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        NSchemeShard::TSchemeLimits lowLimits;
        lowLimits.MaxShardsInPath = 3;
        SetSchemeshardSchemaLimits(runtime, lowLimits);

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(Name: "USER_0")"
        );

        // check that limits have a power, try create 4 shards
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER_0"
                PlanResolution: 50
                Coordinators: 2
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                ExternalSchemeShard: true
            )",
            {{NKikimrScheme::StatusResourceExhausted}}
        );

        // create 3 shards
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER_0"
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                ExternalSchemeShard: true
                StoragePools {
                    Name: "/dc-1/users/tenant-1:hdd"
                    Kind: "hdd"
                }
            )"
        );
        env.TestWaitNotification(runtime, {txId, txId - 1});

        lowLimits.MaxShardsInPath = 2;
        SetSchemeshardSchemaLimits(runtime, lowLimits);

        // one more, but for free
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                Name: "USER_0"
                ExternalStatisticsAggregator: true
            )"
        );

        env.TestWaitNotification(runtime, txId);

        ui64 tenantSchemeShard = 0;
        ui64 tenantSA = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
                            NLs::ExtractTenantStatisticsAggregator(&tenantSA)});

        UNIT_ASSERT(tenantSchemeShard != 0
                    && tenantSchemeShard != (ui64)-1
                    && tenantSchemeShard != TTestTxConfig::SchemeShard);

        UNIT_ASSERT(tenantSA != 0 && tenantSA != (ui64)-1);

        ui64 tenantSAOnTSS = 0;
        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::ExtractTenantStatisticsAggregator(&tenantSAOnTSS)});

        UNIT_ASSERT_EQUAL(tenantSA, tenantSAOnTSS);

        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0",
            R"(
                Name: "table"
                Columns { Name: "RowId"      Type: "Uint64"}
                Columns { Name: "Value"      Type: "Utf8"}
                KeyColumnNames: ["RowId"]
            )"
        );

        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/table"),
                           {NLs::PathExist});

        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::ExtractTenantStatisticsAggregator(&tenantSAOnTSS)});

        UNIT_ASSERT_EQUAL(tenantSA, tenantSAOnTSS);
    }

    Y_UNIT_TEST_FLAG(SchemeQuotas, AlterDatabaseCreateHiveFirst) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
                        Name: "USER_0"
                )");
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
                        Name: "USER_0"
                        ExternalSchemeShard: true
                        PlanResolution: 50
                        Coordinators: 1
                        Mediators: 1
                        TimeCastBucketsPerMediator: 2
                        StoragePools {
                            Name: "/dc-1/users/tenant-1:hdd"
                            Kind: "hdd"
                        }
                        DeclaredSchemeQuotas {
                            SchemeQuotas {
                                BucketSize: 1
                                BucketSeconds: 60
                            }
                            SchemeQuotas {
                                BucketSize: 2
                                BucketSeconds: 600
                            }
                        }
                )");
        env.TestWaitNotification(runtime, {txId, txId - 1});

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER_0"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

        // First table should succeed
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table1"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusAccepted});

        // Second table should fail (out of per-minute quota)
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table2"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusQuotaExceeded});

        // After a minute we should be able to create one more table
        runtime.AdvanceCurrentTime(TDuration::Minutes(1));
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table3"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusAccepted});
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table4"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusQuotaExceeded});

        // After 1 more minute we should still fail because of per 10 minute quota
        runtime.AdvanceCurrentTime(TDuration::Minutes(1));
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table5"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusQuotaExceeded});

        // After 3 more minutes we should succeed, because enough per 10 minute quota regenerates
        runtime.AdvanceCurrentTime(TDuration::Minutes(3));
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table6"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusAccepted});

        // Quotas consumption is persistent, on reboot they should stay consumed
        {
            TActorId sender = runtime.AllocateEdgeActor();
            RebootTablet(runtime, tenantSchemeShard, sender);
        }
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table7"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusQuotaExceeded});

        // Need 5 more minutes to create a table
        runtime.AdvanceCurrentTime(TDuration::Minutes(5));
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table7"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusAccepted});

        // Change quotas to 2 per minute, use ext alter for that
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
                        Name: "USER_0"
                        DeclaredSchemeQuotas {
                            SchemeQuotas {
                                BucketSize: 2
                                BucketSeconds: 60
                            }
                        }
                )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table8"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusAccepted});
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table9"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusAccepted});
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table10"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusQuotaExceeded});

        // After 1 minute we should be able to create more tables
        runtime.AdvanceCurrentTime(TDuration::Minutes(1));
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table10"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusAccepted});
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table11"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusAccepted});
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table12"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                )", {NKikimrScheme::StatusQuotaExceeded});
    }
}
