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
                            "PlanResolution: 50 "
                            "Coordinators: 3 "
                            "Mediators: 3 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\"",
                            {NKikimrScheme::StatusInvalidParameter});

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                               "StoragePools { "
                               "  Name: \"/dc-1/users/tenant-1:hdd\" "
                               "  Kind: \"hdd\" "
                               "} "
                               "StoragePools { "
                               "  Name: \"/dc-1/users/tenant-1:hdd-1\" "
                               "  Kind: \"hdd-1\" "
                               "} "
                               "Name: \"USER_0\"",
                               {NKikimrScheme::StatusInvalidParameter});

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                            "Name: \"USER_0\"");

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
                               "Name: \"USER_0\"");

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
                            "Name: \"USER_0\"");


        TestMkDir(runtime, ++txId, "/MyRoot/USER_0", "dir_0",
                  {NKikimrScheme::StatusRedirectDomain});

        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0/dir_0",
                        "Name: \"table_1\""
                        "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                        "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                        "KeyColumnNames: [\"RowId\"]",
                        {NKikimrScheme::StatusRedirectDomain});

        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0",
                        "Name: \"table_0\""
                        "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                        "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                        "KeyColumnNames: [\"RowId\"]",
                        {NKikimrScheme::StatusRedirectDomain});

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

    Y_UNIT_TEST(CreateAndAlterWithoutTx) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "dir");
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot/dir",
                               "Name: \"USER_0\"");
        TestDescribeResult(DescribePath(runtime, "/MyRoot/dir/USER_0"),
                           {NLs::PathExist,
                            NLs::DomainKey(3, TTestTxConfig::SchemeShard),
                            NLs::DomainCoordinators({}),
                            NLs::DomainMediators({}),
                            NLs::DomainSchemeshard(0)});
        env.TestWaitNotification(runtime, {txId, txId - 1});

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot/dir",
                              "ExternalSchemeShard: true "
                              "Name: \"USER_0\"",
                               {NKikimrScheme::StatusInvalidParameter});
    }

    Y_UNIT_TEST(CreateAndAlter) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;


        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                               "Name: \"USER_0\"");

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"USER_0\" "
                              "PlanResolution: 50 "
                              "Coordinators: 3 "
                              "Mediators: 3 "
                              "TimeCastBucketsPerMediator: 2",
                              {NKikimrScheme::StatusInvalidParameter});

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "StoragePools { "
                              "  Name: \"pool-1\" "
                              "  Kind: \"pool-kind-1\" "
                              "} "
                              "StoragePools { "
                              "  Name: \"pool-2\" "
                              "  Kind: \"pool-kind-2\" "
                              "} "
                              "StoragePools { "
                              "  Name: \"/dc-1/users/tenant-1:hdd\" "
                              "  Kind: \"hdd\" "
                              "} "
                              "StoragePools { "
                              "  Name: \"/dc-1/users/tenant-1:hdd-1\" "
                              "  Kind: \"hdd-1\" "
                              "} "
                              "PlanResolution: 50 "
                              "Coordinators: 1 "
                              "Mediators: 1 "
                              "TimeCastBucketsPerMediator: 2 "
                              "ExternalSchemeShard: true "
                              "Name: \"USER_0\"");

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
                        "Name: \"table_1\""
                        "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                        "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                        "KeyColumnNames: [\"RowId\"]");

        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/dir/table_1"),
                           {NLs::PathRedirected});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/dir/table_1"),
                           {NLs::PathExist,
                            NLs::Finished});
    }

    Y_UNIT_TEST(CreateAndAlterAfter) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;


        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                               "Name: \"USER_0\"");

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"USER_0\" "
                              "ExternalSchemeShard: true "
                              "PlanResolution: 50 "
                              "Coordinators: 1 "
                              "Mediators: 1 "
                              "TimeCastBucketsPerMediator: 2 "
                              "StoragePools { "
                              "  Name: \"pool-1\" "
                              "  Kind: \"hdd\" "
                              "} ");

        env.TestWaitNotification(runtime, {txId, txId - 1});

        TestAlterSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"USER_0\" "
                              "StoragePools { "
                              "  Name: \"pool-1\" "
                              "  Kind: \"hdd\" "
                              "} "
                              "StoragePools { "
                              "  Name: \"pool-2\" "
                              "  Kind: \"hdd-1\" "
                              "} ");
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

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", "Name: \".\"", {NKikimrScheme::StatusSchemeError});
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", "Name: \"..\"", {NKikimrScheme::StatusSchemeError});
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", "Name: \"...\"", {NKikimrScheme::StatusSchemeError});
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", "Name: \"................\"", {NKikimrScheme::StatusSchemeError});
    }

    Y_UNIT_TEST(CreateWithExtraPathSymbolsAllowed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TSchemeLimits lowLimits;
        lowLimits.MaxPathElementLength = 10;
        lowLimits.ExtraPathSymbolsAllowed = ".-";

        TestUserAttrs(runtime, ++txId, "/", "MyRoot", AlterUserAttrs({{"__extra_path_symbols_allowed", lowLimits.ExtraPathSymbolsAllowed}}));
        env.TestWaitNotification(runtime, txId);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                               "Name: \"USER+0\"",
                               {NKikimrScheme::StatusSchemeError});
        env.TestWaitNotification(runtime, txId);

        lowLimits.ExtraPathSymbolsAllowed = ".-+";
        SetSchemeshardSchemaLimits(runtime, lowLimits);

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                               "Name: \"USER+0\"");

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"USER+0\" "
                              "ExternalSchemeShard: true "
                              "PlanResolution: 50 "
                              "Coordinators: 1 "
                              "Mediators: 1 "
                              "TimeCastBucketsPerMediator: 2 "
                              "StoragePools { "
                              "  Name: \"pool-1\" "
                              "  Kind: \"hdd\" "
                              "} ");

        env.TestWaitNotification(runtime, {txId, txId - 1});

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"USER+0\" "
                              "ExternalSchemeShard: true "
                              "PlanResolution: 50 "
                              "Coordinators: 1 "
                              "Mediators: 1 "
                              "TimeCastBucketsPerMediator: 2 "
                              "StoragePools { "
                              "  Name: \"pool-1\" "
                              "  Kind: \"hdd\" "
                              "} ");
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"USER+0\" "
                              "ExternalSchemeShard: false "
                              "PlanResolution: 50 "
                              "Coordinators: 1 "
                              "Mediators: 1 "
                              "TimeCastBucketsPerMediator: 2 "
                              "StoragePools { "
                              "  Name: \"pool-1\" "
                              "  Kind: \"hdd\" "
                              "} ",
                              {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"USER+0\" "
                              "PlanResolution: 50 "
                              "Coordinators: 1 "
                              "Mediators: 1 "
                              "TimeCastBucketsPerMediator: 2 "
                              "StoragePools { "
                              "  Name: \"pool-1\" "
                              "  Kind: \"hdd\" "
                              "} ");
        env.TestWaitNotification(runtime, txId);

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER+0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("USER+0"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

        TestMkDir(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER+0", "Dir__!",  {NKikimrScheme::StatusSchemeError});
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

    Y_UNIT_TEST(AlterWithWrongParams) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                               "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"USER_0\" "
                              "ExternalSchemeShard: true "
                              "PlanResolution: 0 "
                              "Coordinators: 1 "
                              "Mediators: 1 "
                              "TimeCastBucketsPerMediator: 2",
                              {NKikimrScheme::StatusInvalidParameter});

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"USER_0\" "
                              "ExternalSchemeShard: true "
                              "PlanResolution: 50 "
                              "Mediators: 1 "
                              "TimeCastBucketsPerMediator: 2",
                              {NKikimrScheme::StatusInvalidParameter});

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"USER_0\" "
                              "ExternalSchemeShard: true "
                              "PlanResolution: 50 "
                              "Coordinators: 0 "
                              "Mediators: 1 "
                              "TimeCastBucketsPerMediator: 2",
                              {NKikimrScheme::StatusInvalidParameter});

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"USER_0\" "
                              "ExternalSchemeShard: true "
                              "PlanResolution: 50 "
                              "Coordinators: 1 "
                              "Mediators: 0 "
                              "TimeCastBucketsPerMediator: 2",
                              {NKikimrScheme::StatusInvalidParameter});

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"USER_0\" "
                              "ExternalSchemeShard: true "
                              "PlanResolution: 50 "
                              "Coordinators: 1 "
                              "TimeCastBucketsPerMediator: 2",
                              {NKikimrScheme::StatusInvalidParameter});

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"USER_0\" "
                              "ExternalSchemeShard: false "
                              "PlanResolution: 50 "
                              "Coordinators: 1 "
                              "Mediators: 1 ",
                              {NKikimrScheme::StatusInvalidParameter});

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"USER_0\" "
                              "PlanResolution: 50 "
                              "Coordinators: 1 "
                              "Mediators: 1 ",
                              {NKikimrScheme::StatusInvalidParameter});


        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"USER_1\" "
                              "ExternalSchemeShard: true "
                              "PlanResolution: 50 "
                              "Coordinators: 1 "
                              "Mediators: 1 ",
                              {NKikimrScheme::StatusPathDoesNotExist});

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir");
        env.TestWaitNotification(runtime, txId);
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                               "Name: \"Dir\"",
                               {NKikimrScheme::StatusNameConflict});

        TestCreateTable(runtime, ++txId, "/MyRoot",
                        "Name: \"table\""
                        "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                        "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                        "KeyColumnNames: [\"RowId\"]"
                        );
        env.TestWaitNotification(runtime, txId);
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                               "Name: \"table\"",
                               {NKikimrScheme::StatusNameConflict});

        TestCreateSubDomain(runtime, ++txId,  "/MyRoot",
                                    "Name: \"subdomain\"");
        env.TestWaitNotification(runtime, txId);
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                               "Name: \"subdomain\"",
                               {NKikimrScheme::StatusNameConflict});

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"subdomain\" "
                              "ExternalSchemeShard: true "
                              "PlanResolution: 50 "
                              "Coordinators: 1 "
                              "Mediators: 1 "
                              "TimeCastBucketsPerMediator: 2",
                              {NKikimrScheme::StatusNameConflict});

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                            "Name: \"extSubdomain\"");
        env.TestWaitNotification(runtime, txId);
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                               "Name: \"extSubdomain\"",
                               {NKikimrScheme::StatusAlreadyExists});
    }

    Y_UNIT_TEST(NothingInsideGSS) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                               "Name: \"USER_0\"");
        env.TestWaitNotification(runtime, txId);

        auto testCreations = [&] () {
                //dir
                TestMkDir(runtime, ++txId, "/MyRoot/USER_0", "DirA", {NKikimrScheme::StatusRedirectDomain});

                //table
                TestCreateTable(runtime, ++txId, "/MyRoot/USER_0", R"(
                Name: "Table"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )", {NKikimrScheme::StatusRedirectDomain});

                //bsv
                TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot/USER_0", R"(
                Name: "BSVolume"
                VolumeConfig: {
                    NumChannels: 1
                    ChannelProfileId: 2
                    BlockSize: 4096
                    Partitions { BlockCount: 16 }
                }
            )", {NKikimrScheme::StatusRedirectDomain});

                //extSub
                TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot/USER_0", R"(
                Name: "ExtSubDomain"
            )", {NKikimrScheme::StatusRedirectDomain});

                //sub
                TestCreateSubDomain(runtime, ++txId,  "/MyRoot/USER_0", R"(
                Name: "SubDomain"
            )", {NKikimrScheme::StatusRedirectDomain});

                //kesus
                TestCreateKesus(runtime, ++txId, "/MyRoot/USER_0", R"(
                Name: "Kesus"
            )", {NKikimrScheme::StatusRedirectDomain});

                //rtmr
                TestCreateRtmrVolume(runtime, ++txId,  "/MyRoot/USER_0", R"(
                Name: "rtmr1"
                PartitionsCount: 0
            )", {NKikimrScheme::StatusRedirectDomain});

                //solomon
                TestCreateSolomon(runtime, ++txId, "/MyRoot/USER_0", R"(
                Name: "Solomon"
                PartitionCount: 40
            )", {NKikimrScheme::StatusRedirectDomain});

                //pq
                TestCreatePQGroup(runtime, ++txId, "/MyRoot/USER_0", R"(
                Name: "PQGroup"
                TotalGroupCount: 2
                PartitionPerTablet: 1
                PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10 } }
            )", {NKikimrScheme::StatusRedirectDomain});
        };

        testCreations();

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"USER_0\" "
                              "ExternalSchemeShard: true "
                              "PlanResolution: 50 "
                              "Coordinators: 1 "
                              "Mediators: 1 "
                              "TimeCastBucketsPerMediator: 2 "
                              "StoragePools { "
                              "  Name: \"pool-1\" "
                              "  Kind: \"hdd\" "
                              "} ");
        env.TestWaitNotification(runtime, txId);

        testCreations();
    }

    Y_UNIT_TEST(Drop) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;


        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                               "Name: \"USER_0\"");

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"USER_0\" "
                              "ExternalSchemeShard: true "
                              "PlanResolution: 50 "
                              "Coordinators: 1 "
                              "Mediators: 1 "
                              "TimeCastBucketsPerMediator: 2 "
                              "StoragePools { "
                              "  Name: \"pool-1\" "
                              "  Kind: \"hdd\" "
                              "} ");

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
                        "Name: \"table_1\""
                        "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                        "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                        "KeyColumnNames: [\"RowId\"]"
                        "UniformPartitionsCount: 2");

        env.TestWaitNotification(runtime, {txId, txId -1}, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, "/MyRoot" ),
                           {NLs::PathExist,
                            NLs::PathsInsideDomain(1),
                            NLs::ShardsInsideDomain(0)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/dir/table_1"),
                           {NLs::PathRedirected,
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(3)});

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/dir/table_1"),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::PathsInsideDomain(2),
                            NLs::ShardsInsideDomain(5)});

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

        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 5));
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "SubDomains", "PathId", 2));
        UNIT_ASSERT(!CheckLocalRowExists(runtime, TTestTxConfig::SchemeShard, "Paths", "Id", 2));
    }

    Y_UNIT_TEST(SysViewProcessorSync) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        NSchemeShard::TSchemeLimits lowLimits;
        lowLimits.MaxShardsInPath = 3;
        SetSchemeshardSchemaLimits(runtime, lowLimits);

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                               "Name: \"USER_0\"");

        // check that limits have a power, try create 4 shards
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"USER_0\" "
                              "PlanResolution: 50 "
                              "Coordinators: 2 "
                              "Mediators: 1 "
                              "TimeCastBucketsPerMediator: 2 "
                              "ExternalSchemeShard: true "
                              , {NKikimrScheme::StatusResourceExhausted});

        // create 3 shards
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"USER_0\" "
                              "PlanResolution: 50 "
                              "Coordinators: 1 "
                              "Mediators: 1 "
                              "TimeCastBucketsPerMediator: 2 "
                              "ExternalSchemeShard: true "
                              "StoragePools { "
                              "  Name: \"/dc-1/users/tenant-1:hdd\" "
                              "  Kind: \"hdd\" "
                              "} ");
        env.TestWaitNotification(runtime, {txId, txId - 1});

        lowLimits.MaxShardsInPath = 2;
        SetSchemeshardSchemaLimits(runtime, lowLimits);

        // one more, but for free
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"USER_0\" "
                              "ExternalSysViewProcessor: true ");

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
                        "Name: \"table\""
                        "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                        "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                        "KeyColumnNames: [\"RowId\"]");

        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/table"),
                           {NLs::PathExist});

        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0"),
                           {NLs::PathExist,
                            NLs::ExtractTenantSysViewProcessor(&tenantSVPOnTSS)});

        UNIT_ASSERT_EQUAL(tenantSVP, tenantSVPOnTSS);
    }

    Y_UNIT_TEST(SchemeQuotas) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
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

        // Quotas consuption is persistent, on reboot they should stay consumed
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
