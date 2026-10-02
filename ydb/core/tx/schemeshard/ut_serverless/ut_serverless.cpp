#include <ydb/core/blobstorage/base/blobstorage_database_space_events.h>
#include <ydb/core/metering/metering.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

using enum NKikimrSubDomains::EServerlessComputeResourcesMode;

Y_UNIT_TEST_SUITE(TSchemeShardServerLess) {
    Y_UNIT_TEST(Fake) {
    }

    Y_UNIT_TEST_FLAG(BaseCase, AlterDatabaseCreateHiveFirst) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime,
            TTestEnvOptions()
                .EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst)
        );

        ui64 txId = 100;

        auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
        ui64 expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                               "Name: \"SharedDB\"");
        env.TestWaitNotification(runtime, txId);
        expectedDomainPaths += 1;

        const auto describeResult = DescribePath(runtime, "/MyRoot/SharedDB");
        const auto subDomainPathId = describeResult.GetPathId();

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "StoragePools { "
                              "  Name: \"pool-1\" "
                              "  Kind: \"pool-kind-1\" "
                              "} "
                              "StoragePools { "
                              "  Name: \"pool-2\" "
                              "  Kind: \"pool-kind-2\" "
                              "} "
                              "PlanResolution: 50 "
                              "Coordinators: 1 "
                              "Mediators: 1 "
                              "TimeCastBucketsPerMediator: 2 "
                              "ExternalSchemeShard: true "
                              "ExternalHive: true "
                              "Name: \"SharedDB\"");
        env.TestWaitNotification(runtime, txId);

        ui64 sharedHive = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/SharedDB"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("SharedDB"),
                            NLs::ExtractDomainHive(&sharedHive)});

        UNIT_ASSERT(sharedHive != 0
                    && sharedHive != (ui64)-1
                    && sharedHive != TTestTxConfig::Hive);

        TString createData = TStringBuilder()
                << "ResourcesDomainKey { SchemeShard: " << TTestTxConfig::SchemeShard <<  " PathId: " << subDomainPathId << " } "
                << "Name: \"ServerLess0\"";
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", createData);
        env.TestWaitNotification(runtime, txId);
        expectedDomainPaths += 1;

        TString alterData = TStringBuilder()
                << "PlanResolution: 50 "
                << "Coordinators: 1 "
                << "Mediators: 1 "
                << "TimeCastBucketsPerMediator: 2 "
                << "ExternalSchemeShard: true "
                << "ExternalHive: false "
                << "StoragePools { "
                << "  Name: \"pool-1\" "
                << "  Kind: \"pool-kind-1\" "
                << "} "
                << "Name: \"ServerLess0\"";
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", alterData);
        env.TestWaitNotification(runtime, txId);

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/ServerLess0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("ServerLess0"),
                            NLs::SharedHive(sharedHive),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

        UNIT_ASSERT(tenantSchemeShard != 0
                    && tenantSchemeShard != (ui64)-1
                    && tenantSchemeShard != TTestTxConfig::SchemeShard);

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLess0",
                        "Name: \"dir/table0\""
                        "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                        "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                        "KeyColumnNames: [\"RowId\"]");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLess0/dir/table0"),
                           {NLs::PathExist,
                            NLs::Finished});

        TestForceDropExtSubDomain(runtime, ++txId, "/MyRoot", "ServerLess0");
        env.TestWaitNotification(runtime, txId);
        expectedDomainPaths -= 1;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/ServerLess0/dir/table0"),
                           {NLs::PathNotExist});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/ServerLess0"),
                           {NLs::PathNotExist});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist,
                            NLs::PathsInsideDomain(expectedDomainPaths),
                            NLs::ShardsInsideDomain(0)});

        // Check that shards of ServerLess0 db are gone after its deletion.
        //
        // SharedDB contains:
        //  - 3 of its own shards: SchemeShard, Coordinator, Mediator
        //  - 4 of ServerLess0's shards: SchemeShard, Coordinator, Mediator, 1 x DataShard of the table0
        //

        //NOTE: AlterDatabaseCreateHiveFirst create system tablets in a tenant hive, otherwise they are created in the root hive
        ui64 sharedHiveTablets = TTestTxConfig::FakeHiveTablets + (AlterDatabaseCreateHiveFirst ? TFakeHiveState::TABLETS_PER_CHILD_HIVE : 1)
            + 3  // shards of SharedDB
        ;
        env.TestWaitTabletDeletion(runtime, xrange(sharedHiveTablets, sharedHiveTablets + 4), sharedHive);
    }

    Y_UNIT_TEST(StorageBilling) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        SetAllowServerlessStorageBilling(&runtime, true);

        // Set a large enough idle mem compaction interval, so data size and billing are predictable
        runtime.GetAppData().DataShardConfig.SetIdleMemCompactionIntervalSeconds(600);

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "Name: \"ResourceDB\"");
        env.TestWaitNotification(runtime, txId);

        const auto describeResult = DescribePath(runtime, "/MyRoot/ResourceDB");
        const auto subDomainPathId = describeResult.GetPathId();

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
                              "StoragePools { "
                              "  Name: \"pool-1\" "
                              "  Kind: \"pool-kind-1\" "
                              "} "
                              "StoragePools { "
                              "  Name: \"pool-2\" "
                              "  Kind: \"pool-kind-2\" "
                              "} "
                              "PlanResolution: 50 "
                              "Coordinators: 1 "
                              "Mediators: 1 "
                              "TimeCastBucketsPerMediator: 2 "
                              "ExternalSchemeShard: true "
                              "Name: \"ResourceDB\"");
        env.TestWaitNotification(runtime, txId);

        const TInstant now = TInstant::ParseIso8601("2020-09-18T18:00:00.000000Z");
        runtime.UpdateCurrentTime(now);

        TString createData = TStringBuilder()
                << "ResourcesDomainKey { SchemeShard: " << TTestTxConfig::SchemeShard <<  " PathId: " << subDomainPathId << " } "
                << "Name: \"ServerLessDB\"";
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", createData);
        env.TestWaitNotification(runtime, txId);

        TString alterData = TStringBuilder()
            << "PlanResolution: 50 "
            << "Coordinators: 1 "
            << "Mediators: 1 "
            << "TimeCastBucketsPerMediator: 2 "
            << "ExternalSchemeShard: true "
            << "ExternalHive: false "
            << "StoragePools { "
            << "  Name: \"pool-1\" "
            << "  Kind: \"pool-kind-1\" "
            << "} "
            << "Name: \"ServerLessDB\"";
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", alterData);
        env.TestWaitNotification(runtime, txId);

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/ServerLessDB"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("ServerLessDB"),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

        TestUserAttrs(runtime, ++txId, "/MyRoot", "ServerLessDB", AlterUserAttrs({{"cloud_id", "CLOUD_ID_VAL"}, {"folder_id", "FOLDER_ID_VAL"}, {"database_id", "DATABASE_ID_VAL"}}));
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB"),
                           {NLs::UserAttrsHas({{"cloud_id", "CLOUD_ID_VAL"}, {"folder_id", "FOLDER_ID_VAL"}, {"database_id", "DATABASE_ID_VAL"}})});

        // Just create main table
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "index"   Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto fnWriteRow = [&] (ui64 tabletId, ui32 key, ui32 index, TString value, const char* table) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key   '( '('key   (Uint32 '%u ) ) ) )
                    (let row   '( '('index (Uint32 '%u ) )  '('value (Utf8 '%s) ) ) )
                    (return (AsList (UpdateRow '__user__%s key row) ))
                )
            )", key, index, value.c_str(), table);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
        };
        for (ui32 delta = 0; delta < 101; ++delta) {
            fnWriteRow(TTestTxConfig::FakeHiveTablets + 6, 1 + delta, 1000 + delta, "aaaa", "Table");
        }
        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLessDB/Table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3)});

        TStringBuilder meteringMessages;
        auto grabMeteringMessage = [&meteringMessages](TAutoPtr<IEventHandle>& ev) -> auto {
            if (ev->Type == NMetering::TEvMetering::TEvWriteMeteringJson::EventType) {
                auto *msg = ev->Get<NMetering::TEvMetering::TEvWriteMeteringJson>();
                Cerr << "grabMeteringMessage has happened" << Endl;
                meteringMessages << msg->MeteringJson;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };

        auto waitMeteringMessage = [&]() {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(NMetering::TEvMetering::TEvWriteMeteringJson::EventType));
            runtime.DispatchEvents(options);
        };

        auto prevObserver = runtime.SetObserverFunc(grabMeteringMessage);
        runtime.AdvanceCurrentTime(TDuration::Minutes(1));
        waitMeteringMessage();

        {
            TString meteringData = R"({"usage":{"start":1600452180,"quantity":59,"finish":1600452239,"type":"delta","unit":"byte*second"},"tags":{"ydb_size":13280},"labels":{"Category":"Table"},"id":"72057594046678944-3-1600452180-1600452239-13280","cloud_id":"CLOUD_ID_VAL","source_wt":1600452240,"source_id":"sless-docapi-ydb-storage","resource_id":"DATABASE_ID_VAL","schema":"ydb.serverless.v1","folder_id":"FOLDER_ID_VAL","version":"1.0.0"})";
            MeteringDataEqual(meteringMessages, meteringData);
        }

        runtime.SetObserverFunc(prevObserver);

        TestDropTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", "Table");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        meteringMessages.clear();
        runtime.SetObserverFunc(grabMeteringMessage);
        runtime.AdvanceCurrentTime(TDuration::Minutes(1));
        runtime.SimulateSleep(TDuration::Minutes(1));

        UNIT_ASSERT(meteringMessages.empty());
    }

    Y_UNIT_TEST(StorageBillingLabels) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        SetAllowServerlessStorageBilling(&runtime, true);
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "SharedDB"
        )");
        env.TestWaitNotification(runtime, txId);

        const auto describeResult = DescribePath(runtime, "/MyRoot/SharedDB");
        const auto subDomainPathId = describeResult.GetPathId();

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "SharedDB"
            StoragePools {
              Name: "pool-1"
              Kind: "pool-kind-1"
            }
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            ExternalSchemeShard: true
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", Sprintf(R"(
            Name: "ServerlessDB"
            ResourcesDomainKey {
                SchemeShard: %lu
                PathId: %lu
            }
        )", TTestTxConfig::SchemeShard, subDomainPathId));
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "ServerlessDB"
            StoragePools {
              Name: "pool-1"
              Kind: "pool-kind-1"
            }
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            ExternalSchemeShard: true
            ExternalHive: false
        )");
        env.TestWaitNotification(runtime, txId);

        TestUserAttrs(runtime, ++txId, "/MyRoot", "ServerlessDB", AlterUserAttrs({
            {"cloud_id", "CLOUD_ID_VAL"},
            {"folder_id", "FOLDER_ID_VAL"},
            {"database_id", "DATABASE_ID_VAL"},
            {"label_k", "v"},
            {"not_a_label_x", "y"},
        }));
        env.TestWaitNotification(runtime, txId);

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/ServerlessDB"), {
            NLs::PathExist,
            NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
        });

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerlessDB", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        WriteRow(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerlessDB/Table", 0, 1, "v1");

        TBlockEvents<NMetering::TEvMetering::TEvWriteMeteringJson> block(runtime);
        runtime.WaitFor("metering", [&]{ return block.size() >= 1; });

        const auto& jsonStr = block[0]->Get()->MeteringJson;
        UNIT_ASSERT_C(jsonStr.Contains(R"("labels":{"Category":"Table","k":"v"})"), jsonStr);
        UNIT_ASSERT_C(!jsonStr.Contains("not_a_label"), jsonStr);
    }

    Y_UNIT_TEST(TestServerlessComputeResourcesMode) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableServerlessExclusiveDynamicNodes(true));
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(Name: "SharedDB")"
        );
        env.TestWaitNotification(runtime, txId);

        const auto describeResult = DescribePath(runtime, "/MyRoot/SharedDB");
        const auto subDomainPathId = describeResult.GetPathId();

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                StoragePools {
                    Name: "pool-1"
                    Kind: "pool-kind-1"
                }
                StoragePools {
                    Name: "pool-2"
                    Kind: "pool-kind-2"
                }
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                ExternalSchemeShard: true
                ExternalHive: true
                Name: "SharedDB"
            )"
        );
        env.TestWaitNotification(runtime, txId);

        ui64 sharedHive = 0;
        ui64 sharedDbSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/SharedDB"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("SharedDB"),
                            NLs::ExtractDomainHive(&sharedHive),
                            NLs::ExtractTenantSchemeshard(&sharedDbSchemeShard),
                            NLs::ServerlessComputeResourcesMode(EServerlessComputeResourcesModeUnspecified)});

        UNIT_ASSERT(sharedHive != 0
                    && sharedHive != (ui64)-1
                    && sharedHive != TTestTxConfig::Hive);
        UNIT_ASSERT(sharedDbSchemeShard != 0
                    && sharedDbSchemeShard != (ui64)-1
                    && sharedDbSchemeShard != TTestTxConfig::SchemeShard);

        TestDescribeResult(DescribePath(runtime, sharedDbSchemeShard, "/MyRoot/SharedDB"),
                           {NLs::PathExist,
                            NLs::ServerlessComputeResourcesMode(EServerlessComputeResourcesModeUnspecified)});

        TString createData = Sprintf(
            R"(
                ResourcesDomainKey {
                    SchemeShard: %lu
                    PathId: %lu
                }
                Name: "ServerLess0"
            )",
            TTestTxConfig::SchemeShard, subDomainPathId
        );
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", createData);
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                ExternalSchemeShard: true
                ExternalHive: false
                StoragePools {
                    Name: "pool-1"
                    Kind: "pool-kind-1"
                }
                Name: "ServerLess0"
            )"
        );
        env.TestWaitNotification(runtime, txId);

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/ServerLess0"),
                           {NLs::PathExist,
                            NLs::IsExternalSubDomain("ServerLess0"),
                            NLs::ServerlessComputeResourcesMode(EServerlessComputeResourcesModeShared),
                            NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});

        UNIT_ASSERT(tenantSchemeShard != 0
                    && tenantSchemeShard != (ui64)-1
                    && tenantSchemeShard != TTestTxConfig::SchemeShard);

        TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLess0"),
                           {NLs::PathExist,
                            NLs::ServerlessComputeResourcesMode(EServerlessComputeResourcesModeShared)});

        auto checkServerlessComputeResourcesMode = [&](EServerlessComputeResourcesMode serverlessComputeResourcesMode) {
            TString alterData = Sprintf(
                R"(
                    ServerlessComputeResourcesMode: %d
                    Name: "ServerLess0"
                )",
                serverlessComputeResourcesMode
            );
            TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", alterData);
            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/ServerLess0"),
                               {NLs::ServerlessComputeResourcesMode(serverlessComputeResourcesMode)});
            TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLess0"),
                               {NLs::ServerlessComputeResourcesMode(serverlessComputeResourcesMode)});
            env.TestServerlessComputeResourcesModeInHive(runtime, "/MyRoot/ServerLess0", serverlessComputeResourcesMode, sharedHive);
        };

        checkServerlessComputeResourcesMode(EServerlessComputeResourcesModeExclusive);
        checkServerlessComputeResourcesMode(EServerlessComputeResourcesModeShared);
    }

    Y_UNIT_TEST(TestServerlessComputeResourcesModeValidation) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableServerlessExclusiveDynamicNodes(true));
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(Name: "SharedDB")"
        );
        env.TestWaitNotification(runtime, txId);

        const auto describeResult = DescribePath(runtime, "/MyRoot/SharedDB");
        const auto subDomainPathId = describeResult.GetPathId();

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                StoragePools {
                    Name: "pool-1"
                    Kind: "pool-kind-1"
                }
                StoragePools {
                    Name: "pool-2"
                    Kind: "pool-kind-2"
                }
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                ExternalSchemeShard: true
                ExternalHive: true
                Name: "SharedDB"
            )"
        );
        env.TestWaitNotification(runtime, txId);

        TString createData = Sprintf(
            R"(
                ResourcesDomainKey {
                    SchemeShard: %lu
                    PathId: %lu
                }
                Name: "ServerLess0"
            )",
            TTestTxConfig::SchemeShard, subDomainPathId
        );
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", createData);
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                ExternalSchemeShard: true
                ExternalHive: false
                StoragePools {
                    Name: "pool-1"
                    Kind: "pool-kind-1"
                }
                Name: "ServerLess0"
            )"
        );
        env.TestWaitNotification(runtime, txId);

        // Try to change ServerlessComputeResourcesMode not on serverless database
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                ServerlessComputeResourcesMode: EServerlessComputeResourcesModeShared
                Name: "SharedDB"
            )",
            {{ TEvSchemeShard::EStatus::StatusInvalidParameter, "only for serverless" }}
        );

        // Try to set ServerlessComputeResourcesMode to EServerlessComputeResourcesModeUnspecified
        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                ServerlessComputeResourcesMode: EServerlessComputeResourcesModeUnspecified
                Name: "ServerLess0"
            )",
            {{ TEvSchemeShard::EStatus::StatusInvalidParameter, "EServerlessComputeResourcesModeUnspecified" }}
        );
    }


    Y_UNIT_TEST(TestServerlessComputeResourcesModeFeatureFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableServerlessExclusiveDynamicNodes(false));
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(Name: "SharedDB")"
        );
        env.TestWaitNotification(runtime, txId);

        const auto describeResult = DescribePath(runtime, "/MyRoot/SharedDB");
        const auto subDomainPathId = describeResult.GetPathId();

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                StoragePools {
                    Name: "pool-1"
                    Kind: "pool-kind-1"
                }
                StoragePools {
                    Name: "pool-2"
                    Kind: "pool-kind-2"
                }
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                ExternalSchemeShard: true
                ExternalHive: true
                Name: "SharedDB"
            )"
        );
        env.TestWaitNotification(runtime, txId);

        TString createData = Sprintf(
            R"(
                ResourcesDomainKey {
                    SchemeShard: %lu
                    PathId: %lu
                }
                Name: "ServerLess0"
            )",
            TTestTxConfig::SchemeShard, subDomainPathId
        );
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", createData);
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                ExternalSchemeShard: true
                ExternalHive: false
                StoragePools {
                    Name: "pool-1"
                    Kind: "pool-kind-1"
                }
                Name: "ServerLess0"
            )"
        );
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot",
            R"(
                ServerlessComputeResourcesMode: EServerlessComputeResourcesModeExclusive
                Name: "ServerLess0"
            )",
            {{ TEvSchemeShard::EStatus::StatusPreconditionFailed, "Unsupported: feature flag EnableServerlessExclusiveDynamicNodes is off" }}
        );
    }

    Y_UNIT_TEST(ForbidInMemoryCacheModeInServerLess) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "SharedDB"
        )");
        env.TestWaitNotification(runtime, txId);

        const auto describeResult = DescribePath(runtime, "/MyRoot/SharedDB");
        const auto subDomainPathId = describeResult.GetPathId();

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "SharedDB"
            StoragePools {
              Name: "pool-1"
              Kind: "pool-kind-1"
            }
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            ExternalSchemeShard: true
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", Sprintf(R"(
            Name: "ServerlessDB"
            ResourcesDomainKey {
                SchemeShard: %lu
                PathId: %lu
            }
        )", TTestTxConfig::SchemeShard, subDomainPathId));
        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "ServerlessDB"
            StoragePools {
              Name: "pool-1"
              Kind: "pool-kind-1"
            }
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            ExternalSchemeShard: true
            ExternalHive: false
        )");
        env.TestWaitNotification(runtime, txId);

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/ServerlessDB"), {
            NLs::PathExist,
            NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
        });

        // try create with default in-memory family
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerlessDB", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value1" Type: "String" }
            Columns { Name: "value2" Type: "String" }
            KeyColumnNames: ["key"]
            PartitionConfig {
              ColumnFamilies {
                Id: 0
                ColumnCacheMode: ColumnCacheModeTryKeepInMemory
              }
              ColumnFamilies {
                Name: "Other"
                ColumnCacheMode: ColumnCacheModeRegular
              }
            }
        )", {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});

        // try create with other in-memory family
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerlessDB", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value1" Type: "String" }
            Columns { Name: "value2" Type: "String" }
            KeyColumnNames: ["key"]
            PartitionConfig {
              ColumnFamilies {
                Id: 0
                ColumnCacheMode: ColumnCacheModeRegular
              }
              ColumnFamilies {
                Name: "Other"
                ColumnCacheMode: ColumnCacheModeTryKeepInMemory
              }
            }
        )", {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});

        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerlessDB", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value1" Type: "String" }
            Columns { Name: "value2" Type: "String" }
            KeyColumnNames: ["key"]
            PartitionConfig {
              ColumnFamilies {
                Id: 0
                ColumnCacheMode: ColumnCacheModeRegular
              }
              ColumnFamilies {
                Name: "Other"
                ColumnCacheMode: ColumnCacheModeRegular
              }
            }
        )");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        // try alter default in-memory family
        TestAlterTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerlessDB", R"(
            Name: "Table"
            PartitionConfig {
              ColumnFamilies {
                Id: 0
                ColumnCacheMode: ColumnCacheModeTryKeepInMemory
              }
              ColumnFamilies {
                Id: 1
                Name: "Other"
                ColumnCacheMode: ColumnCacheModeRegular
              }
            }
        )", {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});

        // try alter other in-memory family
        TestAlterTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerlessDB", R"(
            Name: "Table"
            PartitionConfig {
              ColumnFamilies {
                Id: 0
                ColumnCacheMode: ColumnCacheModeRegular
              }
              ColumnFamilies {
                Id: 1
                Name: "Other"
                ColumnCacheMode: ColumnCacheModeTryKeepInMemory
              }
            }
        )", {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});
    }

    // a shared database and a serverless one running on its resources, each with its own schemeshard
    struct TSharedAndServerless {
        TPathId SharedDomainKey;
        ui64 SharedSchemeShard = 0;
        TPathId ServerlessDomainKey;
        ui64 ServerlessSchemeShard = 0;

        TSharedAndServerless(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId) {
            TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(Name: "SharedDB")");
            env.TestWaitNotification(runtime, txId);
            TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
                StoragePools { Name: "pool-1" Kind: "pool-kind-1" }
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                ExternalSchemeShard: true
                Name: "SharedDB"
            )");
            env.TestWaitNotification(runtime, txId);
            SharedDomainKey = TPathId(TTestTxConfig::SchemeShard, DescribePath(runtime, "/MyRoot/SharedDB").GetPathId());
            TestDescribeResult(DescribePath(runtime, "/MyRoot/SharedDB"), {NLs::ExtractTenantSchemeshard(&SharedSchemeShard)});

            TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", TStringBuilder()
                << "ResourcesDomainKey { SchemeShard: " << SharedDomainKey.OwnerId << " PathId: " << SharedDomainKey.LocalPathId << " } "
                << "Name: \"ServerlessDB\"");
            env.TestWaitNotification(runtime, txId);
            TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
                StoragePools { Name: "pool-1" Kind: "pool-kind-1" }
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                ExternalSchemeShard: true
                ExternalHive: false
                Name: "ServerlessDB"
            )");
            env.TestWaitNotification(runtime, txId);
            ServerlessDomainKey = TPathId(TTestTxConfig::SchemeShard, DescribePath(runtime, "/MyRoot/ServerlessDB").GetPathId());
            TestDescribeResult(DescribePath(runtime, "/MyRoot/ServerlessDB"), {NLs::ExtractTenantSchemeshard(&ServerlessSchemeShard)});
        }

        // BS_CONTROLLER reports storage state of the shared database
        void SetExhausted(TTestBasicRuntime& runtime, bool exhausted) const {
            ForwardToTablet(runtime, SharedSchemeShard, runtime.AllocateEdgeActor(),
                new TEvBlobStorage::TEvControllerDatabaseSpaceState(SharedDomainKey, exhausted));
        }

        static void WaitState(TTestBasicRuntime& runtime, TTestEnv& env, ui64 schemeShard, const TString& path,
                bool exhausted) {
            auto getState = [&] {
                return DescribePath(runtime, schemeShard, path).GetPathDescription().GetDomainDescription().GetDomainState();
            };
            for (int i = 0; i < 100 && getState().GetStorageSpaceExhausted() != exhausted; ++i) {
                env.SimulateSleep(runtime, TDuration::MilliSeconds(100));
            }
            const auto state = getState();
            UNIT_ASSERT_VALUES_EQUAL_C(state.GetStorageSpaceExhausted(), exhausted, path);
            UNIT_ASSERT_VALUES_EQUAL_C(state.GetDiskQuotaExceeded(), exhausted, path);
        }
    };

    // subscriptions to BS_CONTROLLER (through the local NodeWarden) are dropped, so that no state from the real
    // BS_CONTROLLER interferes with the injected ones
    auto DropDatabaseSpaceSubscriptions(TTestBasicRuntime& runtime,
            std::vector<std::pair<TActorId, TPathId>> *subscriptions = nullptr) {
        return runtime.AddObserver<TEvBlobStorage::TEvControllerSubscribeDatabaseSpace>([=](auto& ev) {
            if (subscriptions) {
                for (const auto& scope : ev->Get()->Record.GetSubscribe()) {
                    subscriptions->emplace_back(ev->Sender, TPathId::FromProto(scope));
                }
            }
            ev.Reset();
        });
    }

    Y_UNIT_TEST(StorageSpaceStateOfSharedDatabase) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        std::vector<std::pair<TActorId, TPathId>> subscriptions; // subscriber, database key
        auto observer = DropDatabaseSpaceSubscriptions(runtime, &subscriptions);

        const TSharedAndServerless dbs(runtime, env, txId);
        env.SimulateSleep(runtime, TDuration::Seconds(1));

        // only the shared database subscribes to its storage space state at BS_CONTROLLER; the serverless one has no
        // storage of its own and follows the shared database
        std::set<TActorId> sharedSubscribers;
        for (const auto& [subscriber, scope] : subscriptions) {
            UNIT_ASSERT_C(scope != dbs.ServerlessDomainKey, "serverless database subscribed to its own key");
            if (scope == dbs.SharedDomainKey) {
                sharedSubscribers.insert(subscriber);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(sharedSubscribers.size(), 1);

        // BS_CONTROLLER reports the shared database's storage exhausted: both databases get blocked
        dbs.SetExhausted(runtime, true);
        dbs.WaitState(runtime, env, dbs.SharedSchemeShard, "/MyRoot/SharedDB", true);
        dbs.WaitState(runtime, env, dbs.ServerlessSchemeShard, "/MyRoot/ServerlessDB", true);

        // the flag survives restart of the serverless database's schemeshard, and so does following the shared one
        RebootTablet(runtime, dbs.ServerlessSchemeShard, runtime.AllocateEdgeActor());
        dbs.WaitState(runtime, env, dbs.ServerlessSchemeShard, "/MyRoot/ServerlessDB", true);

        // and both get unblocked
        dbs.SetExhausted(runtime, false);
        dbs.WaitState(runtime, env, dbs.SharedSchemeShard, "/MyRoot/SharedDB", false);
        dbs.WaitState(runtime, env, dbs.ServerlessSchemeShard, "/MyRoot/ServerlessDB", false);
    }

    // Shared database's state flips and flips back while transactions of the serverless database's schemeshard are
    // deferred: the flip back must win, although it equals the state applied at the moment it arrives.
    Y_UNIT_TEST_FLAG(StorageSpaceStateFlipsWhileDeferred, InitiallyExhausted) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().DisableStatsBatching(true));
        ui64 txId = 100;

        auto observer = DropDatabaseSpaceSubscriptions(runtime);
        const TSharedAndServerless dbs(runtime, env, txId);
        if (InitiallyExhausted) {
            dbs.SetExhausted(runtime, true);
            dbs.WaitState(runtime, env, dbs.ServerlessSchemeShard, "/MyRoot/ServerlessDB", true);
        }

        // events of the serverless database's schemeshard; its executor shares the mailbox with the tablet actor
        const TActorId schemeShardActor = ResolveTablet(runtime, dbs.ServerlessSchemeShard);
        auto toSchemeShard = [=](const IEventHandle& ev) {
            const TActorId& recipient = ev.GetRecipientRewrite();
            return recipient.NodeId() == schemeShardActor.NodeId() && recipient.Hint() == schemeShardActor.Hint();
        };

        // statistics of a table make the schemeshard enqueue a transaction, which defers execution of the following
        // ones until the executor activates them
        TBlockEvents<TEvDataShard::TEvPeriodicTableStats> blockedStats(runtime, [&](const auto& ev) {
            return toSchemeShard(*ev);
        });
        TestCreateTable(runtime, dbs.ServerlessSchemeShard, ++txId, "/MyRoot/ServerlessDB", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId, dbs.ServerlessSchemeShard);
        runtime.WaitFor("table statistics", [&] { return !blockedStats.empty(); });

        // the shared database's state flips and flips back
        TBlockEvents<TEvTxProxySchemeCache::TEvWatchNotifyUpdated> blockedStates(runtime, [&](const auto& ev) {
            return toSchemeShard(*ev);
        });
        auto lastBlockedState = [&]() -> std::optional<bool> {
            if (blockedStates.empty()) {
                return std::nullopt;
            }
            return blockedStates.back()->Get()->Result->GetPathDescription().GetDomainDescription().GetDomainState()
                .GetStorageSpaceExhausted();
        };
        dbs.SetExhausted(runtime, !InitiallyExhausted);
        runtime.WaitFor("flip", [&] { return lastBlockedState() == !InitiallyExhausted; });
        dbs.SetExhausted(runtime, InitiallyExhausted);
        runtime.WaitFor("flip back", [&] { return lastBlockedState() == InitiallyExhausted; });

        // deliver it all in this order into the same mailbox: the statistics transaction is enqueued first, so the
        // storage state transactions are enqueued after it, and none is executed until its activation
        TBlockEvents<IEventHandle> blockedActivations(runtime, [&](const IEventHandle::TPtr& ev) {
            return ev->GetTypeName().Contains("TEvActivateExecution") && toSchemeShard(*ev);
        });
        blockedStats.Unblock().Stop();
        blockedStates.Unblock().Stop();
        runtime.WaitFor("deferred statistics transaction", [&] { return !blockedActivations.empty(); });
        env.SimulateSleep(runtime, TDuration::MilliSeconds(100)); // let the state notifications be handled meanwhile
        UNIT_ASSERT_C(blockedActivations.size() >= 2, "storage state transaction has not been deferred");
        blockedActivations.Unblock().Stop();

        dbs.WaitState(runtime, env, dbs.ServerlessSchemeShard, "/MyRoot/ServerlessDB", InitiallyExhausted);
        env.SimulateSleep(runtime, TDuration::Seconds(1));
        dbs.WaitState(runtime, env, dbs.ServerlessSchemeShard, "/MyRoot/ServerlessDB", InitiallyExhausted);
    }
}
