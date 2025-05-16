#include <ydb/core/metering/metering.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

using enum NKikimrSubDomains::EServerlessComputeResourcesMode;

Y_UNIT_TEST_SUITE(TSchemeShardServerLess) {
    Y_UNIT_TEST(Fake) {
    }

    Y_UNIT_TEST(BaseCase) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot",
                               "Name: \"SharedDB\"");
        env.TestWaitNotification(runtime, txId);

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
                << "ResourcesDomainKey { SchemeShard: " << TTestTxConfig::SchemeShard <<  " PathId: " << 2 << " } "
                << "Name: \"ServerLess0\"";
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

        TestDescribeResult(DescribePath(runtime, "/MyRoot/ServerLess0/dir/table0"),
                           {NLs::PathNotExist});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/ServerLess0"),
                           {NLs::PathNotExist});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist,
                            NLs::PathsInsideDomain(1),
                            NLs::ShardsInsideDomain(0)});

        ui64 sharedHiveTablets = TTestTxConfig::FakeHiveTablets + NKikimr::TFakeHiveState::TABLETS_PER_CHILD_HIVE;
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
                << "ResourcesDomainKey { SchemeShard: " << TTestTxConfig::SchemeShard <<  " PathId: " << 2 << " } "
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
            TString meteringData = R"({"usage":{"start":1600452120,"quantity":59,"finish":1600452179,"type":"delta","unit":"byte*second"},"tags":{"ydb_size":11664},"id":"72057594046678944-3-1600452120-1600452179-11664","cloud_id":"CLOUD_ID_VAL","source_wt":1600452180,"source_id":"sless-docapi-ydb-storage","resource_id":"DATABASE_ID_VAL","schema":"ydb.serverless.v1","folder_id":"FOLDER_ID_VAL","version":"1.0.0"})";
            meteringData += "\n";
            UNIT_ASSERT_NO_DIFF(meteringMessages, meteringData);
        }

        runtime.SetObserverFunc(prevObserver);

        TestDropTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/ServerLessDB", "Table");
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        meteringMessages.clear();
        runtime.SetObserverFunc(grabMeteringMessage);
        runtime.AdvanceCurrentTime(TDuration::Minutes(1));
        waitMeteringMessage();

        {
            TString meteringData = R"({"usage":{"start":1600452180,"quantity":59,"finish":1600452239,"type":"delta","unit":"byte*second"},"tags":{"ydb_size":0},"id":"72057594046678944-3-1600452180-1600452239-0","cloud_id":"CLOUD_ID_VAL","source_wt":1600452240,"source_id":"sless-docapi-ydb-storage","resource_id":"DATABASE_ID_VAL","schema":"ydb.serverless.v1","folder_id":"FOLDER_ID_VAL","version":"1.0.0"})";
            meteringData += "\n";
            UNIT_ASSERT_NO_DIFF(meteringMessages, meteringData);
        }
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
                PathId: 2
            }
        )", TTestTxConfig::SchemeShard));
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

        TBlockEvents<NMetering::TEvMetering::TEvWriteMeteringJson> block(runtime);
        runtime.WaitFor("metering", [&]{ return block.size() >= 1; });

        const auto& jsonStr = block[0]->Get()->MeteringJson;
        UNIT_ASSERT_C(jsonStr.Contains(R"("labels":{"k":"v"})"), jsonStr);
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
                    PathId: 2 
                }
                Name: "ServerLess0"
            )",
            TTestTxConfig::SchemeShard
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
                    PathId: 2 
                }
                Name: "ServerLess0"
            )",
            TTestTxConfig::SchemeShard
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
                    PathId: 2 
                }
                Name: "ServerLess0"
            )",
            TTestTxConfig::SchemeShard
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
}
