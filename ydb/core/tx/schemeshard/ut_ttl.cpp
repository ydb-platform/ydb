#include <ydb/core/kqp/ut/common/kqp_ut_common.h> 
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h> 
#include <ydb/core/tx/schemeshard/schemeshard_private.h> 
#include <ydb/core/tx/datashard/datashard.h> 

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

static void CheckTTLSettings(TTestActorRuntime& runtime, const char* tableName = "TTLEnabledTable") {
    TestDescribeResult(
        DescribePath(runtime, Sprintf("/MyRoot/%s", tableName)), {
            NLs::PathExist,
            NLs::Finished, [=] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
                const auto& table = record.GetPathDescription().GetTable();
                UNIT_ASSERT(table.HasTTLSettings());

                const auto& ttl = table.GetTTLSettings();
                UNIT_ASSERT(ttl.HasEnabled());
                UNIT_ASSERT_VALUES_EQUAL(ttl.GetEnabled().GetColumnName(), "modified_at");
                UNIT_ASSERT_VALUES_EQUAL(ttl.GetEnabled().GetExpireAfterSeconds(), 3600);
            }
        }
    );
}

Y_UNIT_TEST_SUITE(TSchemeShardTTLTests) {
    void CreateTableShouldSucceed(const char* name, const char* ttlColumnType, const char* unit = "UNIT_AUTO") {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            Name: "%s"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "modified_at" Type: "%s" }
            KeyColumnNames: ["key"]
            TTLSettings {
              Enabled {
                ColumnName: "modified_at"
                ExpireAfterSeconds: 3600
                ColumnUnit: %s
              }
            }
        )", name, ttlColumnType, unit));
        env.TestWaitNotification(runtime, txId);
        CheckTTLSettings(runtime, name);
    }

    Y_UNIT_TEST(CreateTableShouldSucceed) {
        for (auto ct : {"Date", "Datetime", "Timestamp"}) {
            CreateTableShouldSucceed(Sprintf("TTLTableWith%sColumn", ct).data(), ct);
        }

        for (auto ct : {"Uint32", "Uint64", "DyNumber"}) {
            for (auto unit : {"UNIT_SECONDS", "UNIT_MILLISECONDS", "UNIT_MICROSECONDS", "UNIT_NANOSECONDS"}) {
                CreateTableShouldSucceed(Sprintf("TTLTableWith%sColumn_%s", ct, unit).data(), ct, unit);
            }
        }
    }

    Y_UNIT_TEST(CreateTableShouldFailOnUnknownColumn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TTLEnabledTable"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "modified_at" Type: "Timestamp" }
            KeyColumnNames: ["key"]
            TTLSettings {
              Enabled {
                ColumnName: "created_at"
              }
            }
        )", {NKikimrScheme::StatusSchemeError});
    }

    Y_UNIT_TEST(CreateTableShouldFailOnWrongColumnType) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TTLEnabledTable"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "modified_at" Type: "String" }
            KeyColumnNames: ["key"]
            TTLSettings {
              Enabled {
                ColumnName: "modified_at"
              }
            }
        )", {NKikimrScheme::StatusSchemeError});
    }

    void CreateTableShouldFailOnWrongUnit(const char* ttlColumnType, const char* unit) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            Name: "TTLEnabledTable"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "modified_at" Type: "%s" }
            KeyColumnNames: ["key"]
            TTLSettings {
              Enabled {
                ColumnName: "modified_at"
                ColumnUnit: %s
              }
            }
        )", ttlColumnType, unit), {NKikimrScheme::StatusSchemeError});
    }

    Y_UNIT_TEST(CreateTableShouldFailOnWrongUnit) {
        for (auto ct : {"Date", "Datetime", "Timestamp"}) {
            for (auto unit : {"UNIT_SECONDS", "UNIT_MILLISECONDS", "UNIT_MICROSECONDS", "UNIT_NANOSECONDS"}) {
                CreateTableShouldFailOnWrongUnit(ct, unit);
            }
        }

        for (auto ct : {"Uint32", "Uint64", "DyNumber"}) {
            CreateTableShouldFailOnWrongUnit(ct, "UNIT_AUTO");
        }
    }

    Y_UNIT_TEST(CreateTableShouldFailOnUnspecifiedTTL) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TTLEnabledTable"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "modified_at" Type: "Timestamp" }
            KeyColumnNames: ["key"]
            TTLSettings {
            }
        )", {NKikimrScheme::StatusSchemeError});
    }

    void CreateTableOnIndexedTable(NKikimrSchemeOp::EIndexType indexType, bool enableTtlOnAsyncIndexedTables = false) {
        const auto opts = TTestEnvOptions()
            .EnableAsyncIndexes(true)
            .EnableTtlOnAsyncIndexedTables(enableTtlOnAsyncIndexedTables);

        TTestBasicRuntime runtime;
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        const bool shouldSucceed = (indexType != NKikimrSchemeOp::EIndexTypeGlobalAsync || enableTtlOnAsyncIndexedTables);
        const auto status = shouldSucceed
            ? NKikimrScheme::StatusAccepted
            : NKikimrScheme::StatusPreconditionFailed;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableDescription {
              Name: "TTLEnabledTable"
              Columns { Name: "key" Type: "Uint64" }
              Columns { Name: "modified_at" Type: "Timestamp" }
              KeyColumnNames: ["key"]
              TTLSettings {
                Enabled {
                  ColumnName: "modified_at"
                  ExpireAfterSeconds: 3600
                }
              }
            }
            IndexDescription {
              Name: "UserDefinedIndexByExpireAt"
              KeyColumnNames: ["modified_at"]
              Type: %s
            }
        )", NKikimrSchemeOp::EIndexType_Name(indexType).c_str()), {status});

        if (shouldSucceed) {
            env.TestWaitNotification(runtime, txId);
            CheckTTLSettings(runtime);
        }
    }

    Y_UNIT_TEST(CreateTableShouldSucceedOnIndexedTable) {
        CreateTableOnIndexedTable(NKikimrSchemeOp::EIndexTypeGlobal);
    }

    Y_UNIT_TEST(CreateTableShouldFailOnAsyncIndexedTable) {
        CreateTableOnIndexedTable(NKikimrSchemeOp::EIndexTypeGlobalAsync, false);
    }

    Y_UNIT_TEST(CreateTableShouldSucceedAsyncOnIndexedTable) {
        CreateTableOnIndexedTable(NKikimrSchemeOp::EIndexTypeGlobalAsync, true);
    }

    Y_UNIT_TEST(AlterTableShouldSuccess) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TTLEnabledTable"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "modified_at" Type: "Timestamp" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TTLEnabledTable"
            TTLSettings {
              Enabled {
                ColumnName: "modified_at"
                ExpireAfterSeconds: 3600
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        CheckTTLSettings(runtime);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TTLEnabledTable"
            DropColumns { Name: "modified_at" }
        )", {NKikimrScheme::StatusInvalidParameter});

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TTLEnabledTable"
            TTLSettings {
              Disabled {
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(
            DescribePath(runtime, "/MyRoot/TTLEnabledTable"), {
                NLs::PathExist,
                NLs::Finished, [=] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    const auto& table = record.GetPathDescription().GetTable();
                    UNIT_ASSERT(table.HasTTLSettings());
                    UNIT_ASSERT(table.GetTTLSettings().HasDisabled());
                }
            }
        );
    }

    Y_UNIT_TEST(AlterTableShouldSuccessOnSimultaneousAddColumnAndEnableTTL) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TTLEnabledTable"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TTLEnabledTable"
            Columns { Name: "modified_at" Type: "Timestamp" }
            TTLSettings {
              Enabled {
                ColumnName: "modified_at"
                ExpireAfterSeconds: 3600
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        CheckTTLSettings(runtime);
    }

    Y_UNIT_TEST(AlterTableShouldFailOnSimultaneousDropColumnAndEnableTTL) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TTLEnabledTable"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "modified_at" Type: "Timestamp" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TTLEnabledTable"
            DropColumns { Name: "modified_at" }
            TTLSettings {
              Enabled {
                ColumnName: "modified_at"
              }
            }
        )", {NKikimrScheme::StatusInvalidParameter});
    }

    void AlterTableOnIndexedTable(NKikimrSchemeOp::EIndexType indexType, bool enableTtlOnAsyncIndexedTables = false) {
        const auto opts = TTestEnvOptions()
            .EnableAsyncIndexes(true)
            .EnableTtlOnAsyncIndexedTables(enableTtlOnAsyncIndexedTables);

        TTestBasicRuntime runtime;
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableDescription {
              Name: "TTLEnabledTable"
              Columns { Name: "key" Type: "Uint64" }
              Columns { Name: "modified_at" Type: "Timestamp" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByExpireAt"
              KeyColumnNames: ["modified_at"]
              Type: %s
            }
        )", NKikimrSchemeOp::EIndexType_Name(indexType).c_str()));
        env.TestWaitNotification(runtime, txId);

        const bool shouldSucceed = (indexType != NKikimrSchemeOp::EIndexTypeGlobalAsync || enableTtlOnAsyncIndexedTables);
        const auto status = shouldSucceed
            ? NKikimrScheme::StatusAccepted
            : NKikimrScheme::StatusPreconditionFailed;

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TTLEnabledTable"
            TTLSettings {
              Enabled {
                ColumnName: "modified_at"
                ExpireAfterSeconds: 3600
              }
            }
        )", {status});

        if (shouldSucceed) {
            env.TestWaitNotification(runtime, txId);
            CheckTTLSettings(runtime);
        }
    }

    Y_UNIT_TEST(AlterTableShouldSucceedOnIndexedTable) {
        AlterTableOnIndexedTable(NKikimrSchemeOp::EIndexTypeGlobal);
    }

    Y_UNIT_TEST(AlterTableShouldFailOnAsyncIndexedTable) {
        AlterTableOnIndexedTable(NKikimrSchemeOp::EIndexTypeGlobalAsync, false);
    }

    Y_UNIT_TEST(AlterTableShouldSucceedOnAsyncIndexedTable) {
        AlterTableOnIndexedTable(NKikimrSchemeOp::EIndexTypeGlobalAsync, true);
    }

    void BuildIndex(NKikimrSchemeOp::EIndexType indexType, bool enableTtlOnAsyncIndexedTables = false) {
        const auto opts = TTestEnvOptions()
            .EnableAsyncIndexes(true)
            .EnableTtlOnAsyncIndexedTables(enableTtlOnAsyncIndexedTables);

        TTestBasicRuntime runtime;
        TTestEnv env(runtime, opts);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TTLEnabledTable"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            Columns { Name: "modified_at" Type: "Timestamp" }
            KeyColumnNames: ["key"]
            TTLSettings {
              Enabled {
                ColumnName: "modified_at"
                ExpireAfterSeconds: 3600
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        CheckTTLSettings(runtime);

        const bool shouldSucceed = (indexType != NKikimrSchemeOp::EIndexTypeGlobalAsync || enableTtlOnAsyncIndexedTables);
        const auto status = shouldSucceed
            ? Ydb::StatusIds::SUCCESS
            : Ydb::StatusIds::PRECONDITION_FAILED;

        TestBuilIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/TTLEnabledTable",
            TBuildIndexConfig{"UserDefinedIndexByValue", indexType, {"value"}, {}}, status);

        if (shouldSucceed) {
            env.TestWaitNotification(runtime, txId);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/TTLEnabledTable"), {
                NLs::PathExist,
                NLs::Finished,
                NLs::IndexesCount(1),
            });
        }
    }

    Y_UNIT_TEST(BuildIndexShouldSucceed) {
        BuildIndex(NKikimrSchemeOp::EIndexTypeGlobal);
    }

    Y_UNIT_TEST(BuildAsyncIndexShouldFail) {
        BuildIndex(NKikimrSchemeOp::EIndexTypeGlobalAsync, false);
    }

    Y_UNIT_TEST(BuildAsyncIndexShouldSucceed) {
        BuildIndex(NKikimrSchemeOp::EIndexTypeGlobalAsync, true);
    }

    using TEvCondEraseReq = TEvDataShard::TEvConditionalEraseRowsRequest;
    using TEvCondEraseResp = TEvDataShard::TEvConditionalEraseRowsResponse;

    void WaitForCondErase(TTestActorRuntimeBase& runtime, TEvCondEraseResp::ProtoRecordType::EStatus status = TEvCondEraseResp::ProtoRecordType::OK) {
        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([status](IEventHandle& ev) -> bool {
            if (ev.GetTypeRewrite() != TEvCondEraseResp::EventType) {
                return false;
            }

            auto resp = ev.Get<TEvCondEraseResp>();
            if (resp->Record.GetStatus() == TEvCondEraseResp::ProtoRecordType::ACCEPTED) {
                return false;
            }

            UNIT_ASSERT_VALUES_EQUAL_C(resp->Record.GetStatus(), status, resp->Record.GetErrorDescription());
            return true;
        });

        runtime.DispatchEvents(opts);
    }

    Y_UNIT_TEST(ConditionalErase) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        auto writeRow = [&](ui64 tabletId, ui64 key, TInstant ts, const char* table, const char* ct = "Timestamp") {
            NKikimrMiniKQL::TResult result;
            TString error;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, Sprintf(R"(
                (
                    (let key   '( '('key (Uint64 '%lu ) ) ) )
                    (let row   '( '('ts (%s '%lu) ) ) )
                    (return (AsList (UpdateRow '__user__%s key row) ))
                )
            )", key, ct, ts.GetValue(), table), result, error);

            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, error);
            UNIT_ASSERT_VALUES_EQUAL(error, "");
        };

        auto readTable = [&](ui64 tabletId, const char* table) {
            NKikimrMiniKQL::TResult result;
            TString error;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, Sprintf(R"(
                (
                    (let range '( '('key (Uint64 '0) (Void) )))
                    (let columns '('key) )
                    (let result (SelectRange '__user__%s range columns '()))
                    (return (AsList (SetResult 'Result result) ))
                )
            )", table), result, error);

            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, error);
            UNIT_ASSERT_VALUES_EQUAL(error, "");

            return result;
        };

        auto waitForScheduleCondErase = [&]() {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvPrivate::EvRunConditionalErase));
            runtime.DispatchEvents(options);
        };

        auto setAllowConditionalEraseOperations = [&](bool value) {
            TAtomic unused;
            runtime.GetAppData().Icb->SetValue("SchemeShard_AllowConditionalEraseOperations", value, unused);
        };

        const TInstant now = TInstant::ParseIso8601("2020-09-18T18:00:00.000000Z");
        runtime.UpdateCurrentTime(now);

        ui64 tabletId = TTestTxConfig::FakeHiveTablets;
        ui64 txId = 100;

        {
            TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                Name: "TTLEnabledTable1"
                Columns { Name: "key" Type: "Uint64" }
                Columns { Name: "ts" Type: "Timestamp" }
                KeyColumnNames: ["key"]
                TTLSettings {
                  Enabled {
                    ColumnName: "ts"
                  }
                }
            )");
            env.TestWaitNotification(runtime, txId);

            writeRow(tabletId, 1, now, "TTLEnabledTable1");
            {
                auto result = readTable(tabletId, "TTLEnabledTable1");
                NKqp::CompareYson(R"([[[[[["1"]]];%false]]])", result);
            }

            runtime.AdvanceCurrentTime(TDuration::Minutes(1));
            WaitForCondErase(runtime);
            {
                auto result = readTable(tabletId, "TTLEnabledTable1");
                NKqp::CompareYson(R"([[[[];%false]]])", result);
            }
        }

        {
            ++tabletId;
            TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                Name: "TTLEnabledTable2"
                Columns { Name: "key" Type: "Uint64" }
                Columns { Name: "ts" Type: "Timestamp" }
                KeyColumnNames: ["key"]
                TTLSettings {
                  Enabled {
                    ColumnName: "ts"
                    ExpireAfterSeconds: 3600
                  }
                }
            )");
            env.TestWaitNotification(runtime, txId);

            writeRow(tabletId, 1, now - TDuration::Hours(1), "TTLEnabledTable2");
            writeRow(tabletId, 2, now, "TTLEnabledTable2");
            {
                auto result = readTable(tabletId, "TTLEnabledTable2");
                NKqp::CompareYson(R"([[[[[["1"]];[["2"]]];%false]]])", result);
            }

            runtime.AdvanceCurrentTime(TDuration::Minutes(1));
            WaitForCondErase(runtime);
            {
                auto result = readTable(tabletId, "TTLEnabledTable2");
                NKqp::CompareYson(R"([[[[[["2"]]];%false]]])", result);
            }

            runtime.AdvanceCurrentTime(TDuration::Hours(1));
            WaitForCondErase(runtime);
            {
                auto result = readTable(tabletId, "TTLEnabledTable2");
                NKqp::CompareYson(R"([[[[];%false]]])", result);
            }
        }

        {
            setAllowConditionalEraseOperations(false);

            ++tabletId;
            TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                Name: "TTLEnabledTable3"
                Columns { Name: "key" Type: "Uint64" }
                Columns { Name: "ts" Type: "Timestamp" }
                KeyColumnNames: ["key"]
                TTLSettings {
                  Enabled {
                    ColumnName: "ts"
                  }
                }
            )");
            env.TestWaitNotification(runtime, txId);

            writeRow(tabletId, 1, now, "TTLEnabledTable3");
            {
                auto result = readTable(tabletId, "TTLEnabledTable3");
                NKqp::CompareYson(R"([[[[[["1"]]];%false]]])", result);
            }

            runtime.AdvanceCurrentTime(TDuration::Minutes(1));
            waitForScheduleCondErase();
            {
                auto result = readTable(tabletId, "TTLEnabledTable3");
                NKqp::CompareYson(R"([[[[[["1"]]];%false]]])", result);
            }

            runtime.AdvanceCurrentTime(TDuration::Hours(1));
            waitForScheduleCondErase();
            {
                auto result = readTable(tabletId, "TTLEnabledTable3");
                NKqp::CompareYson(R"([[[[[["1"]]];%false]]])", result);
            }
        }

        {
            setAllowConditionalEraseOperations(true);

            ++tabletId;
            TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                Name: "TTLEnabledTable4"
                Columns { Name: "key" Type: "Uint64" }
                Columns { Name: "ts" Type: "Uint64" }
                KeyColumnNames: ["key"]
                TTLSettings {
                  Enabled {
                    ColumnName: "ts"
                    ColumnUnit: UNIT_MICROSECONDS
                  }
                }
            )");
            env.TestWaitNotification(runtime, txId);

            writeRow(tabletId, 1, now, "TTLEnabledTable4", "Uint64");
            writeRow(tabletId, 2, now + TDuration::Days(1), "TTLEnabledTable4", "Uint64");
            {
                auto result = readTable(tabletId, "TTLEnabledTable4");
                NKqp::CompareYson(R"([[[[[["1"]];[["2"]]];%false]]])", result);
            }

            runtime.AdvanceCurrentTime(TDuration::Minutes(1));
            WaitForCondErase(runtime);
            {
                auto result = readTable(tabletId, "TTLEnabledTable4");
                NKqp::CompareYson(R"([[[[[["2"]]];%false]]])", result);
            }
        }
    }

    Y_UNIT_TEST(RacyAlterTableAndConditionalErase) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto delayConditionalErase = [&]() -> THolder<IEventHandle> {
            THolder<IEventHandle> delayed;

            auto prevObserver = runtime.SetObserverFunc([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
                switch (ev->GetTypeRewrite()) {
                case TEvCondEraseReq::EventType:
                    delayed.Reset(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                default:
                    return TTestActorRuntime::EEventAction::PROCESS;
                }
            });

            if (!delayed) {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&delayed](IEventHandle&) -> bool {
                    return bool(delayed);
                });
                runtime.DispatchEvents(opts);
            }

            runtime.SetObserverFunc(prevObserver);
            return delayed;
        };

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TTLEnabledTable"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "ts" Type: "Timestamp" }
            KeyColumnNames: ["key"]
            TTLSettings {
              Enabled {
                ColumnName: "ts"
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto delayed = delayConditionalErase();

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TTLEnabledTable"
            Columns { Name: "value" Type: "String" }
        )");
        env.TestWaitNotification(runtime, txId);

        runtime.Send(delayed.Release(), 0, true);
        WaitForCondErase(runtime, TEvCondEraseResp::ProtoRecordType::SCHEME_ERROR);
    }

    Y_UNIT_TEST(ShouldCheckQuotas) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", R"(
            Name: "SubDomain"
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            DatabaseQuotas {
              ttl_min_run_internal_seconds: 1800
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // ok without sys settings
        TestCreateTable(runtime, ++txId, "/MyRoot/SubDomain", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "modified_at" Type: "Timestamp" }
            KeyColumnNames: ["key"]
            TTLSettings {
              Enabled {
                ColumnName: "modified_at"
                ExpireAfterSeconds: 3600
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // ok (RunInterval >= limit)
        TestCreateTable(runtime, ++txId, "/MyRoot/SubDomain", R"(
            Name: "Table3"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "modified_at" Type: "Timestamp" }
            KeyColumnNames: ["key"]
            TTLSettings {
              Enabled {
                ColumnName: "modified_at"
                ExpireAfterSeconds: 3600
                SysSettings {
                  RunInterval: 1800000000
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // error (RunInterval < limit)
        TestCreateTable(runtime, ++txId, "/MyRoot/SubDomain", R"(
            Name: "Table4"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "modified_at" Type: "Timestamp" }
            KeyColumnNames: ["key"]
            TTLSettings {
              Enabled {
                ColumnName: "modified_at"
                ExpireAfterSeconds: 3600
                SysSettings {
                  RunInterval: 1799999999
                }
              }
            }
        )", {NKikimrScheme::StatusSchemeError});
    }

    Y_UNIT_TEST(ShouldSkipDroppedColumn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "TTLEnabledTable"
              Columns { Name: "key" Type: "Uint64" }
              Columns { Name: "ts" Type: "Timestamp" }
              Columns { Name: "indexed" Type: "Uint64" }
              Columns { Name: "extra" Type: "Uint64" }
              KeyColumnNames: ["key"]
              TTLSettings {
                Enabled {
                  ColumnName: "ts"
                }
              }
            }
            IndexDescription {
              Name: "Index"
              KeyColumnNames: ["indexed"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TTLEnabledTable"
            DropColumns { Name: "extra" }
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TTLEnabledTable"
            Columns { Name: "extra" Type: "Uint64" }
        )");
        env.TestWaitNotification(runtime, txId);

        WaitForCondErase(runtime);
    }
}

Y_UNIT_TEST_SUITE(TSchemeShardTTLTestsWithReboots) {
    Y_UNIT_TEST(CreateTable) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                Name: "TTLEnabledTable"
                Columns { Name: "key" Type: "Uint64" }
                Columns { Name: "modified_at" Type: "Timestamp" }
                KeyColumnNames: ["key"]
                TTLSettings {
                  Enabled {
                    ColumnName: "modified_at"
                    ExpireAfterSeconds: 3600
                  }
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                CheckTTLSettings(runtime);
            }
        });
    }

    Y_UNIT_TEST(AlterTable) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "TTLEnabledTable"
                    Columns { Name: "key" Type: "Uint64" }
                    Columns { Name: "modified_at" Type: "Timestamp" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestAlterTable(runtime, ++t.TxId, "/MyRoot", R"(
                Name: "TTLEnabledTable"
                TTLSettings {
                  Enabled {
                    ColumnName: "modified_at"
                    ExpireAfterSeconds: 3600
                  }
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                CheckTTLSettings(runtime);
            }
        });
    }
}
