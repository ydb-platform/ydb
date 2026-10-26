#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

std::unique_ptr<TEvIndexBuilder::TEvCreateRequest> CreateMixedColumnsRequest(ui64 txId, bool invalidDefault = false) {
    Ydb::TypedValue literal;
    literal.mutable_type()->set_type_id(Ydb::Type::UINT32);
    literal.mutable_value()->set_uint32_value(10);
    auto request = CreateBuildColumnRequest(txId, "/MyRoot", "/MyRoot/Table", "default_number", literal);

    auto* settings = request->Record.MutableSettings();
    auto* column = settings->mutable_column_build_operation()->add_column();
    column->SetColumnName("default_text");
    column->mutable_default_from_literal()->mutable_type()->set_type_id(invalidDefault ? Ydb::Type::JSON : Ydb::Type::UTF8);
    column->mutable_default_from_literal()->mutable_value()->set_text_value(invalidDefault ? "{not json]" : "default");

    NKikimrSchemeOp::TModifyScheme payload;
    payload.SetWorkingDir("/MyRoot");
    payload.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterTable);
    auto* table = payload.MutableAlterTable();
    table->SetName("Table");
    table->MutablePartitionConfig(); // Match the payload produced by KQP for ADD COLUMN.
    for (const auto& [name, type] : TVector<std::pair<TString, TString>>{
            {"plain_number", "Uint64"}, {"default_number", "Uint32"},
            {"plain_text", "Utf8"}, {"default_text", invalidDefault ? "Json" : "Utf8"}}) {
        auto* description = table->AddColumns();
        description->SetName(name);
        description->SetType(type);
    }
    settings->MutableAlterMainTablePayload()->PackFrom(payload);
    return request;
}

template <typename T>
void CreateSampleTableWithData(T& t, TTestActorRuntime& runtime, bool& activeZone) {
    auto fnWriteRow = [&] (ui64 tabletId, ui32 key, TString value, const char* table) {
        TString writeQuery = Sprintf(R"(
            (
                (let key   '( '('key   (Uint32 '%u ) ) ) )
                (let row   '( '('value (Utf8 '%s) ) ) )
                (return (AsList (UpdateRow '__user__%s key row) ))
            )
        )", key, value.c_str(), table);
        NKikimrMiniKQL::TResult result;
        TString err;
        NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
        UNIT_ASSERT_VALUES_EQUAL(err, "");
        UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
    };

    {
        TInactiveZone inactive(activeZone);

        TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"     Type: "Uint32" }
              Columns { Name: "value"   Type: "Utf8"   }
              KeyColumnNames: ["key"]
              UniformPartitionsCount: 10
        )");

        t.TestEnv->TestWaitNotification(runtime, t.TxId);

        for (ui32 delta = 0; delta < 101; ++delta) {
            fnWriteRow(TTestTxConfig::FakeHiveTablets, 1 + delta, "value", "Table");
        }
    }
}

} // namespace

Y_UNIT_TEST_SUITE(ColumnBuildRebootsTest) {
    Y_UNIT_TEST_WITH_REBOOTS(MixedColumns) {
        t.GetTestEnvOptions().EnableAddColumsWithDefaults(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            CreateSampleTableWithData(t, runtime, activeZone);
            auto request = CreateMixedColumnsRequest(++t.TxId);
            ForwardToTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(), request.release());
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TInactiveZone inactive(activeZone);
            auto result = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", t.TxId);
            UNIT_ASSERT_VALUES_EQUAL(result.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
                NLs::CheckColumns("Table", {"key", "value", "plain_number", "plain_text", "default_number", "default_text"}, {}, {"key"}, true),
                [](const auto& description) {
                    for (const auto& column : description.GetPathDescription().GetTable().GetColumns()) {
                        UNIT_ASSERT_C(!column.GetIsBuildInProgress(), column.GetName());
                    }
                }
            });
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(MixedColumnsCancelling) {
        t.GetTestEnvOptions().EnableAddColumsWithDefaults(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            CreateSampleTableWithData(t, runtime, activeZone);
            const ui64 buildId = ++t.TxId;
            auto request = CreateMixedColumnsRequest(buildId);
            ForwardToTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(), request.release());
            TestCancelBuildIndex(runtime, ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", buildId);
            t.TestEnv->TestWaitNotification(runtime, buildId);

            TInactiveZone inactive(activeZone);
            auto result = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildId);
            UNIT_ASSERT_VALUES_EQUAL(result.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_CANCELLED);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
                NLs::CheckColumns("Table", {"key", "value"}, {}, {"key"}, true)
            });
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(MixedColumnsRejecting) {
        t.GetTestEnvOptions().EnableAddColumsWithDefaults(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            CreateSampleTableWithData(t, runtime, activeZone);
            auto request = CreateMixedColumnsRequest(++t.TxId, true);
            ForwardToTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(), request.release());
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TInactiveZone inactive(activeZone);
            auto result = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", t.TxId);
            UNIT_ASSERT_VALUES_EQUAL(result.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
                NLs::CheckColumns("Table", {"key", "value"}, {}, {"key"}, true)
            });
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(BaseCase) {
        t.GetTestEnvOptions().EnableAddColumsWithDefaults(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            CreateSampleTableWithData(t, runtime, activeZone);

            runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathExist,
                                    NLs::IndexesCount(0),
                                    NLs::PathVersionEqual(3),
                                    NLs::CheckColumns("Table", {"key", "value"}, {}, {"key"}, true)});
            }

            const TString columnName = "default_value";
            Ydb::TypedValue columnDefaultValue;
            columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT32);
            columnDefaultValue.mutable_value()->set_uint32_value(10);

            AsyncBuildColumn(runtime, ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", columnName, columnDefaultValue);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", t.TxId);
                UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathExist,
                                    NLs::IndexesCount(0),
                                    NLs::PathVersionEqual(6),
                                    NLs::CheckColumns("Table", {"key", "value", columnName}, {}, {"key"}, true)});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(Cancelling) {
        t.GetTestEnvOptions().EnableAddColumsWithDefaults(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            CreateSampleTableWithData(t, runtime, activeZone);

            runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathExist,
                                    NLs::IndexesCount(0),
                                    NLs::PathVersionEqual(3),
                                    NLs::CheckColumns("Table", {"key", "value"}, {}, {"key"}, true)});
            }

            const TString columnName = "default_value";
            Ydb::TypedValue columnDefaultValue;
            columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT32);
            columnDefaultValue.mutable_value()->set_uint32_value(10);

            AsyncBuildColumn(runtime, ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", columnName, columnDefaultValue);

            ui64 buildIndexId = t.TxId;
            TestCancelBuildIndex(runtime, ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
            t.TestEnv->TestWaitNotification(runtime, buildIndexId);

            {
                TInactiveZone inactive(activeZone);
                auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
                UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_CANCELLED);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathExist,
                                    NLs::IndexesCount(0),
                                    NLs::PathVersionEqual(7),
                                    NLs::CheckColumns("Table", {"key", "value", columnName}, {columnName}, {"key"}, true)});
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS(Rejecting) {
        t.GetTestEnvOptions().EnableAddColumsWithDefaults(true);

        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            CreateSampleTableWithData(t, runtime, activeZone);

            runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathExist,
                                    NLs::IndexesCount(0),
                                    NLs::PathVersionEqual(3),
                                    NLs::CheckColumns("Table", {"key", "value"}, {}, {"key"}, true)});
            }

            // Set invalid default value to get rejected
            const TString columnName = "default_value";
            Ydb::TypedValue columnDefaultValue;
            columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::JSON);
            columnDefaultValue.mutable_value()->set_text_value("{not json]");

            AsyncBuildColumn(runtime, ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", columnName, columnDefaultValue);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", t.TxId);
                UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathExist,
                                    NLs::IndexesCount(0),
                                    NLs::PathVersionEqual(7),
                                    NLs::CheckColumns("Table", {"key", "value", columnName}, {columnName}, {"key"}, true)});
            }
        });
    }
}

Y_UNIT_TEST_SUITE(ColumnBuildPayloadTest) {
    Y_UNIT_TEST(MixedColumnsStayHiddenUntilCompletion) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));
        ui64 txId = 100;
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> blocker(runtime);
        auto request = CreateMixedColumnsRequest(++txId);
        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(), request.release());
        runtime.WaitFor("column build scan", [&] { return !blocker.empty(); });

        auto description = DescribePath(runtime, "/MyRoot/Table");
        const auto& columns = description.GetPathDescription().GetTable().GetColumns();
        UNIT_ASSERT_VALUES_EQUAL(columns.size(), 6);
        for (const auto& column : columns) {
            const bool added = column.GetName() != "key" && column.GetName() != "value";
            UNIT_ASSERT_VALUES_EQUAL_C(column.GetIsBuildInProgress(), added, column.GetName());
        }

        blocker.Stop().Unblock();
        env.TestWaitNotification(runtime, txId);
        description = DescribePath(runtime, "/MyRoot/Table");
        for (const auto& column : description.GetPathDescription().GetTable().GetColumns()) {
            UNIT_ASSERT_C(!column.GetIsBuildInProgress(), column.GetName());
        }
    }

    Y_UNIT_TEST(InvalidMixedColumnsPayload) {
        for (ui32 invalidCase = 0; invalidCase < 15; ++invalidCase) {
            TTestBasicRuntime runtime;
            TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));
            ui64 txId = 100;
            TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                Name: "Table"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )");
            env.TestWaitNotification(runtime, txId);

            auto request = CreateMixedColumnsRequest(++txId);
            auto* settings = request->Record.MutableSettings();
            auto* any = settings->MutableAlterMainTablePayload();
            NKikimrSchemeOp::TModifyScheme payload;
            UNIT_ASSERT(any->UnpackTo(&payload));
            switch (invalidCase) {
                case 0:
                    any->set_type_url("type.googleapis.com/Ydb.TypedValue");
                    break;
                case 1:
                    payload.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropTable);
                    break;
                case 2:
                    payload.MutableAlterTable()->SetName("OtherTable");
                    break;
                case 3:
                    payload.MutableAlterTable()->AddDropColumns()->SetName("value");
                    break;
                case 4:
                    *payload.MutableAlterTable()->AddColumns() = payload.GetAlterTable().GetColumns(0);
                    break;
                case 5:
                    payload.MutableAlterTable()->MutableColumns()->RemoveLast();
                    break;
                case 6:
                    payload.MutableAlterTable()->MutableColumns(0)->SetName("value");
                    break;
                case 7:
                    payload.MutableAlterTable()->MutablePartitionConfig()->SetFollowerCount(1);
                    break;
                case 8:
                    *settings->mutable_column_build_operation()->add_column() = settings->column_build_operation().column(0);
                    break;
                case 9:
                    any->set_value("\xff");
                    break;
                case 10:
                    payload.MutableAlterTable()->MutableColumns(3)->SetType("Uint32");
                    break;
                case 11:
                    *payload.MutableAlterTable()->MutableColumns(0)->MutableDefaultFromLiteral()
                        = settings->column_build_operation().column(0).default_from_literal();
                    break;
                case 12:
                    payload.MutableLockGuard()->SetOwnerTxId(1);
                    break;
                case 13:
                    settings->mutable_column_build_operation()->clear_column();
                    break;
                case 14:
                    payload.MutableAlterTable()->MutableColumns(0)->SetSetNotNullInProgress(true);
                    break;
            }
            if (invalidCase != 0 && invalidCase != 9) {
                any->PackFrom(payload);
            }

            const auto sender = runtime.AllocateEdgeActor();
            ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, request.release());
            auto response = runtime.GrabEdgeEventRethrow<TEvIndexBuilder::TEvCreateResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), Ydb::StatusIds::BAD_REQUEST,
                "case " << invalidCase << ": " << response->Get()->Record.DebugString());
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
                NLs::CheckColumns("Table", {"key", "value"}, {}, {"key"}, true)
            });
        }
    }
}
