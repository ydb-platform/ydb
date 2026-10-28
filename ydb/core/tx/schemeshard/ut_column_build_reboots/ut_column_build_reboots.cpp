#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/public/lib/value/value.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

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

enum class EMixedBuildInterruption {
    Restart,
    Cancel,
    Failure,
};

void CheckMixedColumnsAfterPartialBackfill(EMixedBuildInterruption interruption, bool rebootDuringRollback) {
    TTestBasicRuntime runtime;
    TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));
    ui64 txId = 100;
    TestCreateTable(runtime, ++txId, "/MyRoot", R"(
        Name: "Table"
        Columns { Name: "key" Type: "Uint32" }
        Columns { Name: "value" Type: "Utf8" }
        Columns { Name: "removed" Type: "Uint32" }
        KeyColumnNames: ["key"]
        UniformPartitionsCount: 2
        PartitionConfig {
            ColumnFamilies { Id: 0 Name: "default" StorageConfig { SysLog {} Log {} } }
            ColumnFamilies { Id: 1 Name: "cold" }
        }
    )");
    env.TestWaitNotification(runtime, txId);
    TVector<ui64> shards;
    const auto description = DescribePath(runtime, "/MyRoot/Table", true);
    for (const auto& partition : description.GetPathDescription().GetTablePartitions()) {
        shards.push_back(partition.GetDatashardId());
    }
    UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2);
    for (size_t shard = 0; shard < shards.size(); ++shard) {
        for (ui32 offset = 0; offset < 4; ++offset) {
            const ui32 key = (shard ? 3000000000u : 1u) + offset;
            NKikimrMiniKQL::TResult result;
            TString error;
            const auto status = LocalMiniKQL(runtime, shards[shard], Sprintf(R"((
                (let key '('('key (Uint32 '%u))))
                (let row '('('value (Utf8 'original)) '('removed (Uint32 '99))))
                (return (AsList (UpdateRow '__user__Table key row)))
            ))", key), result, error);
            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::OK, error);
        }
    }

    Ydb::TypedValue literal;
    literal.mutable_type()->set_type_id(Ydb::Type::UINT32);
    literal.mutable_value()->set_uint32_value(0);
    const ui64 buildId = ++txId;
    auto request = CreateBuildColumnRequest(buildId, "/MyRoot", "/MyRoot/Table", "default_value", literal, "plain_value");
    auto* settings = request->Record.MutableSettings();
    const TVector<TString> defaultNames{"default_value", "default_two", "default_three"};
    const TVector<TString> plainNames{"plain_value", "plain_two", "plain_three"};
    auto* builtColumn = settings->mutable_column_build_operation()->mutable_column(0);
    builtColumn->SetNotNull(true);
    builtColumn->SetFamily("cold");
    NKikimrSchemeOp::TModifyScheme payload;
    UNIT_ASSERT(settings->GetAlterMainTablePayload().UnpackTo(&payload));
    for (size_t i = 1; i < defaultNames.size(); ++i) {
        auto* column = settings->mutable_column_build_operation()->add_column();
        column->CopyFrom(settings->column_build_operation().column(0));
        column->SetColumnName(defaultNames[i]);
        column->mutable_default_from_literal()->mutable_value()->set_uint32_value(10 * i);
        payload.MutableAlterTable()->AddColumns()->SetName(defaultNames[i]);
        auto* plain = payload.MutableAlterTable()->AddColumns();
        plain->SetName(plainNames[i]);
        plain->SetType("Uint32");
    }
    for (auto& column : *payload.MutableAlterTable()->MutableColumns()) {
        if (column.GetName().StartsWith("plain_")) {
            column.SetFamilyName("cold");
        }
    }
    settings->MutableAlterMainTablePayload()->PackFrom(payload);
    const auto retrySettings = *settings;

    size_t scans = 0;
    auto scanObserver = runtime.AddObserver<TEvDataShard::TEvBuildIndexCreateRequest>([&](auto& ev) {
        ++scans;
        const auto& columns = ev->Get()->Record.GetColumnBuildSettings();
        UNIT_ASSERT_VALUES_EQUAL(columns.column_size(), 3);
        for (size_t i = 0; i < defaultNames.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(columns.column(i).GetColumnName(), defaultNames[i]);
            UNIT_ASSERT(columns.column(i).GetNotNull());
            UNIT_ASSERT_VALUES_EQUAL(columns.column(i).GetFamily(), "cold");
        }
    });
    size_t completedBatches = 0;
    bool injectFailure = false;
    bool failureInjected = false;
    auto schemeShardActorId = ResolveTablet(runtime, TTestTxConfig::SchemeShard);
    auto progressObserver = runtime.AddObserver<TEvDataShard::TEvBuildIndexProgressResponse>([&](auto& ev) {
        auto& record = ev->Get()->Record;
        if (ev->GetRecipientRewrite() != schemeShardActorId || record.GetId() != buildId) {
            return;
        }
        if (record.GetStatus() == NKikimrIndexBuilder::IN_PROGRESS && record.GetRowsDelta() > 0) {
            ++completedBatches;
            if (injectFailure && !failureInjected) {
                failureInjected = true;
                record.SetStatus(NKikimrIndexBuilder::BUILD_ERROR);
                record.SetUploadStatus(Ydb::StatusIds::GENERIC_ERROR);
            }
        }
    });
    THashSet<ui32> uploadedColumnIds;
    TBlockEvents<TEvDataShard::TEvUploadRowsRequest> uploads(runtime, [&](const auto& ev) {
        const auto& scheme = ev->Get()->Record.GetRowScheme();
        UNIT_ASSERT_VALUES_EQUAL(scheme.ValueColumnIdsSize(), 3);
        for (const auto id : scheme.GetValueColumnIds()) {
            uploadedColumnIds.insert(id);
        }
        return true;
    });
    ForwardToTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(), request.release());
    runtime.WaitFor("first mixed-column upload", [&] { return !uploads.empty(); });
    uploads.Unblock(1);
    runtime.WaitFor("persisted backfill batch", [&] { return completedBatches > 0 && !uploads.empty(); });
    // Process a request after the progress response before restarting SchemeShard.
    TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildId);
    UNIT_ASSERT(scans > 0);
    UNIT_ASSERT_VALUES_EQUAL(uploadedColumnIds.size(), 3);
    const auto building = DescribePath(runtime, "/MyRoot/Table");
    for (const auto& column : building.GetPathDescription().GetTable().GetColumns()) {
        if (column.GetName().StartsWith("default_")) {
            UNIT_ASSERT(uploadedColumnIds.contains(column.GetId()));
            UNIT_ASSERT(column.GetIsBuildInProgress());
        } else if (column.GetName().StartsWith("plain_")) {
            UNIT_ASSERT(!uploadedColumnIds.contains(column.GetId()));
            UNIT_ASSERT(!column.GetIsBuildInProgress());
        }
    }
    TestDropTable(runtime, ++txId, "/MyRoot", "Table", {NKikimrScheme::StatusMultipleModifications});

    TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> rollback(runtime, [&](const auto& ev) {
        return rebootDuringRollback
            && ev->Get()->Record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpDropColumnBuild;
    });
    if (interruption == EMixedBuildInterruption::Restart) {
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        schemeShardActorId = ResolveTablet(runtime, TTestTxConfig::SchemeShard);
    }
    if (interruption == EMixedBuildInterruption::Cancel) {
        TestCancelBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", buildId);
    } else if (interruption == EMixedBuildInterruption::Failure) {
        injectFailure = true;
        uploads.Unblock(1);
        runtime.WaitFor("failure after a completed batch", [&] { return failureInjected; });
        UNIT_ASSERT(completedBatches >= 2);
    }
    uploads.Stop().Unblock();
    if (rebootDuringRollback) {
        runtime.WaitFor("blocked rollback", [&] { return !rollback.empty(); });
        rollback.clear();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        schemeShardActorId = ResolveTablet(runtime, TTestTxConfig::SchemeShard);
        runtime.WaitFor("rollback proposed after restart", [&] { return !rollback.empty(); });
        rollback.Stop().Unblock();
    }
    env.TestWaitNotification(runtime, buildId);
    const auto build = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildId);
    const auto expectedState = interruption == EMixedBuildInterruption::Restart
        ? Ydb::Table::IndexBuildState::STATE_DONE
        : interruption == EMixedBuildInterruption::Cancel
            ? Ydb::Table::IndexBuildState::STATE_CANCELLED
            : Ydb::Table::IndexBuildState::STATE_REJECTED;
    UNIT_ASSERT_VALUES_EQUAL_C(build.GetIndexBuild().GetState(), expectedState, build.DebugString());

    const auto checkSchema = [&](bool hasAddedColumns) {
        TSet<TString> dropped;
        if (!hasAddedColumns) {
            dropped.insert(defaultNames.begin(), defaultNames.end());
            dropped.insert(plainNames.begin(), plainNames.end());
        }
        const auto table = DescribePath(runtime, "/MyRoot/Table");
        TestDescribeResult(table, {
            NLs::CheckColumns("Table", {"key", "value", "removed", "default_value", "default_two", "default_three",
                "plain_value", "plain_two", "plain_three"}, dropped, {"key"}, true),
        });
        for (const auto& column : table.GetPathDescription().GetTable().GetColumns()) {
            UNIT_ASSERT(!column.GetIsBuildInProgress());
            if (column.GetName().StartsWith("plain_") || column.GetName().StartsWith("default_")) {
                UNIT_ASSERT_VALUES_EQUAL(column.GetFamily(), 1);
                UNIT_ASSERT_VALUES_EQUAL(column.GetNotNull(), column.GetName().StartsWith("default_"));
            }
        }
    };
    const auto checkRows = [&](bool hasAddedColumns) {
        TVector<TString> names{"key", "value", "removed"};
        if (hasAddedColumns) {
            names.insert(names.end(), defaultNames.begin(), defaultNames.end());
            names.insert(names.end(), plainNames.begin(), plainNames.end());
        }
        for (const auto shard : shards) {
            const auto result = ReadTable(runtime, shard, "Table", {"key"}, names);
            const auto rows = NClient::TValue::Create(result)["Result"]["List"];
            UNIT_ASSERT_VALUES_EQUAL(rows.Size(), 4);
            for (size_t i = 0; i < rows.Size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(TString(rows[i]["value"]), "original");
                UNIT_ASSERT_VALUES_EQUAL(ui32(rows[i]["removed"]), 99);
                if (hasAddedColumns) {
                    for (const auto& name : plainNames) {
                        UNIT_ASSERT(rows[i][name].IsNull());
                    }
                    for (size_t j = 0; j < defaultNames.size(); ++j) {
                        UNIT_ASSERT(rows[i][defaultNames[j]].HaveValue());
                        UNIT_ASSERT_VALUES_EQUAL(ui32(rows[i][defaultNames[j]]), 10 * j);
                    }
                }
            }
        }
    };
    const bool hasDefault = interruption == EMixedBuildInterruption::Restart;
    checkSchema(hasDefault);
    checkRows(hasDefault);
    if (!hasDefault) {
        // Retry the entire request: none of its added columns may survive rollback.
        const ui64 retryId = ++txId;
        auto retry = CreateBuildColumnRequest(retryId, "/MyRoot", "/MyRoot/Table", "default_value", literal);
        retry->Record.MutableSettings()->CopyFrom(retrySettings);
        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(), retry.release());
        env.TestWaitNotification(runtime, retryId);
        const auto result = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", retryId);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE, result.DebugString());
        checkSchema(true);
        checkRows(true);
    }
    TestDropTable(runtime, ++txId, "/MyRoot", "Table");
    env.TestWaitNotification(runtime, txId);
    TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {NLs::PathNotExist});
}

} // namespace

Y_UNIT_TEST_SUITE(ColumnBuildRebootsTest) {
    Y_UNIT_TEST(MixedColumnsRestartAfterPartialBackfill) {
        CheckMixedColumnsAfterPartialBackfill(EMixedBuildInterruption::Restart, false);
    }

    Y_UNIT_TEST_TWIN(MixedColumnsCancelAfterPartialBackfill, RebootDuringRollback) {
        CheckMixedColumnsAfterPartialBackfill(EMixedBuildInterruption::Cancel, RebootDuringRollback);
    }

    Y_UNIT_TEST_TWIN(MixedColumnsFailureAfterPartialBackfill, RebootDuringRollback) {
        CheckMixedColumnsAfterPartialBackfill(EMixedBuildInterruption::Failure, RebootDuringRollback);
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
            const TString nonDefaultColumnName = "plain_value";
            Ydb::TypedValue columnDefaultValue;
            columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT32);
            columnDefaultValue.mutable_value()->set_uint32_value(10);

            ui32 scanRequests = 0;
            THashSet<ui32> uploadedColumnIds;
            auto scanObserver = runtime.AddObserver<TEvDataShard::TEvBuildIndexCreateRequest>([&](auto& ev) {
                const auto& columns = ev->Get()->Record.GetColumnBuildSettings();
                UNIT_ASSERT_VALUES_EQUAL(columns.column_size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(columns.column(0).GetColumnName(), columnName);
                ++scanRequests;
            });
            auto uploadObserver = runtime.AddObserver<TEvDataShard::TEvUploadRowsRequest>([&](auto& ev) {
                const auto& rowScheme = ev->Get()->Record.GetRowScheme();
                UNIT_ASSERT_VALUES_EQUAL(rowScheme.ValueColumnIdsSize(), 1);
                uploadedColumnIds.insert(rowScheme.GetValueColumnIds(0));
            });

            AsyncBuildColumn(runtime, ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", columnName, columnDefaultValue, nonDefaultColumnName);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", t.TxId);
                UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);

                auto table = DescribePath(runtime, "/MyRoot/Table");
                TestDescribeResult(table,
                                   {NLs::PathExist,
                                    NLs::IndexesCount(0),
                                    NLs::PathVersionEqual(6),
                                    NLs::CheckColumns("Table", {"key", "value", columnName, nonDefaultColumnName}, {}, {"key"}, true)});
                UNIT_ASSERT(scanRequests > 0);
                UNIT_ASSERT_VALUES_EQUAL(uploadedColumnIds.size(), 1);
                for (const auto& column : table.GetPathDescription().GetTable().GetColumns()) {
                    if (column.GetName() == columnName) {
                        UNIT_ASSERT(uploadedColumnIds.contains(column.GetId()));
                    } else if (column.GetName() == nonDefaultColumnName) {
                        UNIT_ASSERT(!uploadedColumnIds.contains(column.GetId()));
                    }
                }
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
            const TString nonDefaultColumnName = "plain_value";
            Ydb::TypedValue columnDefaultValue;
            columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::UINT32);
            columnDefaultValue.mutable_value()->set_uint32_value(10);

            AsyncBuildColumn(runtime, ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", columnName, columnDefaultValue, nonDefaultColumnName);

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
                                    NLs::CheckColumns("Table", {"key", "value", columnName, nonDefaultColumnName}, {columnName, nonDefaultColumnName}, {"key"}, true)});
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
            const TString nonDefaultColumnName = "plain_value";
            Ydb::TypedValue columnDefaultValue;
            columnDefaultValue.mutable_type()->set_type_id(Ydb::Type::JSON);
            columnDefaultValue.mutable_value()->set_text_value("{not json]");

            AsyncBuildColumn(runtime, ++t.TxId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", columnName, columnDefaultValue, nonDefaultColumnName);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", t.TxId);
                UNIT_ASSERT_VALUES_EQUAL(descr.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathExist,
                                    NLs::IndexesCount(0),
                                    NLs::PathVersionEqual(7),
                                    NLs::CheckColumns("Table", {"key", "value", columnName, nonDefaultColumnName}, {columnName, nonDefaultColumnName}, {"key"}, true)});
            }
        });
    }
}
