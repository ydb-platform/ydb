#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/schemeshard/index/build_index.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

namespace NKikimr::NKqp {
using namespace NYdb;
using namespace NYdb::NQuery;
namespace {

NKikimrConfig::TAppConfig AddGeneratedConfig() {
    NKikimrConfig::TAppConfig config;
    config.MutableFeatureFlags()->SetEnableGeneratedStored(true);
    config.MutableFeatureFlags()->SetEnableAddColumnGeneratedStored(true);
    config.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);
    return config;
}

class TAddGeneratedFixture {
public:
    explicit TAddGeneratedFixture(NKikimrConfig::TAppConfig config = AddGeneratedConfig(), bool seed = true)
        : Kikimr(TKikimrSettings(config).SetWithSampleTables(false))
        , Client(Kikimr.GetQueryClient())
    {
        Exec("CREATE TABLE t (k Int64 NOT NULL, a Int64, other Int64, PRIMARY KEY(k));");
        if (seed) {
            Exec("UPSERT INTO t (k, a) VALUES (1, 3), (2, NULL);");
        }
    }
    void Exec(const std::string& sql) {
        auto result = Client.ExecuteQuery(sql, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), sql << "\n" << result.GetIssues().ToString());
    }
    void Check(const std::string& sql, const TString& expected) {
        auto result = Client.ExecuteQuery(sql, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(expected, FormatResultSetYson(result.GetResultSet(0)));
    }
    void Reject(const std::string& sql, TStringBuf issue = {}) {
        auto result = Client.ExecuteQuery(sql, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(!result.IsSuccess(), sql);
        if (issue) {
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), issue);
        }
    }
    TKikimrRunner Kikimr;
    TQueryClient Client;
};

void ConcurrentWrite(const std::string& sql, const TString& expected) {
    auto config = AddGeneratedConfig();
    TKikimrRunner kikimr(TKikimrSettings(config).SetWithSampleTables(false).SetUseRealThreads(false));
    auto client = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
    auto exec = [&](const std::string& query) {
        return kikimr.RunCall([&] { return client.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync(); });
    };
    UNIT_ASSERT(exec("CREATE TABLE t (k Int64 NOT NULL, a Int64, PRIMARY KEY(k));").IsSuccess());
    UNIT_ASSERT(exec("UPSERT INTO t (k, a) VALUES (1, 3), (2, NULL);").IsSuccess());
    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    bool capture = true;
    TVector<TAutoPtr<NActors::IEventHandle>> uploads;
    runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& event) {
        if (capture && event->GetTypeRewrite() == TEvDataShard::TEvUploadRowsRequest::EventType
            && event->Get<TEvDataShard::TEvUploadRowsRequest>()->Record.HasGeneratedColumnFillContext()) {
            uploads.emplace_back(event.Release());
            return TTestActorRuntime::EEventAction::DROP;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    });
    auto alter = kikimr.RunInThreadPool([&] {
        return client.ExecuteQuery("ALTER TABLE t ADD COLUMN g Int64 GENERATED ALWAYS AS (a + 1) STORED;", TTxControl::NoTx()).GetValueSync();
    });
    TDispatchOptions options;
    options.FinalEvents.emplace_back([&](NActors::IEventHandle&) { return !uploads.empty() || alter.HasValue(); });
    runtime.DispatchEvents(options);
    UNIT_ASSERT_C(!uploads.empty(), "Build failed before generated upload: " << (alter.HasValue() ? alter.GetValueSync().GetIssues().ToString() : std::string()));
    auto result = exec(sql);
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    // New writes maintain the hidden stored column before it becomes readable.
    capture = false;
    for (auto& event : uploads) {
        runtime.Send(event);
    }
    auto done = runtime.WaitFuture(alter);
    UNIT_ASSERT_C(done.IsSuccess(), done.GetIssues().ToString());
    result = exec("SELECT k, a, g FROM t ORDER BY k;");
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    CompareYson(expected, FormatResultSetYson(result.GetResultSet(0)));
}
} // namespace

Y_UNIT_TEST_SUITE(GeneratedStoredAddColumn) {
    Y_UNIT_TEST(ScalarBackfill) {
        TAddGeneratedFixture fixture;
        fixture.Exec("ALTER TABLE t ADD COLUMN g Int64 GENERATED ALWAYS AS (a + 1) STORED;");
        fixture.Check("SELECT k, a, g FROM t ORDER BY k;", "[[1;[3];[4]];[2;#;#]]");
        fixture.Exec("INSERT INTO t (k, a) VALUES (3, 6);");
        fixture.Exec("UPSERT INTO t (k, a) VALUES (1, 9);");
        fixture.Exec("REPLACE INTO t (k, a) VALUES (2, 7);");
        fixture.Exec("UPDATE t SET a = 12 WHERE k = 3;");
        fixture.Exec("UPDATE t ON SELECT 1 AS k, 20 AS a;");
        fixture.Exec("UPDATE t SET other = 55 WHERE k = 1;");
        fixture.Check("SELECT k, a, g FROM t ORDER BY k;", "[[1;[20];[21]];[2;[7];[8]];[3;[12];[13]]]");
        fixture.Reject("UPSERT INTO t (k, a, g) VALUES (1, 2, 100);", "GENERATED ALWAYS");
        auto description = fixture.Kikimr.GetTableClient().GetSession().GetValueSync().GetSession().DescribeTable("/Root/t").GetValueSync();
        UNIT_ASSERT_C(description.IsSuccess(), description.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(description.GetTableDescription().GetColumns().size(), 4);
    }
    Y_UNIT_TEST(NativeUdfBackfill) {
        TAddGeneratedFixture fixture;
        fixture.Exec("ALTER TABLE t ADD COLUMN g Utf8 GENERATED ALWAYS AS (Unicode::ToLower(CAST(COALESCE(a, 0) AS Utf8))) STORED;");
        fixture.Check("SELECT k, g FROM t ORDER BY k;", "[[1;[\"3\"]];[2;[\"0\"]]]");
    }
    Y_UNIT_TEST(NullableResult) {
        TAddGeneratedFixture fixture;
        fixture.Exec("ALTER TABLE t ADD COLUMN g Int64 GENERATED ALWAYS AS (a + 1) STORED;");
        fixture.Check("SELECT g FROM t WHERE k = 2;", "[[#]]");
    }
    Y_UNIT_TEST(NotNullResult) {
        TAddGeneratedFixture fixture;
        fixture.Exec("ALTER TABLE t ADD COLUMN g Int64 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) + 1) STORED;");
        fixture.Check("SELECT k, g FROM t ORDER BY k;", "[[1;4];[2;1]]");
    }
    Y_UNIT_TEST(EmptyTable) {
        TAddGeneratedFixture fixture(AddGeneratedConfig(), false);
        fixture.Exec("ALTER TABLE t ADD COLUMN g Int64 GENERATED ALWAYS AS (a + 1) STORED;");
        fixture.Exec("UPSERT INTO t (k, a) VALUES (1, 3);");
        fixture.Check("SELECT g FROM t;", "[[[4]]]");
    }
    Y_UNIT_TEST(RejectedExpressionLeavesSchemaUnchanged) {
        TAddGeneratedFixture fixture;
        fixture.Reject("ALTER TABLE t ADD COLUMN g Int64 GENERATED ALWAYS AS (g + 1) STORED;", "reference itself");
        fixture.Reject("ALTER TABLE t ADD COLUMN g Int64 NOT NULL GENERATED ALWAYS AS (a + 1) STORED;", "NULL");
        fixture.Reject("ALTER TABLE t ADD COLUMN g Int64 GENERATED ALWAYS AS ((SELECT COUNT(*) FROM t)) STORED;");
        fixture.Check("SELECT k, a FROM t ORDER BY k;", "[[1;[3]];[2;#]]");
        fixture.Exec("ALTER TABLE t ADD COLUMN g Int64 GENERATED ALWAYS AS (a + 1) STORED;");
    }
    Y_UNIT_TEST(FeatureFlagDisabled) {
        for (ui32 missing = 0; missing < 3; ++missing) {
            auto config = AddGeneratedConfig();
            if (missing == 0) config.MutableFeatureFlags()->SetEnableAddColumnGeneratedStored(false);
            if (missing == 1) config.MutableFeatureFlags()->SetEnableGeneratedStored(false);
            if (missing == 2) config.MutableTableServiceConfig()->SetEnableIndexStreamWrite(false);
            TAddGeneratedFixture fixture(config);
            fixture.Reject("ALTER TABLE t ADD COLUMN g Int64 GENERATED ALWAYS AS (a + 1) STORED;");
        }
    }
    Y_UNIT_TEST(SingleActionScope) {
        TAddGeneratedFixture fixture;
        fixture.Reject("ALTER TABLE t ADD COLUMN g Int64 GENERATED ALWAYS AS (a + 1) STORED, ADD COLUMN b Int64;");
        fixture.Reject("ALTER TABLE t ADD COLUMN g Int64 GENERATED ALWAYS AS (a + 1) STORED, ADD COLUMN h Int64 GENERATED ALWAYS AS (a + 2) STORED;");
        fixture.Reject("ALTER TABLE t ADD COLUMN g Int64 GENERATED ALWAYS AS (a + 1) VIRTUAL;", "not supported");
        fixture.Reject("ALTER TABLE t ADD COLUMN g Int64 GENERATED ALWAYS AS (a + 1);", "not supported");
    }
    Y_UNIT_TEST(ConcurrentDependencyUpdate) {
        ConcurrentWrite("UPDATE t SET a = 9 WHERE k = 1;", "[[1;[9];[10]];[2;#;#]]");
    }
    Y_UNIT_TEST(ConcurrentNullResult) {
        ConcurrentWrite("UPDATE t SET a = NULL WHERE k = 1;", "[[1;#;#];[2;#;#]]");
    }
    Y_UNIT_TEST(ConcurrentInsertAndReplace) {
        ConcurrentWrite("REPLACE INTO t (k, a) VALUES (1, 9), (3, 6);", "[[1;[9];[10]];[2;#;#];[3;[6];[7]]]");
    }
    Y_UNIT_TEST(DeleteReinsertDuringUpload) {
        ConcurrentWrite("DELETE FROM t WHERE k = 1; INSERT INTO t (k, a) VALUES (1, 20);", "[[1;[20];[21]];[2;#;#]]");
    }

    Y_UNIT_TEST(OldTransactionAcrossAlter) {
        TAddGeneratedFixture fixture;
        auto sessionResult = fixture.Client.GetSession().GetValueSync();
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();
        auto pending = session.ExecuteQuery("UPDATE t SET a = 99 WHERE k = 1;",
            TTxControl::BeginTx(TTxSettings::SerializableRW())).GetValueSync();
        UNIT_ASSERT_C(pending.IsSuccess(), pending.GetIssues().ToString());
        UNIT_ASSERT(pending.GetTransaction());
        fixture.Exec("ALTER TABLE t ADD COLUMN g Int64 GENERATED ALWAYS AS (a + 1) STORED;");
        auto transaction = *pending.GetTransaction();
        const auto committed = transaction.Commit().GetValueSync();
        UNIT_ASSERT_C(!committed.IsSuccess(), "A transaction writing the old schema committed after ALTER");
        fixture.Check("SELECT k, a, g FROM t ORDER BY k;", "[[1;[3];[4]];[2;#;#]]");
    }

    Y_UNIT_TEST(GeneratedAllowsIndependentDefaultBuild) {
        TAddGeneratedFixture fixture;
        fixture.Exec("ALTER TABLE t ADD COLUMN g Int64 GENERATED ALWAYS AS (a + 1) STORED;");
        fixture.Exec("ALTER TABLE t ADD COLUMN d Int64 DEFAULT 17;");
        fixture.Check("SELECT k, a, g, d FROM t ORDER BY k;", "[[1;[3];[4];[17]];[2;#;#;[17]]]");

        // SQL ADD Serial is intentionally unsupported. Exercise the existing
        // sequence-default build API on the table with a stored generated column.
        auto& server = fixture.Kikimr.GetTestServer();
        auto& runtime = *server.GetRuntime();
        const auto edge = runtime.AllocateEdgeActor();
        const auto schemeShard = Tests::ChangeStateStorage(Tests::SchemeRoot, server.GetSettings().Domain);
        const ui64 sequenceBuildId = 987654321;
        NKikimrIndexBuilder::TIndexBuildSettings settings;
        settings.set_source_path("/Root/t");
        auto* column = settings.mutable_column_build_operation()->add_column();
        column->SetColumnName("s");
        column->SetNotNull(true);
        column->mutable_default_from_literal()->mutable_type()->set_type_id(Ydb::Type::INT64);
        column->set_default_from_sequence("s_seq");
        runtime.SendToPipe(schemeShard, edge,
            new NSchemeShard::TEvIndexBuilder::TEvCreateRequest(sequenceBuildId, "/Root", std::move(settings)),
            0, GetPipeConfigWithRetries());
        const auto created = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvIndexBuilder::TEvCreateResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL_C(created->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS, created->Get()->Record.DebugString());
        runtime.SendToPipe(schemeShard, edge, new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion(sequenceBuildId),
            0, GetPipeConfigWithRetries());
        runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(edge);
        runtime.SendToPipe(schemeShard, edge, new NSchemeShard::TEvIndexBuilder::TEvGetRequest("/Root", sequenceBuildId),
            0, GetPipeConfigWithRetries());
        const auto built = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvIndexBuilder::TEvGetResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL_C(built->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS, built->Get()->Record.DebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(built->Get()->Record.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
            built->Get()->Record.DebugString());
        auto checkSequence = [&](size_t expectedRows) {
            const auto result = fixture.Client.ExecuteQuery("SELECT s FROM t;", TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            TResultSetParser rows(result.GetResultSet(0));
            THashSet<i64> values;
            size_t count = 0;
            while (rows.TryNextRow()) {
                values.insert(rows.ColumnParser("s").GetInt64());
                ++count;
            }
            UNIT_ASSERT_VALUES_EQUAL(count, expectedRows);
            UNIT_ASSERT_VALUES_EQUAL(values.size(), expectedRows);
        };
        checkSequence(2);
        fixture.Check("SELECT k, a, g, d FROM t ORDER BY k;", "[[1;[3];[4];[17]];[2;#;#;[17]]]");
        fixture.Exec("UPSERT INTO t (k, a) VALUES (1, 10);");
        fixture.Check("SELECT g, d FROM t WHERE k = 1;", "[[[11];[17]]]");
        fixture.Exec("INSERT INTO t (k, a) VALUES (3, 6);");
        checkSequence(3);
        fixture.Check("SELECT k, g, d FROM t ORDER BY k;", "[[1;[11];[17]];[2;#;[17]];[3;[7];[17]]]");
    }

    Y_UNIT_TEST(UnavailableNativeUdfCleansUpBuild) {
        const auto limitedRegistry = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());
        TKikimrRunner kikimr(TKikimrSettings(AddGeneratedConfig())
            .SetWithSampleTables(false).SetUseRealThreads(false));
        auto client = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
        auto exec = [&](const std::string& query) {
            return kikimr.RunCall([&] { return client.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync(); });
        };
        UNIT_ASSERT(exec("CREATE TABLE t (k Int64 NOT NULL, a Utf8, PRIMARY KEY(k));").IsSuccess());
        UNIT_ASSERT(exec("UPSERT INTO t (k, a) VALUES (1, 'ABC'u);").IsSuccess());
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        const auto* fullRegistry = runtime.GetAppData().FunctionRegistry;
        bool replaced = false;
        runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& event) {
            if (!replaced && event->GetTypeRewrite() == TEvDataShard::TEvBuildIndexCreateRequest::EventType) {
                const auto& request = event->Get<TEvDataShard::TEvBuildIndexCreateRequest>()->Record;
                if (request.GetColumnBuildSettings().column_size() == 1
                    && request.GetColumnBuildSettings().column(0).has_generated_from_expression())
                {
                    // KQP has compiled and preflighted against its installed registry.
                    // The scan worker must diagnose the different DataShard registry.
                    runtime.GetAppData().FunctionRegistry = limitedRegistry.Get();
                    replaced = true;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        const auto failed = exec("ALTER TABLE t ADD COLUMN g Utf8 GENERATED ALWAYS AS (Unicode::ToLower(a)) STORED;");
        runtime.GetAppData().FunctionRegistry = fullRegistry;
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        UNIT_ASSERT_C(replaced, failed.GetIssues().ToString());
        UNIT_ASSERT(!failed.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(failed.GetIssues().ToString(), "Unicode");
        const auto description = kikimr.RunCall([&] { return kikimr.GetTestClient().Ls("/Root/t"); });
        for (const auto& column : description->Record.GetPathDescription().GetTable().GetColumns()) {
            UNIT_ASSERT_VALUES_UNEQUAL(column.GetName(), "g");
        }
        const auto retried = exec("ALTER TABLE t ADD COLUMN g Utf8 GENERATED ALWAYS AS (Unicode::ToLower(a)) STORED;");
        UNIT_ASSERT_C(retried.IsSuccess(), retried.GetIssues().ToString());
        const auto rows = exec("SELECT g FROM t;");
        UNIT_ASSERT_C(rows.IsSuccess(), rows.GetIssues().ToString());
        CompareYson("[[[\"abc\"]]]", FormatResultSetYson(rows.GetResultSet(0)));
    }

    Y_UNIT_TEST(RandomResultRetryAndCompaction) {
        TKikimrRunner kikimr(TKikimrSettings(AddGeneratedConfig())
            .SetWithSampleTables(false).SetUseRealThreads(false));
        auto client = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
        auto exec = [&](const std::string& query) {
            return kikimr.RunCall([&] { return client.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync(); });
        };
        UNIT_ASSERT(exec("CREATE TABLE t (k Int64 NOT NULL, a Int64, PRIMARY KEY(k));").IsSuccess());
        UNIT_ASSERT(exec("UPSERT INTO t (k, a) VALUES (1, 3), (2, NULL);").IsSuccess());
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        THashMap<i64, TSerializedCellVec> committedValues;
        NKikimrTxDataShard::TGeneratedColumnFillContext binding;
        TActorId uploader;
        TAutoPtr<NActors::IEventHandle> lostAck;
        ui64 dataShardId = 0;
        runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvDataShard::TEvUploadRowsRequest::EventType) {
                const auto& request = event->Get<TEvDataShard::TEvUploadRowsRequest>()->Record;
                if (request.HasGeneratedColumnFillContext()) {
                    binding = request.GetGeneratedColumnFillContext();
                    uploader = event->Sender;
                    for (const auto& row : request.GetRows()) {
                        const TSerializedCellVec key(row.GetKeyColumns());
                        committedValues.emplace(key.GetCells()[0].AsValue<i64>(), TSerializedCellVec(row.GetValueColumns()));
                    }
                }
            } else if (event->GetTypeRewrite() == TEvDataShard::TEvUploadRowsResponse::EventType
                && uploader && event->GetRecipientRewrite() == uploader && !lostAck)
            {
                const auto& response = event->Get<TEvDataShard::TEvUploadRowsResponse>()->Record;
                UNIT_ASSERT_VALUES_EQUAL_C(response.GetStatus(), ui32(NKikimrTxDataShard::TError::OK), response.GetErrorDescription());
                dataShardId = response.GetTabletID();
                lostAck = event.Release();
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        auto alter = kikimr.RunInThreadPool([&] {
            return client.ExecuteQuery("ALTER TABLE t ADD COLUMN g Uint64 GENERATED ALWAYS AS "
                "(IF(a IS NULL, NULL, RandomNumber(1))) STORED;", TTxControl::NoTx()).GetValueSync();
        });
        TDispatchOptions options;
        options.FinalEvents.emplace_back([&](NActors::IEventHandle&) { return bool(lostAck) || alter.HasValue(); });
        runtime.DispatchEvents(options);
        UNIT_ASSERT_C(lostAck, "Generated build did not reach a committed upload");
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        UNIT_ASSERT_VALUES_EQUAL(committedValues.size(), 2);
        UNIT_ASSERT(!committedValues.at(1).GetCells()[0].IsNull());
        UNIT_ASSERT(committedValues.at(2).GetCells()[0].IsNull());

        const auto edge = runtime.AllocateEdgeActor();
        runtime.SendToPipe(dataShardId, edge, new TEvDataShard::TEvCompactTable(binding.GetOwnerId(), binding.GetPathId()),
            0, GetPipeConfigWithRetries());
        const auto compacted = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvCompactTableResult>(edge);
        UNIT_ASSERT(compacted->Get()->Record.GetStatus() != NKikimrTxDataShard::TEvCompactTableResult::FAILED);
        RebootTablet(runtime, dataShardId, edge);
        runtime.Send(lostAck);
        const auto completed = runtime.WaitFuture(alter);
        UNIT_ASSERT_C(completed.IsSuccess(), completed.GetIssues().ToString());
        const auto result = exec("SELECT k, g FROM t ORDER BY k;");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        TResultSetParser rows(result.GetResultSet(0));
        ui32 count = 0;
        while (rows.TryNextRow()) {
            const auto key = rows.ColumnParser("k").GetInt64();
            const auto actual = rows.ColumnParser("g").GetOptionalUint64();
            const auto& expected = committedValues.at(key).GetCells()[0];
            UNIT_ASSERT_VALUES_EQUAL(actual.has_value(), !expected.IsNull());
            if (actual) {
                UNIT_ASSERT_VALUES_EQUAL(*actual, expected.AsValue<ui64>());
            }
            ++count;
        }
        UNIT_ASSERT_VALUES_EQUAL(count, 2);
    }

    Y_UNIT_TEST(ForegroundRandomBeforeSnapshotSurvivesCompaction) {
        TKikimrRunner kikimr(TKikimrSettings(AddGeneratedConfig())
            .SetWithSampleTables(false).SetUseRealThreads(false));
        auto client = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
        auto exec = [&](const std::string& query) {
            return kikimr.RunCall([&] { return client.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync(); });
        };
        UNIT_ASSERT(exec("CREATE TABLE t (k Int64 NOT NULL, a Int64, PRIMARY KEY(k));").IsSuccess());
        UNIT_ASSERT(exec("UPSERT INTO t (k, a) VALUES (1, 3), (2, NULL);").IsSuccess());
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        ui64 hiddenAlterTxId = 0;
        ui64 hiddenSchemaVersion = 0;
        ui64 dataShardId = 0;
        ui32 targetColumnId = 0;
        TPathId pathId;
        bool holdSchemaChanged = true;
        TVector<TAutoPtr<NActors::IEventHandle>> schemaChanges;
        ui32 uploads = 0;
        runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvDataShard::TEvProposeTransaction::EventType) {
                const auto& request = event->Get<TEvDataShard::TEvProposeTransaction>()->Record;
                if (request.GetTxKind() == NKikimrTxDataShard::TX_KIND_SCHEME) {
                    NKikimrTxDataShard::TFlatSchemeTransaction scheme;
                    UNIT_ASSERT(scheme.ParseFromString(request.GetTxBody()));
                    if (scheme.HasAlterTable()) {
                        for (const auto& column : scheme.GetAlterTable().GetColumns()) {
                            if (column.GetName() == "g" && column.GetIsBuildInProgress()
                                && column.GetDefaultFromExpression().GetStored())
                            {
                                hiddenAlterTxId = request.GetTxId();
                                hiddenSchemaVersion = scheme.GetAlterTable().GetTableSchemaVersion();
                                pathId = TPathId::FromProto(scheme.GetAlterTable().GetPathId());
                                targetColumnId = column.GetId();
                            }
                        }
                    }
                }
            } else if (event->GetTypeRewrite() == TEvDataShard::TEvSchemaChanged::EventType) {
                const auto& response = event->Get<TEvDataShard::TEvSchemaChanged>()->Record;
                if (holdSchemaChanged && hiddenAlterTxId && response.GetTxId() == hiddenAlterTxId) {
                    dataShardId = response.GetOrigin();
                    schemaChanges.emplace_back(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            } else if (event->GetTypeRewrite() == TEvDataShard::TEvUploadRowsRequest::EventType
                && event->Get<TEvDataShard::TEvUploadRowsRequest>()->Record.HasGeneratedColumnFillContext())
            {
                ++uploads;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        auto alter = kikimr.RunInThreadPool([&] {
            return client.ExecuteQuery("ALTER TABLE t ADD COLUMN g Uint64 GENERATED ALWAYS AS "
                "(IF(a IS NULL, NULL, RandomNumber(1))) STORED;", TTxControl::NoTx()).GetValueSync();
        });
        TDispatchOptions options;
        options.FinalEvents.emplace_back([&](NActors::IEventHandle&) { return !schemaChanges.empty() || alter.HasValue(); });
        runtime.DispatchEvents(options);
        UNIT_ASSERT_C(!schemaChanges.empty(), "Generated build did not install the hidden schema");
        // SchemeShard cannot initiate the snapshot until this committed alter is
        // acknowledged. These values must therefore be materialized beforehand.
        const auto foreground = exec("UPSERT INTO t (k, a) VALUES (1, 7), (2, NULL);");
        UNIT_ASSERT_C(foreground.IsSuccess(), foreground.GetIssues().ToString());
        const auto edge = runtime.AllocateEdgeActor();
        auto read = MakeHolder<TEvDataShard::TEvRead>();
        read->Record.SetReadId(1);
        read->Record.MutableTableId()->SetOwnerId(pathId.OwnerId);
        read->Record.MutableTableId()->SetTableId(pathId.LocalPathId);
        read->Record.MutableTableId()->SetSchemaVersion(hiddenSchemaVersion);
        read->Record.AddColumns(targetColumnId);
        read->Record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);
        for (const i64 key : {i64(1), i64(2)}) {
            const TCell cell = TCell::Make(key);
            read->Keys.emplace_back(TConstArrayRef<TCell>(&cell, 1));
        }
        runtime.SendToPipe(dataShardId, edge, read.Release(), 0, GetPipeConfigWithRetries());
        const auto physical = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadResult>(edge);
        UNIT_ASSERT_VALUES_EQUAL_C(physical->Get()->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS,
            physical->Get()->Record.DebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(physical->Get()->GetRowsCount(), 2, physical->Get()->Record.DebugString());
        const TSerializedCellVec first(physical->Get()->GetCells(0));
        const TSerializedCellVec second(physical->Get()->GetCells(1));
        UNIT_ASSERT(!first.GetCells()[0].IsNull());
        UNIT_ASSERT(second.GetCells()[0].IsNull());

        runtime.SendToPipe(dataShardId, edge, new TEvDataShard::TEvCompactTable(pathId.OwnerId, pathId.LocalPathId),
            0, GetPipeConfigWithRetries());
        const auto compacted = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvCompactTableResult>(edge);
        UNIT_ASSERT(compacted->Get()->Record.GetStatus() != NKikimrTxDataShard::TEvCompactTableResult::FAILED);
        RebootTablet(runtime, dataShardId, edge);
        holdSchemaChanged = false;
        for (auto& change : schemaChanges) {
            runtime.Send(change);
        }
        const auto completed = runtime.WaitFuture(alter);
        UNIT_ASSERT_C(completed.IsSuccess(), completed.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(uploads, 0);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        const auto result = exec("SELECT k, g FROM t ORDER BY k;");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        TResultSetParser rows(result.GetResultSet(0));
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("k").GetInt64(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("g").GetOptionalUint64().value(), first.GetCells()[0].AsValue<ui64>());
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("k").GetInt64(), 2);
        UNIT_ASSERT(!rows.ColumnParser("g").GetOptionalUint64());
        UNIT_ASSERT(!rows.TryNextRow());
    }

    Y_UNIT_TEST(GeneratedCancelAfterPartialFillAndNameReuse) {
        TKikimrRunner kikimr(TKikimrSettings(AddGeneratedConfig())
            .SetWithSampleTables(false).SetUseRealThreads(false));
        auto client = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
        auto exec = [&](const std::string& query) {
            return kikimr.RunCall([&] { return client.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync(); });
        };
        UNIT_ASSERT(exec("CREATE TABLE t (k Int64 NOT NULL, a Int64, PRIMARY KEY(k));").IsSuccess());
        UNIT_ASSERT(exec("UPSERT INTO t (k, a) VALUES (1, 3), (2, NULL);").IsSuccess());
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        NKikimrTxDataShard::TEvUploadRowsRequest oldUpload;
        TAutoPtr<NActors::IEventHandle> lostAck;
        ui64 dataShardId = 0;
        runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& event) {
            if (event->GetTypeRewrite() == NSchemeShard::TEvIndexBuilder::TEvCreateRequest::EventType) {
                event->Get<NSchemeShard::TEvIndexBuilder::TEvCreateRequest>()->Record
                    .MutableSettings()->MutableScanSettings()->SetMaxBatchRows(1);
            } else if (event->GetTypeRewrite() == TEvDataShard::TEvUploadRowsRequest::EventType) {
                const auto& request = event->Get<TEvDataShard::TEvUploadRowsRequest>()->Record;
                if (request.HasGeneratedColumnFillContext()) {
                    if (lostAck) {
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    oldUpload = request;
                }
            } else if (event->GetTypeRewrite() == TEvDataShard::TEvUploadRowsResponse::EventType
                && oldUpload.HasGeneratedColumnFillContext() && !lostAck)
            {
                const auto& response = event->Get<TEvDataShard::TEvUploadRowsResponse>()->Record;
                UNIT_ASSERT_VALUES_EQUAL_C(response.GetStatus(), ui32(NKikimrTxDataShard::TError::OK), response.GetErrorDescription());
                dataShardId = response.GetTabletID();
                lostAck = event.Release();
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        auto alter = kikimr.RunInThreadPool([&] {
            return client.ExecuteQuery("ALTER TABLE t ADD COLUMN g Int64 GENERATED ALWAYS AS (a + 1) STORED;",
                TTxControl::NoTx()).GetValueSync();
        });
        TDispatchOptions options;
        options.FinalEvents.emplace_back([&](NActors::IEventHandle&) { return bool(lostAck) || alter.HasValue(); });
        runtime.DispatchEvents(options);
        UNIT_ASSERT_C(lostAck, "Generated build did not reach a partial committed upload");
        UNIT_ASSERT_VALUES_EQUAL(oldUpload.RowsSize(), 1);
        const auto& binding = oldUpload.GetGeneratedColumnFillContext();
        const auto edge = runtime.AllocateEdgeActor();
        auto cancel = MakeHolder<NSchemeShard::TEvIndexBuilder::TEvCancelRequest>();
        cancel->Record.SetTxId(99999999);
        cancel->Record.SetDatabaseName("/Root");
        cancel->Record.SetIndexBuildId(binding.GetBuildId());
        runtime.SendToPipe(binding.GetOwnerId(), edge, cancel.Release(), 0, GetPipeConfigWithRetries());
        const auto cancelled = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvIndexBuilder::TEvCancelResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL(cancelled->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS);
        const auto stopped = runtime.WaitFuture(alter);
        UNIT_ASSERT(!stopped.IsSuccess());
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        const auto retried = exec("ALTER TABLE t ADD COLUMN g Int64 GENERATED ALWAYS AS (a + 2) STORED;");
        UNIT_ASSERT_C(retried.IsSuccess(), retried.GetIssues().ToString());
        auto stale = MakeHolder<TEvDataShard::TEvUploadRowsRequest>();
        stale->Record = oldUpload;
        runtime.SendToPipe(dataShardId, edge, stale.Release(), 0, GetPipeConfigWithRetries());
        const auto rejected = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvUploadRowsResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL(rejected->Get()->Record.GetStatus(), ui32(NKikimrTxDataShard::TError::SCHEME_CHANGED));
        runtime.Send(lostAck);
        const auto rows = exec("SELECT k, a, g FROM t ORDER BY k;");
        UNIT_ASSERT_C(rows.IsSuccess(), rows.GetIssues().ToString());
        CompareYson("[[1;[3];[5]];[2;#;#]]", FormatResultSetYson(rows.GetResultSet(0)));
    }

    Y_UNIT_TEST(LiveMultiShardRebootAndPublication) {
        TKikimrRunner kikimr(TKikimrSettings(AddGeneratedConfig())
            .SetWithSampleTables(false).SetUseRealThreads(false));
        auto client = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
        auto exec = [&](const std::string& query) {
            return kikimr.RunCall([&] { return client.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync(); });
        };
        UNIT_ASSERT(exec("CREATE TABLE t (k Uint64 NOT NULL, a Int64, PRIMARY KEY(k)) WITH "
            "(UNIFORM_PARTITIONS = 4, AUTO_PARTITIONING_BY_SIZE = DISABLED, AUTO_PARTITIONING_BY_LOAD = DISABLED);").IsSuccess());
        const auto seeded = exec("UPSERT INTO t (k, a) VALUES (1ul, 3), (9223372036854775808ul, NULL), (18446744073709551614ul, 9);");
        UNIT_ASSERT_C(seeded.IsSuccess(), seeded.GetIssues().ToString());
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        THashSet<ui64> requestedShards;
        THashSet<ui64> completedShards;
        TVector<TAutoPtr<NActors::IEventHandle>> held;
        ui64 blockedShard = 0;
        ui64 schemeShard = 0;
        ui64 buildId = 0;
        bool hold = true;
        runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvDataShard::TEvBuildIndexCreateRequest::EventType) {
                const auto& request = event->Get<TEvDataShard::TEvBuildIndexCreateRequest>()->Record;
                if (request.GetColumnBuildSettings().column_size() == 1
                    && request.GetColumnBuildSettings().column(0).has_generated_from_expression())
                {
                    requestedShards.insert(request.GetTabletId());
                    schemeShard = request.GetOwnerId();
                    if (!buildId) {
                        buildId = request.GetId();
                    }
                    UNIT_ASSERT_VALUES_EQUAL(request.GetId(), buildId);
                    if (!blockedShard) {
                        blockedShard = request.GetTabletId();
                    }
                    if (hold && request.GetTabletId() == blockedShard) {
                        held.emplace_back(event.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
            } else if (event->GetTypeRewrite() == TEvDataShard::TEvBuildIndexProgressResponse::EventType) {
                const auto& progress = event->Get<TEvDataShard::TEvBuildIndexProgressResponse>()->Record;
                if (progress.GetId() == buildId && progress.GetStatus() == NKikimrIndexBuilder::DONE) {
                    completedShards.insert(progress.GetTabletId());
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        auto alter = kikimr.RunInThreadPool([&] {
            return client.ExecuteQuery("ALTER TABLE t ADD COLUMN g Int64 GENERATED ALWAYS AS (a + 1) STORED;",
                TTxControl::NoTx()).GetValueSync();
        });
        TDispatchOptions options;
        options.FinalEvents.emplace_back([&](NActors::IEventHandle&) {
            return (requestedShards.size() == 4 && completedShards.size() == 3) || alter.HasValue();
        });
        runtime.DispatchEvents(options);
        UNIT_ASSERT_VALUES_EQUAL(requestedShards.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(completedShards.size(), 3);
        UNIT_ASSERT(!alter.HasValue());
        UNIT_ASSERT(buildId);
        const auto edge = runtime.AllocateEdgeActor();
        RebootTablet(runtime, schemeShard, edge);
        runtime.SendToPipe(schemeShard, edge, new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion(buildId),
            0, GetPipeConfigWithRetries());
        hold = false;
        for (auto& request : held) {
            runtime.Send(request);
        }
        const auto notification = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(edge);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetTxId(), buildId);
        runtime.SendToPipe(schemeShard, edge, new NSchemeShard::TEvIndexBuilder::TEvGetRequest("/Root", buildId),
            0, GetPipeConfigWithRetries());
        const auto build = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvIndexBuilder::TEvGetResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL_C(build->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS, build->Get()->Record.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(build->Get()->Record.GetIndexBuild().GetId(), buildId);
        UNIT_ASSERT_VALUES_EQUAL_C(build->Get()->Record.GetIndexBuild().GetState(), Ydb::Table::IndexBuildState::STATE_DONE,
            build->Get()->Record.DebugString());
        const auto completed = runtime.WaitFuture(alter);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        if (!completed.IsSuccess()) {
            // The SDK may repeat the DDL after losing the SchemeShard reply.
            // Only this duplicate-column response is compatible with the original
            // persisted build having reached STATE_DONE above.
            UNIT_ASSERT_VALUES_EQUAL_C(completed.GetStatus(), EStatus::GENERIC_ERROR, completed.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(completed.GetIssues().Size(), 1, completed.GetIssues().ToString());
            const auto* issue = &*completed.GetIssues().begin();
            while (!issue->GetSubIssues().empty()) {
                UNIT_ASSERT_VALUES_EQUAL_C(issue->GetSubIssues().size(), 1, completed.GetIssues().ToString());
                issue = issue->GetSubIssues().front().Get();
            }
            UNIT_ASSERT_VALUES_EQUAL(issue->GetMessage(), "AlterTable : db.[/Root/t] Column: \"g\" already exists");
        }
        const auto description = kikimr.RunCall([&] { return kikimr.GetTestClient().Ls("/Root/t"); });
        bool published = false;
        for (const auto& column : description->Record.GetPathDescription().GetTable().GetColumns()) {
            if (column.GetName() == "g") {
                UNIT_ASSERT(column.GetDefaultFromExpression().GetStored());
                UNIT_ASSERT(!column.GetIsBuildInProgress());
                published = true;
            }
        }
        UNIT_ASSERT(published);
        const auto result = exec("SELECT k, g FROM t ORDER BY k;");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        TResultSetParser rows(result.GetResultSet(0));
        ui32 count = 0;
        while (rows.TryNextRow()) {
            const auto key = rows.ColumnParser("k").GetUint64();
            const auto value = rows.ColumnParser("g").GetOptionalInt64();
            if (key == 1) {
                UNIT_ASSERT(value && *value == 4);
            } else if (key == (ui64(1) << 63)) {
                UNIT_ASSERT(!value);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(key, Max<ui64>() - 1);
                UNIT_ASSERT(value && *value == 10);
            }
            ++count;
        }
        UNIT_ASSERT_VALUES_EQUAL(count, 3);
    }
}
} // namespace NKikimr::NKqp
