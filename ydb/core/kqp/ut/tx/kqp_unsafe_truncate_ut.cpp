#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <type_traits>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;

namespace {

constexpr const char* TablePath = "/Root/UnsafeTruncateTable";

TKikimrRunner MakeRunner(bool enableUnsafeTruncate) {
    auto settings = TKikimrSettings().SetWithSampleTables(false);
    settings.FeatureFlags.SetEnableUnsafeTruncateTable(enableUnsafeTruncate);
    return TKikimrRunner(settings);
}

template <bool UseQueryService>
using TTxControlFor = std::conditional_t<UseQueryService, NYdb::NQuery::TTxControl, NYdb::NTable::TTxControl>;

template <bool UseQueryService>
using TExecuteQuerySettingsFor = std::conditional_t<UseQueryService,
    NYdb::NQuery::TExecuteQuerySettings, NYdb::NTable::TExecDataQuerySettings>;

template <bool UseQueryService>
auto GetClient(TKikimrRunner& kikimr, const TString& authToken = {}) {
    if constexpr (UseQueryService) {
        return kikimr.GetQueryClient(NYdb::NQuery::TClientSettings().AuthToken(authToken));
    } else {
        return kikimr.GetTableClient(NYdb::NTable::TClientSettings().AuthToken(authToken));
    }
}

// Table Service requires an explicit transaction control for data queries.
template <bool UseQueryService>
auto AutoCommit() {
    if constexpr (UseQueryService) {
        return NYdb::NQuery::TTxControl::NoTx();
    } else {
        return NYdb::NTable::TTxControl::BeginTx().CommitTx();
    }
}

template <typename TSession>
auto ExecuteQuery(TSession& session, const TString& sql,
    const TTxControlFor<std::is_same_v<TSession, NYdb::NQuery::TSession>>& txControl,
    const TExecuteQuerySettingsFor<std::is_same_v<TSession, NYdb::NQuery::TSession>>& settings = {})
{
    if constexpr (std::is_same_v<TSession, NYdb::NQuery::TSession>) {
        return session.ExecuteQuery(sql, txControl, settings);
    } else {
        return session.ExecuteDataQuery(sql, txControl, settings);
    }
}

template <typename TSession>
auto ExecuteSchemeQuery(TSession& session, const TString& sql) {
    if constexpr (std::is_same_v<TSession, NYdb::NQuery::TSession>) {
        return session.ExecuteQuery(sql, NYdb::NQuery::TTxControl::NoTx());
    } else {
        return session.ExecuteSchemeQuery(sql);
    }
}

TString CountQuery() {
    return Sprintf("SELECT COUNT(*) AS cnt FROM `%s`;", TablePath);
}

TString UnsafeTruncateQuery(bool enablePragma = true) {
    return Sprintf(R"(
        PRAGMA kikimr.EnableUnsafeTruncateTable = "%s";
        TRUNCATE TABLE `%s` WITH (unsafe = true);
    )", enablePragma ? "true" : "false", TablePath);
}

template <typename TResult>
ui64 ReadCount(const TResult& result) {
    auto parser = result.GetResultSetParser(0);
    UNIT_ASSERT(parser.TryNextRow());
    return parser.ColumnParser("cnt").GetUint64();
}

template <typename TSession>
void CreateAndFill(TSession& session) {
    using TTxControl = TTxControlFor<std::is_same_v<TSession, NYdb::NQuery::TSession>>;
    auto create = ExecuteSchemeQuery(session, Sprintf(R"(
        CREATE TABLE `%s` (
            Key Uint64,
            Value String,
            PRIMARY KEY (Key)
        );
    )", TablePath)).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());

    auto fill = ExecuteQuery(session, Sprintf(R"(
        UPSERT INTO `%s` (Key, Value) VALUES (1u, "one"), (2u, "two"), (3u, "three");
    )", TablePath), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(fill.GetStatus(), EStatus::SUCCESS, fill.GetIssues().ToString());
}

template <typename TSession>
ui64 CountRows(TSession& session) {
    using TTxControl = TTxControlFor<std::is_same_v<TSession, NYdb::NQuery::TSession>>;
    auto result = ExecuteQuery(session, CountQuery(), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return ReadCount(result);
}

template <typename TSession>
ui64 CountOf(TSession& session, const TString& path) {
    using TTxControl = TTxControlFor<std::is_same_v<TSession, NYdb::NQuery::TSession>>;
    auto result = ExecuteQuery(session, Sprintf("SELECT COUNT(*) AS cnt FROM `%s`;", path.c_str()),
        TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return ReadCount(result);
}

void SplitShard(TKikimrRunner& kikimr, const TString& path, ui64 shard, ui64 splitKey) {
    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    TControlBoard::SetValue(-1, runtime.GetAppData().Icb->SchemeShardControls.SplitMergePartCountLimit);

    auto sender = runtime.AllocateEdgeActor();

    auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    request->Record.SetExecTimeoutPeriod(Max<ui64>());
    auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
    tx.SetOperationType(NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions);
    auto& desc = *tx.MutableSplitMergeTablePartitions();
    desc.SetTablePath(path);
    desc.AddSourceTabletId(shard);
    desc.AddSplitBoundary()->MutableKeyPrefix()->AddTuple()->MutableOptional()->SetUint64(splitKey);

    runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()), 0, true);
    auto status = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
    UNIT_ASSERT_VALUES_EQUAL(status->Get()->Record.GetStatus(),
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress);
    const ui64 txId = status->Get()->Record.GetTxId();

    auto notify = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
    notify->Record.SetTxId(txId);
    const auto schemeShard = NKikimr::Tests::ChangeStateStorage(
        NKikimr::Tests::SchemeRoot, kikimr.GetTestServer().GetSettings().Domain);
    runtime.SendToPipe(schemeShard, sender, notify.Release(), 0, GetPipeConfigWithRetries());
    runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(sender);
}

void MergeShards(TKikimrRunner& kikimr, const TString& path, ui64 left, ui64 right) {
    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    TControlBoard::SetValue(-1, runtime.GetAppData().Icb->SchemeShardControls.SplitMergePartCountLimit);

    auto sender = runtime.AllocateEdgeActor();

    auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    request->Record.SetExecTimeoutPeriod(Max<ui64>());
    auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
    tx.SetOperationType(NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions);
    auto& desc = *tx.MutableSplitMergeTablePartitions();
    desc.SetTablePath(path);
    desc.AddSourceTabletId(left);
    desc.AddSourceTabletId(right);

    runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()), 0, true);
    auto status = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
    UNIT_ASSERT_VALUES_EQUAL(status->Get()->Record.GetStatus(),
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress);

    auto notify = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
    notify->Record.SetTxId(status->Get()->Record.GetTxId());
    const auto schemeShard = NKikimr::Tests::ChangeStateStorage(
        NKikimr::Tests::SchemeRoot, kikimr.GetTestServer().GetSettings().Domain);
    runtime.SendToPipe(schemeShard, sender, notify.Release(), 0, GetPipeConfigWithRetries());
    runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(sender);
}

ui64 GetSchemaVersion(TKikimrRunner& kikimr, const TString& path) {
    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    const auto describe = DescribeTable(&kikimr.GetTestServer(), runtime.AllocateEdgeActor(), path);
    return describe.GetPathDescription().GetTable().GetTableSchemaVersion();
}

template <typename TSession>
void ExecDdl(TSession& session, const TString& sql) {
    auto result = ExecuteSchemeQuery(session, sql).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

// Uint64 keys spread over the whole domain, so UNIFORM_PARTITIONS puts one row on each shard.
// A truncate that only reached the first shard would leave the other three rows behind.
constexpr ui64 ShardKeys[] = {
    1ull,
    4611686018427387904ull,  // 2^62
    9223372036854775808ull,  // 2^63
    13835058055282163712ull, // 3 * 2^62
};

template <typename TSession>
void CreateAndFillSharded(TSession& session) {
    using TTxControl = TTxControlFor<std::is_same_v<TSession, NYdb::NQuery::TSession>>;
    ExecDdl(session, Sprintf(R"(
        CREATE TABLE `%s` (
            Key Uint64,
            Value String,
            PRIMARY KEY (Key)
        ) WITH (
            UNIFORM_PARTITIONS = 4
        );
    )", TablePath));

    TStringBuilder values;
    for (size_t i = 0; i < Y_ARRAY_SIZE(ShardKeys); ++i) {
        values << (i ? ", " : "") << "(" << ShardKeys[i] << "ul, \"v" << i << "\")";
    }

    auto fill = ExecuteQuery(session, Sprintf("UPSERT INTO `%s` (Key, Value) VALUES %s;",
        TablePath, values.c_str()), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(fill.GetStatus(), EStatus::SUCCESS, fill.GetIssues().ToString());
}

} // namespace

Y_UNIT_TEST_SUITE(KqpUnsafeTruncate) {

    Y_UNIT_TEST_TWIN(FeatureFlagDisabled, UseQueryService) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ false);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto result = ExecuteQuery(session, UnsafeTruncateQuery(), AutoCommit<UseQueryService>()).ExtractValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), EStatus::SUCCESS,
            "unsafe truncate must be rejected while the feature flag is off");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "disabled");

        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 3u);
    }

    Y_UNIT_TEST_TWIN(FeatureFlagAndPragmaDisabled, UseQueryService) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ false);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto result = ExecuteQuery(session, UnsafeTruncateQuery(/* enablePragma */ false),
            AutoCommit<UseQueryService>()).ExtractValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "disabled");
        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 3u);
    }

    Y_UNIT_TEST_TWIN(PragmaDisabled, UseQueryService) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto result = ExecuteQuery(session, UnsafeTruncateQuery(/* enablePragma */ false),
            AutoCommit<UseQueryService>()).ExtractValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
            "requires PRAGMA kikimr.EnableUnsafeTruncateTable");
        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 3u);
    }

    Y_UNIT_TEST_TWIN(UnknownSettingRejected, UseQueryService) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto result = ExecuteQuery(session, Sprintf(
            "PRAGMA kikimr.EnableUnsafeTruncateTable = \"true\"; "
            "TRUNCATE TABLE `%s` WITH (nonsense = true);", TablePath), AutoCommit<UseQueryService>()).ExtractValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Unknown TRUNCATE TABLE setting");
    }

    // The plain statement still goes through SchemeShard exactly as before.
    Y_UNIT_TEST_TWIN(PlainTruncateStillWorks, UseQueryService) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto result = ExecuteSchemeQuery(session, Sprintf(
            "PRAGMA kikimr.EnableUnsafeTruncateTable = \"true\"; "
            "TRUNCATE TABLE `%s`;", TablePath)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
    }

    Y_UNIT_TEST_TWIN(WipesTable, UseQueryService) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto result = ExecuteQuery(session, UnsafeTruncateQuery(), AutoCommit<UseQueryService>()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
    }

    // The point of the whole feature: the statement runs inside T_user without aborting it.
    Y_UNIT_TEST_TWIN(InsideTransaction, UseQueryService) {
        using TTxControl = TTxControlFor<UseQueryService>;
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto before = ExecuteQuery(session, CountQuery(), TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(before.GetStatus(), EStatus::SUCCESS, before.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReadCount(before), 3u);

        auto tx = before.GetTransaction();
        UNIT_ASSERT(tx);

        auto trunc = ExecuteQuery(session, UnsafeTruncateQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(trunc.GetStatus(), EStatus::SUCCESS, trunc.GetIssues().ToString());

        auto after = ExecuteQuery(session, CountQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(after.GetStatus(), EStatus::SUCCESS, after.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReadCount(after), 0u);

        auto commit = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
    }

    // Anomaly (a): T_trunc is committed on its own, so rolling T_user back does not bring rows back.
    Y_UNIT_TEST_TWIN(SurvivesRollback, UseQueryService) {
        using TTxControl = TTxControlFor<UseQueryService>;
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto before = ExecuteQuery(session, CountQuery(), TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(before.GetStatus(), EStatus::SUCCESS, before.GetIssues().ToString());

        auto tx = before.GetTransaction();
        UNIT_ASSERT(tx);

        auto trunc = ExecuteQuery(session, UnsafeTruncateQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(trunc.GetStatus(), EStatus::SUCCESS, trunc.GetIssues().ToString());

        auto rollback = tx->Rollback().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(rollback.GetStatus(), EStatus::SUCCESS, rollback.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
    }

    // Anomaly (b): the effect is visible outside T_user before T_user commits.
    Y_UNIT_TEST_TWIN(VisibleInConcurrentTransaction, UseQueryService) {
        using TTxControl = TTxControlFor<UseQueryService>;
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session1 = client.GetSession().GetValueSync().GetSession();
        auto session2 = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session1);

        auto before = ExecuteQuery(session1, CountQuery(), TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(before.GetStatus(), EStatus::SUCCESS, before.GetIssues().ToString());

        auto tx = before.GetTransaction();
        UNIT_ASSERT(tx);

        auto trunc = ExecuteQuery(session1, UnsafeTruncateQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(trunc.GetStatus(), EStatus::SUCCESS, trunc.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL_C(CountRows(session2), 0u,
            "the truncate must be visible to others while T_user is still open");

        auto commit = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());
    }

    // Everything above runs on a single shard, which takes the immediate path. From here on the
    // table has several shards, so the truncate really goes through prepare and the coordinator.
    Y_UNIT_TEST_TWIN(MultiShardWipesAllShards, UseQueryService) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFillSharded(session);

        UNIT_ASSERT_VALUES_EQUAL_C(CountRows(session), Y_ARRAY_SIZE(ShardKeys),
            "the rows must be spread over the shards, otherwise this proves nothing");

        auto result = ExecuteQuery(session, UnsafeTruncateQuery(), AutoCommit<UseQueryService>()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
    }

    Y_UNIT_TEST_TWIN(MultiShardInsideTransaction, UseQueryService) {
        using TTxControl = TTxControlFor<UseQueryService>;
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFillSharded(session);

        auto before = ExecuteQuery(session, CountQuery(), TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(before.GetStatus(), EStatus::SUCCESS, before.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReadCount(before), Y_ARRAY_SIZE(ShardKeys));

        auto tx = before.GetTransaction();
        UNIT_ASSERT(tx);

        auto trunc = ExecuteQuery(session, UnsafeTruncateQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(trunc.GetStatus(), EStatus::SUCCESS, trunc.GetIssues().ToString());

        auto after = ExecuteQuery(session, CountQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(after.GetStatus(), EStatus::SUCCESS, after.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReadCount(after), 0u);

        auto commit = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());
    }

    // The index impl table must be wiped in the same transaction, or the table and its index
    // silently disagree. Read the impl table directly: a query through VIEW would join the empty
    // main table and report zero even if the index still held rows.
    Y_UNIT_TEST_TWIN(WithIndexWipesImplTable, UseQueryService) {
        using TTxControl = TTxControlFor<UseQueryService>;
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();

        ExecDdl(session, R"(
            CREATE TABLE `/Root/UnsafeTruncateIndexed` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key),
                INDEX idx GLOBAL ON (Value)
            );
        )");

        auto fill = ExecuteQuery(session, R"(
            UPSERT INTO `/Root/UnsafeTruncateIndexed` (Key, Value)
            VALUES (1u, "a"), (2u, "b"), (3u, "c");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(fill.GetStatus(), EStatus::SUCCESS, fill.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(CountOf(session, "/Root/UnsafeTruncateIndexed"), 3u);
        UNIT_ASSERT_VALUES_EQUAL_C(CountOf(session, "/Root/UnsafeTruncateIndexed/idx/indexImplTable"), 3u,
            "the index must hold the rows before the truncate, otherwise this proves nothing");

        auto result = ExecuteQuery(session, R"(
            PRAGMA kikimr.EnableUnsafeTruncateTable = "true";
            TRUNCATE TABLE `/Root/UnsafeTruncateIndexed` WITH (unsafe = true);
        )", AutoCommit<UseQueryService>()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(CountOf(session, "/Root/UnsafeTruncateIndexed"), 0u);
        UNIT_ASSERT_VALUES_EQUAL_C(CountOf(session, "/Root/UnsafeTruncateIndexed/idx/indexImplTable"), 0u,
            "the index impl table must be wiped together with the main table");
    }

    Y_UNIT_TEST_TWIN(AsyncIndexRejected, UseQueryService) {
        using TTxControl = TTxControlFor<UseQueryService>;
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();

        ExecDdl(session, R"(
            CREATE TABLE `/Root/UnsafeTruncateAsync` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key),
                INDEX idx GLOBAL ASYNC ON (Value)
            );
        )");

        auto fill = ExecuteQuery(session, R"(
            UPSERT INTO `/Root/UnsafeTruncateAsync` (Key, Value) VALUES (1u, "a");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(fill.GetStatus(), EStatus::SUCCESS, fill.GetIssues().ToString());

        auto result = ExecuteQuery(session, R"(
            PRAGMA kikimr.EnableUnsafeTruncateTable = "true";
            TRUNCATE TABLE `/Root/UnsafeTruncateAsync` WITH (unsafe = true);
        )", AutoCommit<UseQueryService>()).ExtractValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), EStatus::SUCCESS,
            "an async index cannot be kept in sync by this operation, so it must be refused");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "synchronous");

        UNIT_ASSERT_VALUES_EQUAL(CountOf(session, "/Root/UnsafeTruncateAsync"), 1u);
    }

    Y_UNIT_TEST_TWIN(ChangefeedRejected, UseQueryService) {
        using TTxControl = TTxControlFor<UseQueryService>;
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();

        ExecDdl(session, R"(
            CREATE TABLE `/Root/UnsafeTruncateCdc` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )");
        ExecDdl(session, R"(
            ALTER TABLE `/Root/UnsafeTruncateCdc` ADD CHANGEFEED `feed` WITH (
                MODE = 'UPDATES', FORMAT = 'JSON'
            );
        )");

        auto fill = ExecuteQuery(session, R"(
            UPSERT INTO `/Root/UnsafeTruncateCdc` (Key, Value) VALUES (1u, "a");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(fill.GetStatus(), EStatus::SUCCESS, fill.GetIssues().ToString());

        auto result = ExecuteQuery(session, R"(
            PRAGMA kikimr.EnableUnsafeTruncateTable = "true";
            TRUNCATE TABLE `/Root/UnsafeTruncateCdc` WITH (unsafe = true);
        )", AutoCommit<UseQueryService>()).ExtractValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), EStatus::SUCCESS,
            "wiping rows without emitting change records would silently break the feed");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "changefeed");

        UNIT_ASSERT_VALUES_EQUAL(CountOf(session, "/Root/UnsafeTruncateCdc"), 1u);
    }

    // Anomaly (d): being a data-plane operation, it must not bump the schema version the way the
    // plain statement does. The plain form is measured too, so the check cannot pass vacuously.
    Y_UNIT_TEST_TWIN(SchemaVersionUnchanged, UseQueryService) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        const ui64 before = GetSchemaVersion(kikimr, TablePath);

        auto unsafe = ExecuteQuery(session, UnsafeTruncateQuery(), AutoCommit<UseQueryService>()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(unsafe.GetStatus(), EStatus::SUCCESS, unsafe.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL_C(GetSchemaVersion(kikimr, TablePath), before,
            "unsafe truncate must not touch the schema version");

        auto plain = ExecuteSchemeQuery(session, Sprintf(
            "PRAGMA kikimr.EnableUnsafeTruncateTable = \"true\"; "
            "TRUNCATE TABLE `%s`;", TablePath)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(plain.GetStatus(), EStatus::SUCCESS, plain.GetIssues().ToString());

        UNIT_ASSERT_C(GetSchemaVersion(kikimr, TablePath) > before,
            "the plain statement is expected to bump it, otherwise the check above measures nothing");
    }

    // Anomaly (c), the issuing side: T_user keeps its own locks and carries on.
    Y_UNIT_TEST_TWIN(LocksOfIssuingTransactionSurvive, UseQueryService) {
        using TTxControl = TTxControlFor<UseQueryService>;
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto before = ExecuteQuery(session, CountQuery(), TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(before.GetStatus(), EStatus::SUCCESS, before.GetIssues().ToString());
        auto tx = before.GetTransaction();
        UNIT_ASSERT(tx);

        auto trunc = ExecuteQuery(session, UnsafeTruncateQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(trunc.GetStatus(), EStatus::SUCCESS, trunc.GetIssues().ToString());

        auto write = ExecuteQuery(session, Sprintf(
            "UPSERT INTO `%s` (Key, Value) VALUES (42u, \"after\");", TablePath),
            TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(write.GetStatus(), EStatus::SUCCESS, write.GetIssues().ToString());

        auto commit = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL_C(CountRows(session), 1u,
            "the write issued after the truncate must survive it");
    }

    // Rows written earlier in the same transaction are not committed yet and, with sinks, may still
    // be sitting in the buffer actor. They must be wiped all the same: the statement forces them
    // out to the shards first, otherwise the same SQL would give a different answer depending on
    // whether a flush happened to occur.
    Y_UNIT_TEST_TWIN(UncommittedWritesAreWiped, UseQueryService) {
        using TTxControl = TTxControlFor<UseQueryService>;
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto begin = ExecuteQuery(session, Sprintf(
            "UPSERT INTO `%s` (Key, Value) VALUES (100u, \"uncommitted\");", TablePath),
            TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(begin.GetStatus(), EStatus::SUCCESS, begin.GetIssues().ToString());

        auto tx = begin.GetTransaction();
        UNIT_ASSERT(tx);

        auto trunc = ExecuteQuery(session, UnsafeTruncateQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(trunc.GetStatus(), EStatus::SUCCESS, trunc.GetIssues().ToString());

        auto after = ExecuteQuery(session, CountQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(after.GetStatus(), EStatus::SUCCESS, after.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(ReadCount(after), 0u,
            "the row written earlier in this very transaction must be gone too");

        auto commit = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL_C(CountRows(session), 0u,
            "and it must not reappear once the transaction commits");
    }

    // The whole shape the feature exists for, in one transaction.
    Y_UNIT_TEST_TWIN(FullTransactionScenario, UseQueryService) {
        using TTxControl = TTxControlFor<UseQueryService>;
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto begin = ExecuteQuery(session, Sprintf(
            "UPSERT INTO `%s` (Key, Value) VALUES (10u, \"before\");", TablePath),
            TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(begin.GetStatus(), EStatus::SUCCESS, begin.GetIssues().ToString());

        auto tx = begin.GetTransaction();
        UNIT_ASSERT(tx);

        auto beforeCount = ExecuteQuery(session, CountQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(beforeCount.GetStatus(), EStatus::SUCCESS, beforeCount.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(ReadCount(beforeCount), 4u,
            "three seeded rows plus the one just written in this transaction");

        auto trunc = ExecuteQuery(session, UnsafeTruncateQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(trunc.GetStatus(), EStatus::SUCCESS, trunc.GetIssues().ToString());

        auto afterCount = ExecuteQuery(session, CountQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(afterCount.GetStatus(), EStatus::SUCCESS, afterCount.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReadCount(afterCount), 0u);

        auto write = ExecuteQuery(session, Sprintf(
            "UPSERT INTO `%s` (Key, Value) VALUES (20u, \"after\");", TablePath),
            TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(write.GetStatus(), EStatus::SUCCESS, write.GetIssues().ToString());

        auto commit = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL_C(CountRows(session), 1u,
            "only the row written after the truncate survives");
    }

    // The shape the feature exists for: writes, reads and the truncate in one query text, executed
    // in statement order. The truncate is compiled as a transaction of the data query, not through
    // the scheme path, which is what lets it sit between them at all.
    Y_UNIT_TEST_TWIN(MixedWithDataInOneQuery, UseQueryService) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto result = ExecuteQuery(session, Sprintf(R"(
            PRAGMA kikimr.EnableUnsafeTruncateTable = "true";
            UPSERT INTO `%s` (Key, Value) VALUES (10u, "before");
            SELECT COUNT(*) AS cnt FROM `%s`;
            TRUNCATE TABLE `%s` WITH (unsafe = true);
            SELECT COUNT(*) AS cnt FROM `%s`;
        )", TablePath, TablePath, TablePath, TablePath), AutoCommit<UseQueryService>()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetResultSets().size(), 2u, "both SELECTs must produce a result");

        {
            auto before = result.GetResultSetParser(0);
            UNIT_ASSERT(before.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL_C(before.ColumnParser("cnt").GetUint64(), 4u,
                "the read before the truncate must see the write that precedes it");
        }
        {
            auto after = result.GetResultSetParser(1);
            UNIT_ASSERT(after.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL_C(after.ColumnParser("cnt").GetUint64(), 0u,
                "the read after the truncate must see an empty table");
        }

        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
    }

    // Anomaly (c), the other side: everyone else's locks are broken.
    Y_UNIT_TEST_TWIN(CompetingTransactionAborted, UseQueryService) {
        using TTxControl = TTxControlFor<UseQueryService>;
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session1 = client.GetSession().GetValueSync().GetSession();
        auto session2 = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session1);

        // The read is what actually takes a lock on the shard: with sinks a lone UPSERT sits in the
        // buffer actor until commit, so there would be nothing for the truncate to break.
        auto competingRead = ExecuteQuery(session2, CountQuery(), TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(competingRead.GetStatus(), EStatus::SUCCESS, competingRead.GetIssues().ToString());
        auto competingTx = competingRead.GetTransaction();
        UNIT_ASSERT(competingTx);

        // The write is what makes the commit reach the shard at all, so the broken lock is noticed.
        auto competingWrite = ExecuteQuery(session2, Sprintf(
            "UPSERT INTO `%s` (Key, Value) VALUES (7u, \"competing\");", TablePath),
            TTxControl::Tx(*competingTx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(competingWrite.GetStatus(), EStatus::SUCCESS, competingWrite.GetIssues().ToString());

        auto trunc = ExecuteQuery(session1, UnsafeTruncateQuery(), AutoCommit<UseQueryService>()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(trunc.GetStatus(), EStatus::SUCCESS, trunc.GetIssues().ToString());

        auto commit = competingTx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::ABORTED, commit.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL_C(CountRows(session1), 0u,
            "the aborted transaction must not have left its row behind");
    }

    // The truncate opens the transaction, so there is no lock to preserve yet and
    // PreserveLockTxIds goes out empty.
    Y_UNIT_TEST_TWIN(TruncateAsFirstStatement, UseQueryService) {
        using TTxControl = TTxControlFor<UseQueryService>;
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto trunc = ExecuteQuery(session, UnsafeTruncateQuery(), TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(trunc.GetStatus(), EStatus::SUCCESS, trunc.GetIssues().ToString());

        auto tx = trunc.GetTransaction();
        if (tx) {
            auto write = ExecuteQuery(session, Sprintf(
                "UPSERT INTO `%s` (Key, Value) VALUES (1u, \"again\");", TablePath),
                TTxControl::Tx(*tx)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(write.GetStatus(), EStatus::SUCCESS, write.GetIssues().ToString());

            auto commit = tx->Commit().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 1u);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
        }
    }

    // A table repartitioned since it was created: the shard set the executer resolves is not the
    // one the table was born with, and every descendant must still be wiped.
    Y_UNIT_TEST_TWIN(TruncateAfterSplit, UseQueryService) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFillSharded(session);

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        const auto shardsBefore = GetTableShards(&kikimr.GetTestServer(), runtime.AllocateEdgeActor(), TablePath);
        UNIT_ASSERT_VALUES_EQUAL(shardsBefore.size(), 4u);

        SplitShard(kikimr, TablePath, shardsBefore.at(0), ShardKeys[0] + 1);

        const auto shardsAfter = GetTableShards(&kikimr.GetTestServer(), runtime.AllocateEdgeActor(), TablePath);
        UNIT_ASSERT_VALUES_EQUAL_C(shardsAfter.size(), 5u, "the split must have happened");

        auto result = ExecuteQuery(session, UnsafeTruncateQuery(), AutoCommit<UseQueryService>()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
    }

    // Truncating an already empty table is a no-op, which is what makes a client retry after
    // UNDETERMINED safe.
    Y_UNIT_TEST_TWIN(TruncateIsIdempotent, UseQueryService) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFillSharded(session);

        for (int i = 0; i < 3; ++i) {
            auto result = ExecuteQuery(session, UnsafeTruncateQuery(), AutoCommit<UseQueryService>()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
        }
    }

    // A shard that has started splitting refuses everything with STATUS_OVERLOADED rather than a
    // distinctive status, and that refusal is what must send the executer back to resolve a fresh
    // shard set. Injecting the refusal directly keeps the test off any timing: forcing a real split
    // to land inside the resolve->prepare window would be inherently racy, while the code being
    // exercised - RestartOrFail, the new TxId, dropping the results of the abandoned attempt - is
    // the same either way.
    Y_UNIT_TEST_TWIN(ReResolvesWhenPrepareIsRefused, UseQueryService) {
        using TTxControl = TTxControlFor<UseQueryService>;
        // Observers are only honoured while the runtime is single threaded, which in turn means
        // every client call has to go through RunCall so the runtime keeps being pumped.
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false);
        settings.FeatureFlags.SetEnableUnsafeTruncateTable(true);
        TKikimrRunner kikimr(settings);

        auto client = GetClient<UseQueryService>(kikimr);
        auto session = kikimr.RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

        auto exec = [&](const TString& sql, bool inTx) {
            return kikimr.RunCall([&] {
                return ExecuteQuery(session, sql,
                    inTx ? TTxControl::BeginTx().CommitTx() : AutoCommit<UseQueryService>()).ExtractValueSync();
            });
        };

        {
            auto create = kikimr.RunCall([&] {
                return ExecuteSchemeQuery(session, Sprintf(R"(
                    CREATE TABLE `%s` (
                        Key Uint64,
                        Value String,
                        PRIMARY KEY (Key)
                    ) WITH (
                        UNIFORM_PARTITIONS = 4
                    );
                )", TablePath)).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());
        }

        {
            TStringBuilder values;
            for (size_t i = 0; i < Y_ARRAY_SIZE(ShardKeys); ++i) {
                values << (i ? ", " : "") << "(" << ShardKeys[i] << "ul, \"v" << i << "\")";
            }
            auto fill = exec(Sprintf("UPSERT INTO `%s` (Key, Value) VALUES %s;",
                TablePath, values.c_str()), /* inTx */ true);
            UNIT_ASSERT_VALUES_EQUAL_C(fill.GetStatus(), EStatus::SUCCESS, fill.GetIssues().ToString());
        }

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        std::atomic<int> refused{0};

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NKikimr::NEvents::TDataEvents::TEvWriteResult::EventType) {
                auto* msg = ev->Get<NKikimr::NEvents::TDataEvents::TEvWriteResult>();
                if (msg && msg->Record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED
                    && refused.fetch_add(1) == 0)
                {
                    msg->Record.SetStatus(NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED);
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto result = exec(UnsafeTruncateQuery(), /* inTx */ false);

        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

        UNIT_ASSERT_C(refused.load() > 0, "no prepare was refused, so no retry was exercised");
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto count = exec(CountQuery(), /* inTx */ true);
        UNIT_ASSERT_VALUES_EQUAL_C(count.GetStatus(), EStatus::SUCCESS, count.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReadCount(count), 0u);
    }

    // A table that keeps repartitioning must eventually give a clear error instead of spinning:
    // every prepare is refused here, so the resolve->prepare loop runs into its attempt cap.
    Y_UNIT_TEST_TWIN(GivesUpAfterTooManyRefusals, UseQueryService) {
        using TTxControl = TTxControlFor<UseQueryService>;
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false);
        settings.FeatureFlags.SetEnableUnsafeTruncateTable(true);
        TKikimrRunner kikimr(settings);

        auto client = GetClient<UseQueryService>(kikimr);
        auto session = kikimr.RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

        auto exec = [&](const TString& sql, bool inTx) {
            return kikimr.RunCall([&] {
                return ExecuteQuery(session, sql,
                    inTx ? TTxControl::BeginTx().CommitTx() : AutoCommit<UseQueryService>()).ExtractValueSync();
            });
        };

        {
            auto create = kikimr.RunCall([&] {
                return ExecuteSchemeQuery(session, Sprintf(R"(
                    CREATE TABLE `%s` (
                        Key Uint64,
                        Value String,
                        PRIMARY KEY (Key)
                    ) WITH (
                        UNIFORM_PARTITIONS = 4
                    );
                )", TablePath)).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());
        }

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        std::atomic<int> refused{0};

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NKikimr::NEvents::TDataEvents::TEvWriteResult::EventType) {
                auto* msg = ev->Get<NKikimr::NEvents::TDataEvents::TEvWriteResult>();
                if (msg && msg->Record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED) {
                    refused.fetch_add(1);
                    msg->Record.SetStatus(NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED);
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto result = exec(UnsafeTruncateQuery(), /* inTx */ false);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

        UNIT_ASSERT_C(refused.load() > 1, "the loop must have retried, not given up at once");
        UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "repartitioning");
    }

    Y_UNIT_TEST_TWIN(RestartAllShardsDuringCommit, UseQueryService) {
        using TTxControl = TTxControlFor<UseQueryService>;
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false);
        settings.FeatureFlags.SetEnableUnsafeTruncateTable(true);
        TKikimrRunner kikimr(settings);

        auto client = GetClient<UseQueryService>(kikimr);
        auto session = kikimr.RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
        kikimr.RunCall([&] { CreateAndFillSharded(session); });
        UNIT_ASSERT_VALUES_EQUAL(kikimr.RunCall([&] { return CountRows(session); }), 4u);

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        const auto shards = GetTableShards(&kikimr.GetTestServer(), runtime.AllocateEdgeActor(), TablePath);
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 4u);
        const THashSet<ui64> tableShards(shards.begin(), shards.end());
        const THashSet<ui64> delayedShards{shards[2], shards[3]};
        TVector<TActorId> shardActors;
        for (ui64 shard : shards) {
            shardActors.push_back(ResolveTablet(runtime, shard));
        }

        ui64 truncateTxId = 0;
        THashSet<ui64> preparedShards;
        THashSet<ui64> completedShards;
        auto observeResults = runtime.AddObserver<NEvents::TDataEvents::TEvWriteResult>(
            [&](NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
                const auto& record = ev->Get()->Record;
                if (!tableShards.contains(record.GetOrigin())) {
                    return;
                }
                if (record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED) {
                    if (!truncateTxId) {
                        truncateTxId = record.GetTxId();
                    }
                    UNIT_ASSERT_VALUES_EQUAL(record.GetTxId(), truncateTxId);
                    preparedShards.insert(record.GetOrigin());
                } else if (record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED
                    && record.GetTxId() == truncateTxId)
                {
                    completedShards.insert(record.GetOrigin());
                }
            });

        // Hold the commit decision at half of the shards. The other half must durably
        // complete the truncate before any tablet is restarted.
        THashSet<ui64> blockedShards;
        TBlockEvents<TEvTxProcessing::TEvPlanStep> blockedPlans(runtime, [&](const auto& ev) {
            const auto& record = ev->Get()->Record;
            if (delayedShards.contains(record.GetTabletID())) {
                for (const auto& tx : record.GetTransactions()) {
                    if (truncateTxId && tx.GetTxId() == truncateTxId) {
                        blockedShards.insert(record.GetTabletID());
                        return true;
                    }
                }
            }
            return false;
        });

        auto truncateFuture = kikimr.RunInThreadPool([&] {
            return ExecuteQuery(session, UnsafeTruncateQuery(), AutoCommit<UseQueryService>()).ExtractValueSync();
        });
        runtime.WaitFor("half of the shards committed unsafe truncate", [&] {
            return completedShards.size() == 2 && blockedShards.size() == 2;
        }, TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(preparedShards.size(), shards.size());
        for (ui64 shard : delayedShards) {
            UNIT_ASSERT(!completedShards.contains(shard));
        }
        UNIT_ASSERT(!truncateFuture.HasValue());

        THashSet<ui64> rebootedShards;
        auto observeBoots = runtime.AddObserver<TEvTablet::TEvBoot>([&](TEvTablet::TEvBoot::TPtr& ev) {
            const ui64 shard = ev->Get()->TabletID;
            if (tableShards.contains(shard)) {
                rebootedShards.insert(shard);
            }
        });

        // Synchronous sends kill every old actor before any shard can resume processing.
        for (const auto& actor : shardActors) {
            runtime.Send(new IEventHandle(actor, TActorId(), new TEvents::TEvPoison),
                actor.NodeId() - runtime.GetFirstNodeId(), /* viaActorSystem */ false);
        }
        // Discard events addressed to the old actors. The mediator must redeliver the
        // plan to the new generations; the test never retries the SQL statement.
        blockedPlans.Stop().clear();

        runtime.WaitFor("all shards rebooted and the original truncate completed", [&] {
            return rebootedShards.size() == shards.size() && completedShards.size() == shards.size();
        }, TDuration::Seconds(30));
        observeResults.Remove();
        observeBoots.Remove();

        auto truncateResult = runtime.WaitFuture(truncateFuture, TDuration::Seconds(30));
        // Losing contact after planning makes the client outcome uncertain even though
        // the shards recover and finish the already committed transaction.
        UNIT_ASSERT_VALUES_EQUAL_C(truncateResult.GetStatus(), EStatus::UNDETERMINED,
            truncateResult.GetIssues().ToString());

        auto observer = kikimr.RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
        UNIT_ASSERT_VALUES_EQUAL_C(kikimr.RunCall([&] { return CountRows(observer); }), 0u,
            "completed shards must stay empty and prepared shards must finish the original truncate");

        TStringBuilder values;
        for (size_t i = 0; i < Y_ARRAY_SIZE(ShardKeys); ++i) {
            values << (i ? ", " : "") << "(" << ShardKeys[i] << "ul, \"after restart\")";
        }
        auto refill = kikimr.RunCall([&] {
            return ExecuteQuery(observer, Sprintf("UPSERT INTO `%s` (Key, Value) VALUES %s;",
                TablePath, values.c_str()), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        });
        UNIT_ASSERT_VALUES_EQUAL_C(refill.GetStatus(), EStatus::SUCCESS, refill.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(kikimr.RunCall([&] { return CountRows(observer); }), 4u,
            "all restarted shards must accept new reads and writes");
    }

    // Losing the client after the coordinator has planned the transaction is the one case the
    // client cannot be told anything definite: the shards apply the truncate regardless, so the
    // answer is UNDETERMINED and the rows stay gone.
    Y_UNIT_TEST_TWIN(CancelledAfterPlanIsNotRolledBack, UseQueryService) {
        using TTxControl = TTxControlFor<UseQueryService>;
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false);
        settings.FeatureFlags.SetEnableUnsafeTruncateTable(true);
        TKikimrRunner kikimr(settings);

        auto client = GetClient<UseQueryService>(kikimr);
        auto session = kikimr.RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

        auto exec = [&](const TString& sql, bool inTx, const TExecuteQuerySettingsFor<UseQueryService>& s = {}) {
            return kikimr.RunCall([&] {
                return ExecuteQuery(session, sql,
                    inTx ? TTxControl::BeginTx().CommitTx() : AutoCommit<UseQueryService>(), s).ExtractValueSync();
            });
        };

        {
            auto create = kikimr.RunCall([&] {
                return ExecuteSchemeQuery(session, Sprintf(R"(
                    CREATE TABLE `%s` (
                        Key Uint64,
                        Value String,
                        PRIMARY KEY (Key)
                    ) WITH (
                        UNIFORM_PARTITIONS = 4
                    );
                )", TablePath)).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());
        }

        {
            TStringBuilder values;
            for (size_t i = 0; i < Y_ARRAY_SIZE(ShardKeys); ++i) {
                values << (i ? ", " : "") << "(" << ShardKeys[i] << "ul, \"v" << i << "\")";
            }
            auto fill = exec(Sprintf("UPSERT INTO `%s` (Key, Value) VALUES %s;",
                TablePath, values.c_str()), /* inTx */ true);
            UNIT_ASSERT_VALUES_EQUAL_C(fill.GetStatus(), EStatus::SUCCESS, fill.GetIssues().ToString());
        }

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        std::atomic<int> swallowed{0};

        // The shards do apply the truncate; only the executer never hears about it.
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NKikimr::NEvents::TDataEvents::TEvWriteResult::EventType) {
                auto* msg = ev->Get<NKikimr::NEvents::TDataEvents::TEvWriteResult>();
                if (msg && msg->Record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED) {
                    swallowed.fetch_add(1);
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        TExecuteQuerySettingsFor<UseQueryService> querySettings;
        querySettings.ClientTimeout(TDuration::Seconds(5));

        auto result = exec(UnsafeTruncateQuery(), /* inTx */ false, querySettings);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

        UNIT_ASSERT_C(swallowed.load() > 0, "the truncate never reached the shards");
        UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), EStatus::SUCCESS,
            "the executer was never told the truncate finished");

        // The abandoned query still occupies its session, so read the outcome from another one -
        // which is also how anybody else would observe it.
        auto observer = kikimr.RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
        auto count = kikimr.RunCall([&] {
            return ExecuteQuery(observer, CountQuery(), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        });
        UNIT_ASSERT_VALUES_EQUAL_C(count.GetStatus(), EStatus::SUCCESS, count.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(ReadCount(count), 0u,
            "a planned truncate is not rolled back just because the client gave up on it");
    }

    Y_UNIT_TEST_TWIN(TruncateAfterMerge, UseQueryService) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFillSharded(session);

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        const auto shardsBefore = GetTableShards(&kikimr.GetTestServer(), runtime.AllocateEdgeActor(), TablePath);
        UNIT_ASSERT_VALUES_EQUAL(shardsBefore.size(), 4u);

        // UNIFORM_PARTITIONS also sets the minimum partition count, so the table cannot be merged
        // below four. Split first and merge the two halves back, which keeps it at the limit.
        SplitShard(kikimr, TablePath, shardsBefore.at(0), ShardKeys[0] + 1);

        const auto shardsSplit = GetTableShards(&kikimr.GetTestServer(), runtime.AllocateEdgeActor(), TablePath);
        UNIT_ASSERT_VALUES_EQUAL(shardsSplit.size(), 5u);

        MergeShards(kikimr, TablePath, shardsSplit.at(0), shardsSplit.at(1));

        const auto shardsAfter = GetTableShards(&kikimr.GetTestServer(), runtime.AllocateEdgeActor(), TablePath);
        UNIT_ASSERT_VALUES_EQUAL_C(shardsAfter.size(), 4u, "the merge must have happened");

        auto result = ExecuteQuery(session, UnsafeTruncateQuery(), AutoCommit<UseQueryService>()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
    }

    // Losing the client while the truncate is still preparing must produce an error, not an
    // unhandled event: before TEvAbortExecution was handled the executer asserted and died.
    Y_UNIT_TEST_TWIN(CancelledWhilePreparing, UseQueryService) {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false);
        settings.FeatureFlags.SetEnableUnsafeTruncateTable(true);
        TKikimrRunner kikimr(settings);

        auto client = GetClient<UseQueryService>(kikimr);
        auto session = kikimr.RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

        {
            auto create = kikimr.RunCall([&] {
                return ExecuteSchemeQuery(session, Sprintf(R"(
                    CREATE TABLE `%s` (
                        Key Uint64,
                        Value String,
                        PRIMARY KEY (Key)
                    ) WITH (
                        UNIFORM_PARTITIONS = 4
                    );
                )", TablePath)).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());
        }

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        std::atomic<int> swallowed{0};

        // The prepare never gets an answer, so the truncate stays in its pre-plan phase until the
        // client gives up on it.
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NKikimr::NEvents::TDataEvents::TEvWriteResult::EventType) {
                auto* msg = ev->Get<NKikimr::NEvents::TDataEvents::TEvWriteResult>();
                if (msg && msg->Record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED) {
                    swallowed.fetch_add(1);
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        // Losing the client is what drives the abort down to the executer.
        TExecuteQuerySettingsFor<UseQueryService> querySettings;
        querySettings.ClientTimeout(TDuration::Seconds(5));

        auto result = kikimr.RunCall([&] {
            return ExecuteQuery(session, UnsafeTruncateQuery(), AutoCommit<UseQueryService>(), querySettings)
                .ExtractValueSync();
        });

        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

        UNIT_ASSERT_C(swallowed.load() > 0, "the truncate never reached the prepare phase");
        UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), EStatus::SUCCESS,
            "a truncate whose prepare was never answered cannot report success");
    }

    // Truncating an impl table on its own is the one way this statement could produce the very
    // disagreement it goes out of its way to avoid: an empty index over a full table. The plain
    // TRUNCATE and every write refuse it, so this must too.
    Y_UNIT_TEST_TWIN(IndexImplTableRejected, UseQueryService) {
        using TTxControl = TTxControlFor<UseQueryService>;
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();

        ExecDdl(session, R"(
            CREATE TABLE `/Root/UnsafeTruncateImpl` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key),
                INDEX idx GLOBAL ON (Value)
            );
        )");

        auto fill = ExecuteQuery(session, R"(
            UPSERT INTO `/Root/UnsafeTruncateImpl` (Key, Value) VALUES (1u, "a"), (2u, "b");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(fill.GetStatus(), EStatus::SUCCESS, fill.GetIssues().ToString());

        const TString implPath = "/Root/UnsafeTruncateImpl/idx/indexImplTable";

        auto result = ExecuteQuery(session, Sprintf(
            "PRAGMA kikimr.EnableUnsafeTruncateTable = \"true\"; "
            "TRUNCATE TABLE `%s` WITH (unsafe = true);", implPath.c_str()),
            AutoCommit<UseQueryService>()).ExtractValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), EStatus::SUCCESS,
            "wiping an index impl table alone would leave the index disagreeing with its table");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "index implementation table");

        UNIT_ASSERT_VALUES_EQUAL_C(CountOf(session, implPath), 2u, "the index must be untouched");
        UNIT_ASSERT_VALUES_EQUAL(CountOf(session, "/Root/UnsafeTruncateImpl"), 2u);

        // The table itself still truncates, impl table included.
        auto viaTable = ExecuteQuery(session, R"(
            PRAGMA kikimr.EnableUnsafeTruncateTable = "true";
            TRUNCATE TABLE `/Root/UnsafeTruncateImpl` WITH (unsafe = true);
        )", AutoCommit<UseQueryService>()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(viaTable.GetStatus(), EStatus::SUCCESS, viaTable.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(CountOf(session, "/Root/UnsafeTruncateImpl"), 0u);
        UNIT_ASSERT_VALUES_EQUAL(CountOf(session, implPath), 0u);
    }

    // Wiping a table is at least as destructive as deleting its rows, so the statement must require
    // the same right a DELETE does. The UPSERT and the plain TRUNCATE under the same user are the
    // negative controls: if either of them were allowed, the environment would be enforcing nothing
    // and this test would measure nothing.
    Y_UNIT_TEST_TWIN(AclReaderCannotTruncate, UseQueryService) {
        using TTxControl = TTxControlFor<UseQueryService>;
        const TString user = "user0@builtin";

        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.FeatureFlags.SetEnableUnsafeTruncateTable(true);
        TKikimrRunner kikimr(settings);

        auto admin = GetClient<UseQueryService>(kikimr).GetSession().GetValueSync().GetSession();
        CreateAndFill(admin);

        auto grant = [&](const TString& path, const std::vector<std::string>& rights) {
            auto driver = NYdb::TDriver(NYdb::TDriverConfig()
                .SetEndpoint(kikimr.GetEndpoint())
                .SetDatabase("/Root")
                .SetAuthToken("root@builtin"));
            auto schemeClient = NYdb::NScheme::TSchemeClient(driver);
            auto result = schemeClient.ModifyPermissions(path,
                NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(
                    NYdb::NScheme::TPermissions(user, rights))).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            Tests::TClient::RefreshPathCache(kikimr.GetTestServer().GetRuntime(), path);
        };

        grant("/Root", {"ydb.database.connect"});
        WaitForProxy(kikimr, user);
        grant(TablePath, {"ydb.deprecated.describe_schema", "ydb.deprecated.select_row"});

        auto userClient = GetClient<UseQueryService>(kikimr, user);
        auto reader = userClient.GetSession().GetValueSync().GetSession();

        auto select = ExecuteQuery(reader, CountQuery(), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(select.GetStatus(), EStatus::SUCCESS, select.GetIssues().ToString());

        auto upsert = ExecuteQuery(reader, Sprintf(
            "UPSERT INTO `%s` (Key, Value) VALUES (9u, \"x\");", TablePath),
            TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL_C(upsert.GetStatus(), EStatus::SUCCESS,
            "a reader may not write, otherwise this test measures nothing");

        auto plain = ExecuteSchemeQuery(reader, Sprintf(
            "PRAGMA kikimr.EnableUnsafeTruncateTable = \"true\"; "
            "TRUNCATE TABLE `%s`;", TablePath)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(plain.GetStatus(), EStatus::UNAUTHORIZED, plain.GetIssues().ToString());

        auto unsafe = ExecuteQuery(reader, UnsafeTruncateQuery(), AutoCommit<UseQueryService>()).ExtractValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL_C(unsafe.GetStatus(), EStatus::SUCCESS,
            "a user who may not even delete a row must not be able to wipe the table");
        UNIT_ASSERT_STRING_CONTAINS(unsafe.GetIssues().ToString(), "Access denied");

        UNIT_ASSERT_VALUES_EQUAL_C(CountRows(admin), 3u, "nothing may have been wiped");
    }

    // Several truncates interleaved with writes and reads in one query text. Each truncate gets a
    // query block of its own, so this is what would break if a block ever held anything else.
    Y_UNIT_TEST_TWIN(InterleavedTruncatesInOneQuery, UseQueryService) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto result = ExecuteQuery(session, Sprintf(R"(
            PRAGMA kikimr.EnableUnsafeTruncateTable = "true";
            UPSERT INTO `%s` (Key, Value) VALUES (10u, "a");
            TRUNCATE TABLE `%s` WITH (unsafe = true);
            UPSERT INTO `%s` (Key, Value) VALUES (20u, "b"), (21u, "b2");
            SELECT COUNT(*) AS cnt FROM `%s`;
            TRUNCATE TABLE `%s` WITH (unsafe = true);
            UPSERT INTO `%s` (Key, Value) VALUES (30u, "c");
            SELECT COUNT(*) AS cnt FROM `%s`;
        )", TablePath, TablePath, TablePath, TablePath, TablePath, TablePath, TablePath),
            AutoCommit<UseQueryService>()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 2u);
        {
            auto first = result.GetResultSetParser(0);
            UNIT_ASSERT(first.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL_C(first.ColumnParser("cnt").GetUint64(), 2u,
                "only the two rows written after the first truncate may be visible");
        }
        {
            auto second = result.GetResultSetParser(1);
            UNIT_ASSERT(second.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL_C(second.ColumnParser("cnt").GetUint64(), 1u,
                "only the row written after the second truncate may be visible");
        }

        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 1u);
    }

    // The path travels from the parsed statement to the executer as written, so a prefix or a bare
    // relative name has to survive the trip.
    Y_UNIT_TEST_TWIN(RelativePathIsResolved, UseQueryService) {
        using TTxControl = TTxControlFor<UseQueryService>;
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = GetClient<UseQueryService>(kikimr);
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto viaPrefix = ExecuteQuery(session, R"(
            PRAGMA TablePathPrefix = "/Root";
            PRAGMA kikimr.EnableUnsafeTruncateTable = "true";
            TRUNCATE TABLE `UnsafeTruncateTable` WITH (unsafe = true);
        )", AutoCommit<UseQueryService>()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(viaPrefix.GetStatus(), EStatus::SUCCESS, viaPrefix.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);

        auto refill = ExecuteQuery(session, Sprintf(
            R"(UPSERT INTO `%s` (Key, Value) VALUES (1u, "one"), (2u, "two");)", TablePath),
            TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(refill.GetStatus(), EStatus::SUCCESS, refill.GetIssues().ToString());

        auto bare = ExecuteQuery(session,
            "PRAGMA kikimr.EnableUnsafeTruncateTable = \"true\"; "
            "TRUNCATE TABLE `UnsafeTruncateTable` WITH (unsafe = true);",
            AutoCommit<UseQueryService>()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(bare.GetStatus(), EStatus::SUCCESS, bare.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
    }
}

} // namespace NKqp
} // namespace NKikimr
