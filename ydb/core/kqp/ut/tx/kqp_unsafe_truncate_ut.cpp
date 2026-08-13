#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

constexpr const char* TablePath = "/Root/UnsafeTruncateTable";

TKikimrRunner MakeRunner(bool enableUnsafeTruncate) {
    auto settings = TKikimrSettings().SetWithSampleTables(false);
    settings.FeatureFlags.SetEnableUnsafeTruncateTable(enableUnsafeTruncate);
    return TKikimrRunner(settings);
}

TString CountQuery() {
    return Sprintf("SELECT COUNT(*) AS cnt FROM `%s`;", TablePath);
}

TString UnsafeTruncateQuery() {
    return Sprintf("TRUNCATE TABLE `%s` WITH (unsafe = true);", TablePath);
}

ui64 ReadCount(const TExecuteQueryResult& result) {
    auto parser = result.GetResultSetParser(0);
    UNIT_ASSERT(parser.TryNextRow());
    return parser.ColumnParser("cnt").GetUint64();
}

void CreateAndFill(TSession& session) {
    auto create = session.ExecuteQuery(Sprintf(R"(
        CREATE TABLE `%s` (
            Key Uint64,
            Value String,
            PRIMARY KEY (Key)
        );
    )", TablePath), TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());

    auto fill = session.ExecuteQuery(Sprintf(R"(
        UPSERT INTO `%s` (Key, Value) VALUES (1u, "one"), (2u, "two"), (3u, "three");
    )", TablePath), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(fill.GetStatus(), EStatus::SUCCESS, fill.GetIssues().ToString());
}

ui64 CountRows(TSession& session) {
    auto result = session.ExecuteQuery(CountQuery(), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return ReadCount(result);
}

ui64 CountOf(TSession& session, const TString& path) {
    auto result = session.ExecuteQuery(Sprintf("SELECT COUNT(*) AS cnt FROM `%s`;", path.c_str()),
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

void ExecDdl(TSession& session, const TString& sql) {
    auto result = session.ExecuteQuery(sql, TTxControl::NoTx()).ExtractValueSync();
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

void CreateAndFillSharded(TSession& session) {
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

    auto fill = session.ExecuteQuery(Sprintf("UPSERT INTO `%s` (Key, Value) VALUES %s;",
        TablePath, values.c_str()), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(fill.GetStatus(), EStatus::SUCCESS, fill.GetIssues().ToString());
}

} // namespace

Y_UNIT_TEST_SUITE(KqpUnsafeTruncate) {

    Y_UNIT_TEST(DisabledByDefault) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ false);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto result = session.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), EStatus::SUCCESS,
            "unsafe truncate must be rejected while the feature flag is off");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "disabled");

        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 3u);
    }

    Y_UNIT_TEST(UnknownSettingRejected) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto result = session.ExecuteQuery(Sprintf(
            "TRUNCATE TABLE `%s` WITH (nonsense = true);", TablePath), TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Unknown TRUNCATE TABLE setting");
    }

    // The plain statement still goes through SchemeShard exactly as before.
    Y_UNIT_TEST(PlainTruncateStillWorks) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto result = session.ExecuteQuery(Sprintf(
            "TRUNCATE TABLE `%s`;", TablePath), TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
    }

    Y_UNIT_TEST(WipesTable) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto result = session.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
    }

    // The point of the whole feature: the statement runs inside T_user without aborting it.
    Y_UNIT_TEST(InsideTransaction) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto before = session.ExecuteQuery(CountQuery(), TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(before.GetStatus(), EStatus::SUCCESS, before.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReadCount(before), 3u);

        auto tx = before.GetTransaction();
        UNIT_ASSERT(tx);

        auto trunc = session.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(trunc.GetStatus(), EStatus::SUCCESS, trunc.GetIssues().ToString());

        auto after = session.ExecuteQuery(CountQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(after.GetStatus(), EStatus::SUCCESS, after.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReadCount(after), 0u);

        auto commit = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
    }

    // Anomaly (a): T_trunc is committed on its own, so rolling T_user back does not bring rows back.
    Y_UNIT_TEST(SurvivesRollback) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto before = session.ExecuteQuery(CountQuery(), TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(before.GetStatus(), EStatus::SUCCESS, before.GetIssues().ToString());

        auto tx = before.GetTransaction();
        UNIT_ASSERT(tx);

        auto trunc = session.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(trunc.GetStatus(), EStatus::SUCCESS, trunc.GetIssues().ToString());

        auto rollback = tx->Rollback().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(rollback.GetStatus(), EStatus::SUCCESS, rollback.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
    }

    // Anomaly (b): the effect is visible outside T_user before T_user commits.
    Y_UNIT_TEST(VisibleInConcurrentTransaction) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session1 = client.GetSession().GetValueSync().GetSession();
        auto session2 = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session1);

        auto before = session1.ExecuteQuery(CountQuery(), TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(before.GetStatus(), EStatus::SUCCESS, before.GetIssues().ToString());

        auto tx = before.GetTransaction();
        UNIT_ASSERT(tx);

        auto trunc = session1.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(trunc.GetStatus(), EStatus::SUCCESS, trunc.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL_C(CountRows(session2), 0u,
            "the truncate must be visible to others while T_user is still open");

        auto commit = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());
    }

    // Everything above runs on a single shard, which takes the immediate path. From here on the
    // table has several shards, so the truncate really goes through prepare and the coordinator.
    Y_UNIT_TEST(MultiShardWipesAllShards) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFillSharded(session);

        UNIT_ASSERT_VALUES_EQUAL_C(CountRows(session), Y_ARRAY_SIZE(ShardKeys),
            "the rows must be spread over the shards, otherwise this proves nothing");

        auto result = session.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
    }

    Y_UNIT_TEST(MultiShardInsideTransaction) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFillSharded(session);

        auto before = session.ExecuteQuery(CountQuery(), TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(before.GetStatus(), EStatus::SUCCESS, before.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReadCount(before), Y_ARRAY_SIZE(ShardKeys));

        auto tx = before.GetTransaction();
        UNIT_ASSERT(tx);

        auto trunc = session.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(trunc.GetStatus(), EStatus::SUCCESS, trunc.GetIssues().ToString());

        auto after = session.ExecuteQuery(CountQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(after.GetStatus(), EStatus::SUCCESS, after.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReadCount(after), 0u);

        auto commit = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());
    }

    // The index impl table must be wiped in the same transaction, or the table and its index
    // silently disagree. Read the impl table directly: a query through VIEW would join the empty
    // main table and report zero even if the index still held rows.
    Y_UNIT_TEST(WithIndexWipesImplTable) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();

        ExecDdl(session, R"(
            CREATE TABLE `/Root/UnsafeTruncateIndexed` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key),
                INDEX idx GLOBAL ON (Value)
            );
        )");

        auto fill = session.ExecuteQuery(R"(
            UPSERT INTO `/Root/UnsafeTruncateIndexed` (Key, Value)
            VALUES (1u, "a"), (2u, "b"), (3u, "c");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(fill.GetStatus(), EStatus::SUCCESS, fill.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(CountOf(session, "/Root/UnsafeTruncateIndexed"), 3u);
        UNIT_ASSERT_VALUES_EQUAL_C(CountOf(session, "/Root/UnsafeTruncateIndexed/idx/indexImplTable"), 3u,
            "the index must hold the rows before the truncate, otherwise this proves nothing");

        auto result = session.ExecuteQuery(R"(
            TRUNCATE TABLE `/Root/UnsafeTruncateIndexed` WITH (unsafe = true);
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(CountOf(session, "/Root/UnsafeTruncateIndexed"), 0u);
        UNIT_ASSERT_VALUES_EQUAL_C(CountOf(session, "/Root/UnsafeTruncateIndexed/idx/indexImplTable"), 0u,
            "the index impl table must be wiped together with the main table");
    }

    Y_UNIT_TEST(AsyncIndexRejected) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();

        ExecDdl(session, R"(
            CREATE TABLE `/Root/UnsafeTruncateAsync` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key),
                INDEX idx GLOBAL ASYNC ON (Value)
            );
        )");

        auto fill = session.ExecuteQuery(R"(
            UPSERT INTO `/Root/UnsafeTruncateAsync` (Key, Value) VALUES (1u, "a");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(fill.GetStatus(), EStatus::SUCCESS, fill.GetIssues().ToString());

        auto result = session.ExecuteQuery(R"(
            TRUNCATE TABLE `/Root/UnsafeTruncateAsync` WITH (unsafe = true);
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), EStatus::SUCCESS,
            "an async index cannot be kept in sync by this operation, so it must be refused");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "synchronous");

        UNIT_ASSERT_VALUES_EQUAL(CountOf(session, "/Root/UnsafeTruncateAsync"), 1u);
    }

    Y_UNIT_TEST(ChangefeedRejected) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
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

        auto fill = session.ExecuteQuery(R"(
            UPSERT INTO `/Root/UnsafeTruncateCdc` (Key, Value) VALUES (1u, "a");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(fill.GetStatus(), EStatus::SUCCESS, fill.GetIssues().ToString());

        auto result = session.ExecuteQuery(R"(
            TRUNCATE TABLE `/Root/UnsafeTruncateCdc` WITH (unsafe = true);
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), EStatus::SUCCESS,
            "wiping rows without emitting change records would silently break the feed");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "changefeed");

        UNIT_ASSERT_VALUES_EQUAL(CountOf(session, "/Root/UnsafeTruncateCdc"), 1u);
    }

    // Anomaly (d): being a data-plane operation, it must not bump the schema version the way the
    // plain statement does. The plain form is measured too, so the check cannot pass vacuously.
    Y_UNIT_TEST(SchemaVersionUnchanged) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        const ui64 before = GetSchemaVersion(kikimr, TablePath);

        auto unsafe = session.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(unsafe.GetStatus(), EStatus::SUCCESS, unsafe.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL_C(GetSchemaVersion(kikimr, TablePath), before,
            "unsafe truncate must not touch the schema version");

        auto plain = session.ExecuteQuery(Sprintf("TRUNCATE TABLE `%s`;", TablePath),
            TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(plain.GetStatus(), EStatus::SUCCESS, plain.GetIssues().ToString());

        UNIT_ASSERT_C(GetSchemaVersion(kikimr, TablePath) > before,
            "the plain statement is expected to bump it, otherwise the check above measures nothing");
    }

    // Anomaly (c), the issuing side: T_user keeps its own locks and carries on.
    Y_UNIT_TEST(LocksOfIssuingTransactionSurvive) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto before = session.ExecuteQuery(CountQuery(), TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(before.GetStatus(), EStatus::SUCCESS, before.GetIssues().ToString());
        auto tx = before.GetTransaction();
        UNIT_ASSERT(tx);

        auto trunc = session.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(trunc.GetStatus(), EStatus::SUCCESS, trunc.GetIssues().ToString());

        auto write = session.ExecuteQuery(Sprintf(
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
    Y_UNIT_TEST(UncommittedWritesAreWiped) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto begin = session.ExecuteQuery(Sprintf(
            "UPSERT INTO `%s` (Key, Value) VALUES (100u, \"uncommitted\");", TablePath),
            TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(begin.GetStatus(), EStatus::SUCCESS, begin.GetIssues().ToString());

        auto tx = begin.GetTransaction();
        UNIT_ASSERT(tx);

        auto trunc = session.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(trunc.GetStatus(), EStatus::SUCCESS, trunc.GetIssues().ToString());

        auto after = session.ExecuteQuery(CountQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(after.GetStatus(), EStatus::SUCCESS, after.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(ReadCount(after), 0u,
            "the row written earlier in this very transaction must be gone too");

        auto commit = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL_C(CountRows(session), 0u,
            "and it must not reappear once the transaction commits");
    }

    // The whole shape the feature exists for, in one transaction.
    Y_UNIT_TEST(FullTransactionScenario) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto begin = session.ExecuteQuery(Sprintf(
            "UPSERT INTO `%s` (Key, Value) VALUES (10u, \"before\");", TablePath),
            TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(begin.GetStatus(), EStatus::SUCCESS, begin.GetIssues().ToString());

        auto tx = begin.GetTransaction();
        UNIT_ASSERT(tx);

        auto beforeCount = session.ExecuteQuery(CountQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(beforeCount.GetStatus(), EStatus::SUCCESS, beforeCount.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(ReadCount(beforeCount), 4u,
            "three seeded rows plus the one just written in this transaction");

        auto trunc = session.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(trunc.GetStatus(), EStatus::SUCCESS, trunc.GetIssues().ToString());

        auto afterCount = session.ExecuteQuery(CountQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(afterCount.GetStatus(), EStatus::SUCCESS, afterCount.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReadCount(afterCount), 0u);

        auto write = session.ExecuteQuery(Sprintf(
            "UPSERT INTO `%s` (Key, Value) VALUES (20u, \"after\");", TablePath),
            TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(write.GetStatus(), EStatus::SUCCESS, write.GetIssues().ToString());

        auto commit = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL_C(CountRows(session), 1u,
            "only the row written after the truncate survives");
    }

    // Separate statements of one transaction are the supported shape and are covered above. Putting
    // both into a single query text is a different thing: the truncate is compiled along the scheme
    // path, which builds no data query blocks, so the writes would be dropped on the floor. Pin the
    // refusal rather than leave it to chance.
    Y_UNIT_TEST(MixedWithDataInOneQueryRejected) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto result = session.ExecuteQuery(Sprintf(R"(
            UPSERT INTO `%s` (Key, Value) VALUES (5u, "mixed");
            TRUNCATE TABLE `%s` WITH (unsafe = true);
        )", TablePath, TablePath), TTxControl::NoTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), EStatus::SUCCESS,
            "a single query text may not mix writes with an unsafe truncate");
    }

    // Anomaly (c), the other side: everyone else's locks are broken.
    Y_UNIT_TEST(CompetingTransactionAborted) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session1 = client.GetSession().GetValueSync().GetSession();
        auto session2 = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session1);

        // The read is what actually takes a lock on the shard: with sinks a lone UPSERT sits in the
        // buffer actor until commit, so there would be nothing for the truncate to break.
        auto competingRead = session2.ExecuteQuery(CountQuery(), TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(competingRead.GetStatus(), EStatus::SUCCESS, competingRead.GetIssues().ToString());
        auto competingTx = competingRead.GetTransaction();
        UNIT_ASSERT(competingTx);

        // The write is what makes the commit reach the shard at all, so the broken lock is noticed.
        auto competingWrite = session2.ExecuteQuery(Sprintf(
            "UPSERT INTO `%s` (Key, Value) VALUES (7u, \"competing\");", TablePath),
            TTxControl::Tx(*competingTx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(competingWrite.GetStatus(), EStatus::SUCCESS, competingWrite.GetIssues().ToString());

        auto trunc = session1.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(trunc.GetStatus(), EStatus::SUCCESS, trunc.GetIssues().ToString());

        auto commit = competingTx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::ABORTED, commit.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL_C(CountRows(session1), 0u,
            "the aborted transaction must not have left its row behind");
    }

    // The truncate opens the transaction, so there is no lock to preserve yet and
    // PreserveLockTxIds goes out empty.
    Y_UNIT_TEST(TruncateAsFirstStatement) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto trunc = session.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(trunc.GetStatus(), EStatus::SUCCESS, trunc.GetIssues().ToString());

        auto tx = trunc.GetTransaction();
        if (tx) {
            auto write = session.ExecuteQuery(Sprintf(
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
    Y_UNIT_TEST(TruncateAfterSplit) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFillSharded(session);

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        const auto shardsBefore = GetTableShards(&kikimr.GetTestServer(), runtime.AllocateEdgeActor(), TablePath);
        UNIT_ASSERT_VALUES_EQUAL(shardsBefore.size(), 4u);

        SplitShard(kikimr, TablePath, shardsBefore.at(0), ShardKeys[0] + 1);

        const auto shardsAfter = GetTableShards(&kikimr.GetTestServer(), runtime.AllocateEdgeActor(), TablePath);
        UNIT_ASSERT_VALUES_EQUAL_C(shardsAfter.size(), 5u, "the split must have happened");

        auto result = session.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
    }

    // Truncating an already empty table is a no-op, which is what makes a client retry after
    // UNDETERMINED safe.
    Y_UNIT_TEST(TruncateIsIdempotent) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFillSharded(session);

        for (int i = 0; i < 3; ++i) {
            auto result = session.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::NoTx()).ExtractValueSync();
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
    Y_UNIT_TEST(ReResolvesWhenPrepareIsRefused) {
        // Observers are only honoured while the runtime is single threaded, which in turn means
        // every client call has to go through RunCall so the runtime keeps being pumped.
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false);
        settings.FeatureFlags.SetEnableUnsafeTruncateTable(true);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();
        auto session = kikimr.RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

        auto exec = [&](const TString& sql, bool inTx) {
            return kikimr.RunCall([&] {
                return session.ExecuteQuery(sql,
                    inTx ? TTxControl::BeginTx().CommitTx() : TTxControl::NoTx()).ExtractValueSync();
            });
        };

        {
            auto create = exec(Sprintf(R"(
                CREATE TABLE `%s` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                ) WITH (
                    UNIFORM_PARTITIONS = 4
                );
            )", TablePath), /* inTx */ false);
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
    Y_UNIT_TEST(GivesUpAfterTooManyRefusals) {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false);
        settings.FeatureFlags.SetEnableUnsafeTruncateTable(true);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();
        auto session = kikimr.RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

        auto exec = [&](const TString& sql, bool inTx) {
            return kikimr.RunCall([&] {
                return session.ExecuteQuery(sql,
                    inTx ? TTxControl::BeginTx().CommitTx() : TTxControl::NoTx()).ExtractValueSync();
            });
        };

        {
            auto create = exec(Sprintf(R"(
                CREATE TABLE `%s` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                ) WITH (
                    UNIFORM_PARTITIONS = 4
                );
            )", TablePath), /* inTx */ false);
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

    // Losing the client after the coordinator has planned the transaction is the one case the
    // client cannot be told anything definite: the shards apply the truncate regardless, so the
    // answer is UNDETERMINED and the rows stay gone.
    Y_UNIT_TEST(CancelledAfterPlanIsNotRolledBack) {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false);
        settings.FeatureFlags.SetEnableUnsafeTruncateTable(true);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();
        auto session = kikimr.RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

        auto exec = [&](const TString& sql, bool inTx, const NYdb::NQuery::TExecuteQuerySettings& s = {}) {
            return kikimr.RunCall([&] {
                return session.ExecuteQuery(sql,
                    inTx ? TTxControl::BeginTx().CommitTx() : TTxControl::NoTx(), s).ExtractValueSync();
            });
        };

        {
            auto create = exec(Sprintf(R"(
                CREATE TABLE `%s` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                ) WITH (
                    UNIFORM_PARTITIONS = 4
                );
            )", TablePath), /* inTx */ false);
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

        NYdb::NQuery::TExecuteQuerySettings querySettings;
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
            return observer.ExecuteQuery(CountQuery(), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        });
        UNIT_ASSERT_VALUES_EQUAL_C(count.GetStatus(), EStatus::SUCCESS, count.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(ReadCount(count), 0u,
            "a planned truncate is not rolled back just because the client gave up on it");
    }

    Y_UNIT_TEST(TruncateAfterMerge) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
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

        auto result = session.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
    }

    // Losing the client while the truncate is still preparing must produce an error, not an
    // unhandled event: before TEvAbortExecution was handled the executer asserted and died.
    Y_UNIT_TEST(CancelledWhilePreparing) {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false);
        settings.FeatureFlags.SetEnableUnsafeTruncateTable(true);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();
        auto session = kikimr.RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

        {
            auto create = kikimr.RunCall([&] {
                return session.ExecuteQuery(Sprintf(R"(
                    CREATE TABLE `%s` (
                        Key Uint64,
                        Value String,
                        PRIMARY KEY (Key)
                    ) WITH (
                        UNIFORM_PARTITIONS = 4
                    );
                )", TablePath), TTxControl::NoTx()).ExtractValueSync();
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
        NYdb::NQuery::TExecuteQuerySettings querySettings;
        querySettings.ClientTimeout(TDuration::Seconds(5));

        auto result = kikimr.RunCall([&] {
            return session.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::NoTx(), querySettings)
                .ExtractValueSync();
        });

        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

        UNIT_ASSERT_C(swallowed.load() > 0, "the truncate never reached the prepare phase");
        UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), EStatus::SUCCESS,
            "a truncate whose prepare was never answered cannot report success");
    }

    // Wiping a table is at least as destructive as deleting its rows, so the statement must require
    // the same right a DELETE does. The UPSERT and the plain TRUNCATE under the same user are the
    // negative controls: if either of them were allowed, the environment would be enforcing nothing
    // and this test would measure nothing.
    Y_UNIT_TEST(AclReaderCannotTruncate) {
        const TString user = "user0@builtin";

        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.FeatureFlags.SetEnableUnsafeTruncateTable(true);
        TKikimrRunner kikimr(settings);

        auto admin = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
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

        auto userClient = kikimr.GetQueryClient(NYdb::NQuery::TClientSettings().AuthToken(user));
        auto reader = userClient.GetSession().GetValueSync().GetSession();

        auto select = reader.ExecuteQuery(CountQuery(), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(select.GetStatus(), EStatus::SUCCESS, select.GetIssues().ToString());

        auto upsert = reader.ExecuteQuery(Sprintf(
            "UPSERT INTO `%s` (Key, Value) VALUES (9u, \"x\");", TablePath),
            TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL_C(upsert.GetStatus(), EStatus::SUCCESS,
            "a reader may not write, otherwise this test measures nothing");

        auto plain = reader.ExecuteQuery(Sprintf("TRUNCATE TABLE `%s`;", TablePath),
            TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(plain.GetStatus(), EStatus::UNAUTHORIZED, plain.GetIssues().ToString());

        auto unsafe = reader.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL_C(unsafe.GetStatus(), EStatus::SUCCESS,
            "a user who may not even delete a row must not be able to wipe the table");
        UNIT_ASSERT_STRING_CONTAINS(unsafe.GetIssues().ToString(), "Access denied");

        UNIT_ASSERT_VALUES_EQUAL_C(CountRows(admin), 3u, "nothing may have been wiped");
    }
}

} // namespace NKqp
} // namespace NKikimr
