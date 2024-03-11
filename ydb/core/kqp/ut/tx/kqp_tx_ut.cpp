#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpTx) {
    Y_UNIT_TEST(DeferredEffects) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/Test`
            SELECT Group, "Sergey" AS Name
            FROM `/Root/Test`;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto tx = result.GetTransaction();

        result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/Test` WHERE Group = 1;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[3500u];["None"];[1u];["Anna"]];
            [[300u];["None"];[1u];["Paul"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        auto commitResult = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());

        result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/Test` WHERE Group = 1;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[3500u];["None"];[1u];["Anna"]];
            [[300u];["None"];[1u];["Paul"]];
            [#;#;[1u];["Sergey"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(ExplicitTcl) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto tx = session.BeginTransaction(TTxSettings::SerializableRW())
            .ExtractValueSync()
            .GetTransaction();
        UNIT_ASSERT(tx.IsActive());

        auto result = session.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (10u, "New");
        )"), TTxControl::Tx(tx)).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/KeyValue` WHERE Value = "New";
        )"), TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));

        auto commitResult = tx.Commit().ExtractValueSync();
        UNIT_ASSERT_C(commitResult.IsSuccess(), commitResult.GetIssues().ToString());

        result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/KeyValue` WHERE Value = "New";
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        CompareYson(R"([[[10u];["New"]]])", FormatResultSetYson(result.GetResultSet(0)));

        commitResult = tx.Commit().ExtractValueSync();
        UNIT_ASSERT(!commitResult.IsSuccess());
        UNIT_ASSERT(HasIssue(commitResult.GetIssues(), NYql::TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND));
    }

    Y_UNIT_TEST(LocksAbortOnCommit) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            auto result = session.ExecuteDataQuery(Q_(R"(
                UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (1, "One");
                UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (2, "Two");
                UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (3, "Three");
                UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (4, "Four");
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }


        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/KeyValue`;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto tx = result.GetTransaction();

        result = session.ExecuteDataQuery(Q_(R"(
            UPDATE `/Root/KeyValue` SET Value = "second" WHERE Key = 3;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(Q_(R"(
            UPDATE `/Root/KeyValue` SET Value = "third" WHERE Key = 4;
        )"), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());

        auto commitResult = tx->Commit().ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::NOT_FOUND, commitResult.GetIssues().ToString());
    }

    Y_UNIT_TEST(InteractiveTx) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key = 1;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        auto value = parser.ColumnParser("Value").GetOptionalString();

        auto tx = result.GetTransaction();
        UNIT_ASSERT(tx->IsActive());

        auto params = db.GetParamsBuilder()
            .AddParam("$name")
                .String(*value)
                .Build()
            .Build();

        result = session.ExecuteDataQuery(Q_(R"(
            DECLARE $name AS String;
            UPSERT INTO `/Root/Test` (Group, Name, Amount) VALUES
                (10, $name, 500);
        )"), TTxControl::Tx(*tx).CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(!result.GetTransaction()->IsActive());

        result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/Test` WHERE Group = 10;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(!result.GetTransaction()->IsActive());
        CompareYson(R"([[[500u];#;[10u];["One"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(BeginTransactionBadMode) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.BeginTransaction(TTxSettings::OnlineRO()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);

        result = session.BeginTransaction(TTxSettings::StaleRO()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
    }

    Y_UNIT_TEST(CommitRequired) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/KeyValue` WHERE Value = "New";
        )"), TTxControl::BeginTx(TTxSettings::OnlineRO())).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);

        result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/KeyValue` WHERE Value = "New";
        )"), TTxControl::BeginTx(TTxSettings::StaleRO())).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
    }

    Y_UNIT_TEST(RollbackTx) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // with effects, without locks
        auto result = session.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (10u, "New");
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        auto tx = result.GetTransaction();
        UNIT_ASSERT(tx);
        UNIT_ASSERT(tx->IsActive());

        auto rollbackResult = tx->Rollback().ExtractValueSync();
        UNIT_ASSERT(rollbackResult.IsSuccess());

        result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/KeyValue` WHERE Value = "New";
        )"), TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));

        rollbackResult = tx->Rollback().ExtractValueSync();
        UNIT_ASSERT(!rollbackResult.IsSuccess());
        UNIT_ASSERT(HasIssue(rollbackResult.GetIssues(), NYql::TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND));
    }

    Y_UNIT_TEST(RollbackTx2) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // with effects, with locks
        auto result = session.ExecuteDataQuery(Q_(R"(
            UPDATE `/Root/KeyValue` SET Value = "New" WHERE Key = 1;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        auto tx = result.GetTransaction();
        UNIT_ASSERT(tx);
        UNIT_ASSERT(tx->IsActive());

        auto rollbackResult = tx->Rollback().ExtractValueSync();
        UNIT_ASSERT(rollbackResult.IsSuccess());

        result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/KeyValue` WHERE Value = "New";
        )"), TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));

        rollbackResult = tx->Rollback().ExtractValueSync();
        UNIT_ASSERT(!rollbackResult.IsSuccess());
        UNIT_ASSERT(HasIssue(rollbackResult.GetIssues(), NYql::TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND));
    }

    Y_UNIT_TEST(RollbackManyTx) {
        auto setting = NKikimrKqp::TKqpSetting();
        setting.SetName("_KqpMaxActiveTxPerSession");
        setting.SetValue("10");

        auto kikimr = DefaultKikimrRunner({setting});
        auto db = kikimr.GetTableClient();

        auto query = R"(
            --!syntax_v1

            DECLARE $key AS Uint64;

            UPDATE `/Root/TwoShard` SET Value1 = "Updated" WHERE Key = $key;
        )";

        auto session = db.CreateSession().GetValueSync().GetSession();
        auto beginTx = [&](ui32 idx) {
            auto params = kikimr.GetTableClient().GetParamsBuilder()
                .AddParam("$key").Uint64(302 + idx).Build()
                .Build();

            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

            auto result = session.ExecuteDataQuery(query,
                TTxControl::BeginTx(TTxSettings::SerializableRW()), params, execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            return result;
        };

        // Explicit rollback
        for (ui32 i = 0; i < 10; ++i) {
            auto result = beginTx(i);

            auto tx = result.GetTransaction();
            UNIT_ASSERT(tx);
            UNIT_ASSERT(tx->IsActive());

            auto rollbackResult = tx->Rollback().ExtractValueSync();
            UNIT_ASSERT(rollbackResult.IsSuccess());
        }
        session.Close();

        // Implicit rollback
        session = db.CreateSession().GetValueSync().GetSession();
        for (ui32 i = 0; i < 10; ++i) {
            beginTx(i);
        }
        session.Close();
    }

    Y_UNIT_TEST(RollbackRoTx) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key = 1
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        auto tx = result.GetTransaction();
        UNIT_ASSERT(tx);
        UNIT_ASSERT(tx->IsActive());

        auto rollbackResult = tx->Rollback().ExtractValueSync();
        UNIT_ASSERT(rollbackResult.IsSuccess());

        rollbackResult = tx->Rollback().ExtractValueSync();
        UNIT_ASSERT(!rollbackResult.IsSuccess());
        UNIT_ASSERT(HasIssue(rollbackResult.GetIssues(), NYql::TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND));
    }

    Y_UNIT_TEST(CommitRoTx) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key = 1
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        auto tx = result.GetTransaction();
        UNIT_ASSERT(tx);
        UNIT_ASSERT(tx->IsActive());

        auto commitResult = tx->Commit().ExtractValueSync();
        UNIT_ASSERT(commitResult.IsSuccess());
    }

    Y_UNIT_TEST(CommitRoTx_TLI) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key = 1
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        auto tx = result.GetTransaction();
        UNIT_ASSERT(tx);
        UNIT_ASSERT(tx->IsActive());

        {
            result = session.ExecuteDataQuery(Q_(R"(
                UPDATE `/Root/KeyValue` SET Value = "New" WHERE Key = 1;
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        auto commitResult = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
    }

    Y_UNIT_TEST(EmptyTxOnCommit) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (10u, "New");
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        auto tx = result.GetTransaction();
        UNIT_ASSERT(tx);
        UNIT_ASSERT(!tx->IsActive());
    }

    Y_UNIT_TEST(TooManyTx) {
        auto setting = NKikimrKqp::TKqpSetting();
        setting.SetName("_KqpMaxActiveTxPerSession");
        setting.SetValue("2");

        auto kikimr = DefaultKikimrRunner({setting});
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.BeginTransaction(TTxSettings::SerializableRW()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        result = session.BeginTransaction(TTxSettings::SerializableRW()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        auto tx = result.GetTransaction();

        result = session.BeginTransaction(TTxSettings::SerializableRW()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_SESSION);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_TOO_MANY_TRANSACTIONS));

        auto rollbackResult = tx.Rollback().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(rollbackResult.GetStatus(), EStatus::BAD_SESSION);
    }

    Y_UNIT_TEST(RollbackByIdle) {
        TVector<NKikimrKqp::TKqpSetting> settings;
        auto setting = NKikimrKqp::TKqpSetting();
        setting.SetName("_KqpMaxActiveTxPerSession");
        setting.SetValue("2");
        settings.push_back(setting);
        setting.SetName("_KqpTxIdleTimeoutSec");
        setting.SetValue("0");
        settings.push_back(setting);

        auto kikimr = DefaultKikimrRunner(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.BeginTransaction(TTxSettings::SerializableRW()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        auto tx1 = result.GetTransaction();

        result = session.BeginTransaction(TTxSettings::SerializableRW()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        auto tx2 = result.GetTransaction();

        result = session.BeginTransaction(TTxSettings::SerializableRW()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        auto commitResult = tx1.Commit().ExtractValueSync();
        UNIT_ASSERT(!commitResult.IsSuccess());
        UNIT_ASSERT(HasIssue(commitResult.GetIssues(), NYql::TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND));

        commitResult = tx2.Commit().ExtractValueSync();
        UNIT_ASSERT(commitResult.IsSuccess());
    }

    Y_UNIT_TEST(RollbackInvalidated) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (10u, "New");
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto tx = result.GetTransaction();

        result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `BadTable`;
        )"), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());

        UNIT_ASSERT(result.GetTransaction());
        UNIT_ASSERT(!result.GetTransaction()->IsActive());

        auto commitResult = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::NOT_FOUND, result.GetIssues().ToString());
        UNIT_ASSERT(HasIssue(commitResult.GetIssues(), NYql::TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND));
    }

    Y_UNIT_TEST(CommitPrepared) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = session.PrepareDataQuery(Q_(R"(
            UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (10u, "New");
        )")).ExtractValueSync().GetQuery();

        auto result = query.Execute(TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        auto tx = result.GetTransaction();
        UNIT_ASSERT(!tx->IsActive());

        result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/KeyValue` WHERE Value = "New";
        )"), TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        CompareYson(R"([[[10u];["New"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(InvalidateOnError) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto tx = session.BeginTransaction(TTxSettings::SerializableRW())
            .ExtractValueSync()
            .GetTransaction();
        UNIT_ASSERT(tx.IsActive());

        auto result = session.ExecuteDataQuery(Q_(R"(
            INSERT INTO `/Root/KeyValue` (Key, Value) VALUES (1u, "New");
        )"), TTxControl::Tx(tx)).ExtractValueSync();
        // result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (1u, "New");
        )"), TTxControl::Tx(tx)).ExtractValueSync();
        // result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::NOT_FOUND, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CommitStats) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto tx = session.BeginTransaction(TTxSettings::SerializableRW())
            .ExtractValueSync()
            .GetTransaction();
        UNIT_ASSERT(tx.IsActive());

        auto result = session.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (10u, "New");
        )"), TTxControl::Tx(tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        NYdb::NTable::TCommitTxSettings commitSettings;
        commitSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto commitResult = tx.Commit(commitSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());

        UNIT_ASSERT(commitResult.GetStats());
        auto stats = NYdb::TProtoAccessor::GetProto(*commitResult.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).name(), "/Root/KeyValue");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().rows(), 1);
    }

    Y_UNIT_TEST(MixEnginesOldNew) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto tx = session.BeginTransaction(TTxSettings::SerializableRW())
            .ExtractValueSync()
            .GetTransaction();
        UNIT_ASSERT(tx.IsActive());

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/KeyValue`
        )", TTxControl::Tx(tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (1u, "New");
        )", TTxControl::Tx(tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/KeyValue`
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[1u];["New"]];
            [[2u];["Two"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(SnapshotRO) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // Read Immediate
        auto result = session.ExecuteDataQuery(Q1_(R"(
            SELECT * FROM EightShard WHERE Key = 102;
        )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[3];[102u];["Value2"]]])", FormatResultSetYson(result.GetResultSet(0)));

        // Read Distributed
        result = session.ExecuteDataQuery(Q1_(R"(
            SELECT COUNT(*) FROM EightShard WHERE Text = "Value1";
        )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[8u]])", FormatResultSetYson(result.GetResultSet(0)));

        // Write
        result = session.ExecuteDataQuery(Q1_(R"(
            UPSERT INTO `/Root/EightShard` (Key, Data) VALUES
                (100, 100500),
                (100500, 100);
        )"), TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_OPERATION));
    }

    Y_UNIT_TEST(SnapshotROInteractive1) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto readQuery = Q1_(R"(
            SELECT * FROM EightShard WHERE Key = 102;
        )");

        auto readResult = R"([
            [[3];[102u];["Value2"]]
        ])";

        auto result = session.ExecuteDataQuery(readQuery,
            TTxControl::BeginTx(TTxSettings::SnapshotRO())).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(readResult, FormatResultSetYson(result.GetResultSet(0)));

        auto tx = result.GetTransaction();
        UNIT_ASSERT(tx);
        UNIT_ASSERT(tx->IsActive());

        result = session.ExecuteDataQuery(Q1_(R"(
            UPSERT INTO `/Root/EightShard` (Key, Data) VALUES
                (102, 100500);
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(readQuery,
            TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(readResult, FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(SnapshotROInteractive2) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto readQuery = Q1_(R"(
            SELECT COUNT(*) FROM EightShard WHERE Text = "Value1";
        )");

        auto readResult = R"([
            [8u]
        ])";

        auto tx = session.BeginTransaction(TTxSettings::SnapshotRO())
            .ExtractValueSync()
            .GetTransaction();
        UNIT_ASSERT(tx.IsActive());

        auto result = session.ExecuteDataQuery(readQuery,
            TTxControl::Tx(tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(readResult, FormatResultSetYson(result.GetResultSet(0)));

        result = session.ExecuteDataQuery(Q1_(R"(
            UPSERT INTO `/Root/EightShard` (Key, Data, Text) VALUES
                (100500u, -1, "Value1");
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(readQuery,
            TTxControl::Tx(tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(readResult, FormatResultSetYson(result.GetResultSet(0)));

        auto commitResult = tx.Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
    }
}

} // namespace NKqp
} // namespace NKikimr
