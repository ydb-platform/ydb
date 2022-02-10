#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpTx) {
    Y_UNIT_TEST_NEW_ENGINE(DeferredEffects) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            UPSERT INTO [/Root/Test]
            SELECT Group, "Sergey" AS Name
            FROM [/Root/Test];
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto tx = result.GetTransaction();

        result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM [/Root/Test] WHERE Group = 1;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[3500u];["None"];[1u];["Anna"]];
            [[300u];["None"];[1u];["Paul"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        auto commitResult = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());

        result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM [/Root/Test] WHERE Group = 1;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[3500u];["None"];[1u];["Anna"]];
            [[300u];["None"];[1u];["Paul"]];
            [#;#;[1u];["Sergey"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST_NEW_ENGINE(ExplicitTcl) { 
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto tx = session.BeginTransaction(TTxSettings::SerializableRW())
            .ExtractValueSync()
            .GetTransaction();
        UNIT_ASSERT(tx.IsActive());

        auto result = session.ExecuteDataQuery(Q_(R"( 
            UPSERT INTO [/Root/KeyValue] (Key, Value) VALUES (10u, "New");
        )"), TTxControl::Tx(tx)).ExtractValueSync(); 
        UNIT_ASSERT(result.IsSuccess());

        result = session.ExecuteDataQuery(Q_(R"( 
            SELECT * FROM [/Root/KeyValue] WHERE Value = "New";
        )"), TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx()).ExtractValueSync(); 
        UNIT_ASSERT(result.IsSuccess());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));

        auto commitResult = tx.Commit().ExtractValueSync();
        UNIT_ASSERT_C(commitResult.IsSuccess(), commitResult.GetIssues().ToString()); 

        result = session.ExecuteDataQuery(Q_(R"( 
            SELECT * FROM [/Root/KeyValue] WHERE Value = "New";
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync(); 
        UNIT_ASSERT(result.IsSuccess());
        CompareYson(R"([[[10u];["New"]]])", FormatResultSetYson(result.GetResultSet(0)));

        commitResult = tx.Commit().ExtractValueSync();
        UNIT_ASSERT(!commitResult.IsSuccess());
        UNIT_ASSERT(HasIssue(commitResult.GetIssues(), NYql::TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND));
    }

    Y_UNIT_TEST_NEW_ENGINE(InteractiveTx) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM [/Root/KeyValue] WHERE Key = 1;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        auto value = parser.ColumnParser("Value").GetOptionalString();

        auto tx = result.GetTransaction();

        auto params = db.GetParamsBuilder()
            .AddParam("$name")
                .String(*value)
                .Build()
            .Build();

        result = session.ExecuteDataQuery(Q_(R"(
            DECLARE $name AS String;
            UPSERT INTO [/Root/Test] (Group, Name, Amount) VALUES
                (10, $name, 500);
        )"), TTxControl::Tx(*tx).CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM [/Root/Test] WHERE Group = 10;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[500u];#;[10u];["One"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(BeginTransactionBadMode) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.BeginTransaction(TTxSettings::OnlineRO()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);

        result = session.BeginTransaction(TTxSettings::StaleRO()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
    }

    Y_UNIT_TEST_NEW_ENGINE(CommitRequired) { 
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"( 
            SELECT * FROM [/Root/KeyValue] WHERE Value = "New";
        )"), TTxControl::BeginTx(TTxSettings::OnlineRO())).ExtractValueSync(); 
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);

        result = session.ExecuteDataQuery(Q_(R"( 
            SELECT * FROM [/Root/KeyValue] WHERE Value = "New";
        )"), TTxControl::BeginTx(TTxSettings::StaleRO())).ExtractValueSync(); 
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
    }

    Y_UNIT_TEST_NEW_ENGINE(RollbackTx) { 
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // with effects, without locks 
        auto result = session.ExecuteDataQuery(Q_(R"( 
            UPSERT INTO [/Root/KeyValue] (Key, Value) VALUES (10u, "New");
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync(); 
        UNIT_ASSERT(result.IsSuccess());

        auto tx = result.GetTransaction();
        UNIT_ASSERT(tx);
        UNIT_ASSERT(tx->IsActive());

        auto rollbackResult = tx->Rollback().ExtractValueSync();
        UNIT_ASSERT(rollbackResult.IsSuccess());

        result = session.ExecuteDataQuery(Q_(R"( 
            SELECT * FROM [/Root/KeyValue] WHERE Value = "New";
        )"), TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx()).ExtractValueSync(); 
        UNIT_ASSERT(result.IsSuccess());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));

        rollbackResult = tx->Rollback().ExtractValueSync();
        UNIT_ASSERT(!rollbackResult.IsSuccess());
        UNIT_ASSERT(HasIssue(rollbackResult.GetIssues(), NYql::TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND));
    }

    Y_UNIT_TEST_NEW_ENGINE(RollbackTx2) { 
        TKikimrRunner kikimr; 
        auto db = kikimr.GetTableClient(); 
        auto session = db.CreateSession().GetValueSync().GetSession(); 
 
        // with effects, with locks 
        auto result = session.ExecuteDataQuery(Q_(R"( 
            UPDATE [/Root/KeyValue] SET Value = "New" WHERE Key = 1;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync(); 
        UNIT_ASSERT(result.IsSuccess()); 
 
        auto tx = result.GetTransaction(); 
        UNIT_ASSERT(tx); 
        UNIT_ASSERT(tx->IsActive()); 
 
        auto rollbackResult = tx->Rollback().ExtractValueSync(); 
        UNIT_ASSERT(rollbackResult.IsSuccess()); 
 
        result = session.ExecuteDataQuery(Q_(R"( 
            SELECT * FROM [/Root/KeyValue] WHERE Value = "New";
        )"), TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx()).ExtractValueSync(); 
        UNIT_ASSERT(result.IsSuccess()); 
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0))); 
 
        rollbackResult = tx->Rollback().ExtractValueSync(); 
        UNIT_ASSERT(!rollbackResult.IsSuccess()); 
        UNIT_ASSERT(HasIssue(rollbackResult.GetIssues(), NYql::TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND)); 
    } 
 
    Y_UNIT_TEST_NEW_ENGINE(RollbackRoTx) { 
        TKikimrRunner kikimr; 
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
 
    Y_UNIT_TEST_NEW_ENGINE(CommitRoTx) { 
        TKikimrRunner kikimr; 
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
 
    Y_UNIT_TEST_NEW_ENGINE(CommitRoTx_TLI) { 
        TKikimrRunner kikimr; 
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
        if (kikimr.IsUsingSnapshotReads()) {
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::ABORTED, commitResult.GetIssues().ToString());
            UNIT_ASSERT(HasIssue(commitResult.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED));
        }
    } 
 
    Y_UNIT_TEST_NEW_ENGINE(EmptyTxOnCommit) { 
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"( 
            UPSERT INTO [/Root/KeyValue] (Key, Value) VALUES (10u, "New");
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

        TKikimrRunner kikimr({setting});
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

        TKikimrRunner kikimr(settings);
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

    Y_UNIT_TEST_NEW_ENGINE(RollbackInvalidated) { 
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"( 
            UPSERT INTO [/Root/KeyValue] (Key, Value) VALUES (10u, "New");
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync(); 
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto tx = result.GetTransaction();

        result = session.ExecuteDataQuery(Q_(R"( 
            SELECT * FROM [BadTable];
        )"), TTxControl::Tx(*tx)).ExtractValueSync(); 
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());

        UNIT_ASSERT(result.GetTransaction());
        UNIT_ASSERT(!result.GetTransaction()->IsActive());

        auto commitResult = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::NOT_FOUND, result.GetIssues().ToString());
        UNIT_ASSERT(HasIssue(commitResult.GetIssues(), NYql::TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND));
    }

    Y_UNIT_TEST_NEW_ENGINE(CommitPrepared) { 
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = session.PrepareDataQuery(Q_(R"( 
            UPSERT INTO [/Root/KeyValue] (Key, Value) VALUES (10u, "New");
        )")).ExtractValueSync().GetQuery(); 

        auto result = query.Execute(TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        auto tx = result.GetTransaction();
        UNIT_ASSERT(!tx->IsActive());

        result = session.ExecuteDataQuery(Q_(R"( 
            SELECT * FROM [/Root/KeyValue] WHERE Value = "New";
        )"), TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx()).ExtractValueSync(); 
        UNIT_ASSERT(result.IsSuccess());
        CompareYson(R"([[[10u];["New"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST_NEW_ENGINE(InvalidateOnError) { 
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto tx = session.BeginTransaction(TTxSettings::SerializableRW())
            .ExtractValueSync()
            .GetTransaction();
        UNIT_ASSERT(tx.IsActive());

        auto result = session.ExecuteDataQuery(Q_(R"( 
            INSERT INTO [/Root/KeyValue] (Key, Value) VALUES (1u, "New");
        )"), TTxControl::Tx(tx)).ExtractValueSync(); 
        // result.GetIssues().PrintTo(Cerr); 
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString()); 

        result = session.ExecuteDataQuery(Q_(R"( 
            UPSERT INTO [/Root/KeyValue] (Key, Value) VALUES (1u, "New");
        )"), TTxControl::Tx(tx)).ExtractValueSync(); 
        // result.GetIssues().PrintTo(Cerr); 
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::NOT_FOUND, result.GetIssues().ToString()); 
    }

    Y_UNIT_TEST_NEW_ENGINE(CommitStats) { 
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto tx = session.BeginTransaction(TTxSettings::SerializableRW())
            .ExtractValueSync()
            .GetTransaction();
        UNIT_ASSERT(tx.IsActive());

        auto result = session.ExecuteDataQuery(Q_(R"( 
            UPSERT INTO [/Root/KeyValue] (Key, Value) VALUES (10u, "New");
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
        TKikimrRunner kikimr; 
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
            PRAGMA Kikimr.UseNewEngine = "true"; 
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
}

} // namespace NKqp 
} // namespace NKikimr
