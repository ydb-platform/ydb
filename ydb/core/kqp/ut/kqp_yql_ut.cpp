#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NScripting;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpYql) {
    Y_UNIT_TEST_TWIN(RefSelect, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            --!syntax_v1
            PRAGMA RefSelect;
            SELECT * FROM `/Root/Test`;
        )").GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);

        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::CORE_OPTIMIZATION));
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR));
    }

    Y_UNIT_TEST_TWIN(TableConcat, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            --!syntax_v1
            SELECT * FROM CONCAT(`/Root/Test`, `/Root/Test`)
            WHERE Group = 1;
        )").GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);

        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::CORE_INTENT));
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR));
    }

    Y_UNIT_TEST_TWIN(TableRange, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            --!syntax_v1
            SELECT * FROM RANGE(`Root`, `/Root/Test`, `/Root/Test`)
            WHERE Group = 1;
        )").GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);

        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::CORE_INTENT));
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR));
    }

    Y_UNIT_TEST_TWIN(TableUseBeforeCreate, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            --!syntax_v1
            SELECT * FROM `/Root/NewTable`;

            COMMIT;

            CREATE TABLE `/Root/NewTable` (
                Id Uint32,
                Value String,
                PRIMARY KEY(Id)
            );
        )").GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);

        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::CORE_TYPE_ANN));
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR));
    }

    Y_UNIT_TEST_TWIN(ColumnNameConflict, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            --!syntax_v1
            CREATE TABLE `/Root/ConflictColumn` (
                Id Uint32,
                Value int32,
                Value int32,
                PRIMARY KEY (Id)
            );
        )").GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);

        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::CORE_TYPE_ANN));
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR));
    }

    Y_UNIT_TEST_TWIN(TableNameConflict, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            --!syntax_v1
            SELECT * FROM `/Root/Test`;

            COMMIT;

            DROP TABLE `/Root/Test`;

            CREATE TABLE `/Root/Test` (
                Id Uint32,
                Value String,
                PRIMARY KEY (Id)
            );

            COMMIT;

            SELECT * FROM `/Root/Test`;
        )").GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);

        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::CORE_TYPE_ANN));
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR));
    }

    Y_UNIT_TEST_TWIN(DdlDmlMix, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            --!syntax_v1
            SELECT * FROM `/Root/Test`;
            DROP TABLE `/Root/Test`;
        )").GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);

        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_MIXED_SCHEME_DATA_TX));
    }

    Y_UNIT_TEST_TWIN(ScriptUdf, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            --!syntax_v1
            $extractor = @@
            def inc_value(value):
                return value + 1
            @@;

            $udf = Python::inc_value(Callable<(Uint64)->Uint64>, $extractor);

            SELECT $udf(10) AS Result;
        )").GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);

        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::CORE_TYPE_ANN));
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR));
    }

    Y_UNIT_TEST_TWIN(UpdatePk, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            --!syntax_v1
            UPDATE `/Root/Test`
            SET Group = Group + 1
            WHERE Name != "Paul";
        )").GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);

        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::CORE_TYPE_ANN));
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR));
    }

    Y_UNIT_TEST_TWIN(UpdateBadType, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            --!syntax_v1
            UPDATE `/Root/Test`
            SET Amount = Name;
        )").GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);

        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::CORE_TYPE_ANN));
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR));
    }

    Y_UNIT_TEST_TWIN(InsertCV, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            --!syntax_v1
            INSERT INTO `/Root/Test` (Group, Name, Amount) VALUES
                (1u, "Anna", 10000);
        )").GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION));
    }

    Y_UNIT_TEST_TWIN(InsertCVList, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            --!syntax_v1
            INSERT INTO `/Root/Test` (Group, Name, Amount) VALUES
                (100u, "NewName1", 10),
                (110u, "NewName2", 20),
                (100u, "NewName1", 30);
        )").GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);

        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION));
    }

    Y_UNIT_TEST_TWIN(InsertIgnore, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            --!syntax_v1
            INSERT OR IGNORE INTO `/Root/Test` (Group, Name, Amount) VALUES
                (1u, "Anna", 10000),
                (100u, "NewName1", 10);
        )").GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);

        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::CORE_INTENT));
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR));
    }

    Y_UNIT_TEST_NEW_ENGINE(StrictDml) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            --!syntax_v1
            DELETE FROM `/Root/Test` WHERE Group = 1;
            UPDATE `/Root/Test` SET Comment = "Updated" WHERE Group = 2;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);

        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_READ_MODIFIED_TABLE));
    }

    Y_UNIT_TEST_TWIN(UnwrapReadTableValues, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            --!syntax_v1
            PRAGMA kikimr.UnwrapReadTableValues = "true";
            SELECT LENGTH(Name) == 4 AND Amount > 1000 FROM `/Root/Test`;
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [%true];[%false];[%true]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST_TWIN(CreateUseTable, UseSessionActor) {
        auto kikimr = KikimrRunnerEnableSessionActor(UseSessionActor);
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            --!syntax_v1
            CREATE TABLE `/Root/NewTable` (
                Id Uint32,
                Value String,
                PRIMARY KEY(Id)
            );
            COMMIT;

            REPLACE INTO `/Root/NewTable` (Id, Value) VALUES
                (1, "One"),
                (2, "Two");
            COMMIT;

            SELECT * FROM `/Root/NewTable`;
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["One"]];
            [[2u];["Two"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }
}

} // namespace NKqp
} // namespace NKikimr
