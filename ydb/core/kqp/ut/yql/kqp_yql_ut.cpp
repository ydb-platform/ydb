#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NScripting;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpYql) {
    Y_UNIT_TEST(RefSelect) {
        auto kikimr = DefaultKikimrRunner();
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

    Y_UNIT_TEST(TableConcat) {
        auto kikimr = DefaultKikimrRunner();
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

    Y_UNIT_TEST(TableRange) {
        auto kikimr = DefaultKikimrRunner();
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

    Y_UNIT_TEST(TableUseBeforeCreate) {
        auto kikimr = DefaultKikimrRunner();
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

    Y_UNIT_TEST(ColumnNameConflict) {
        auto kikimr = DefaultKikimrRunner();
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

    Y_UNIT_TEST(TableNameConflict) {
        auto kikimr = DefaultKikimrRunner();
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

    Y_UNIT_TEST(DdlDmlMix) {
        auto kikimr = DefaultKikimrRunner();
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

    Y_UNIT_TEST(ScriptUdf) {
        auto kikimr = DefaultKikimrRunner();
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

    Y_UNIT_TEST(UpdatePk) {
        auto kikimr = DefaultKikimrRunner();
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

    Y_UNIT_TEST(UpdateBadType) {
        auto kikimr = DefaultKikimrRunner();
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

    Y_UNIT_TEST(InsertCV) {
        auto kikimr = DefaultKikimrRunner();
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

    Y_UNIT_TEST(InsertCVList) {
        auto kikimr = DefaultKikimrRunner();
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

    Y_UNIT_TEST(InsertIgnore) {
        auto kikimr = DefaultKikimrRunner();
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

    Y_UNIT_TEST(NonStrictDml) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            --!syntax_v1
            DELETE FROM `/Root/Test` WHERE Group = 1;
            UPDATE `/Root/Test` SET Comment = "Updated" WHERE Group = 2;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CreateUseTable) {
        auto kikimr = DefaultKikimrRunner();
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

    Y_UNIT_TEST(SelectNoAsciiValue) {
        auto kikimr = DefaultKikimrRunner();
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            --!syntax_v1
            CREATE TABLE ascii_test
            (
                id String,
                PRIMARY KEY (id)
            );
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto insertResult = client.ExecuteYqlScript(R"(
            --!syntax_v1
            INSERT INTO ascii_test (id) VALUES
                ('\xBF');
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), EStatus::SUCCESS, insertResult.GetIssues().ToString());

        auto selectResult = client.ExecuteYqlScript(R"(
            --!syntax_v1
            SELECT * FROM ascii_test WHERE id='\xBF';
        )").GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(selectResult.GetStatus(), EStatus::SUCCESS, selectResult.GetIssues().ToString());
    }

    Y_UNIT_TEST(ColumnTypeMismatch) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = TParamsBuilder()
            .AddParam("$key").Uint64(1).Build()
            .AddParam("$value").Uint64(2).Build()
            .Build();

        TExecDataQuerySettings settings;
        auto req = session.ExecuteDataQuery(Q_(R"(
            DECLARE $key AS Uint64;
            DECLARE $value AS Uint64;

            REPLACE INTO `KeyValue`
                    (Key, Value)
            VALUES
                    ($key, $value);
        )"), TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();

        req.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(req.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(req.GetIssues().ToString(), "Failed to convert 'Value': Uint64 to Optional<String>");
    }

    Y_UNIT_TEST(FlexibleTypes) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto params = TParamsBuilder()
            .AddParam("$text").Utf8("Some text").Build()
            .AddParam("$data").String("Some bytes").Build()
            .Build();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DECLARE $text AS Text;
            DECLARE $data AS Bytes;

            SELECT $text, $data;
        )"), TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([["Some text";"Some bytes"]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(BinaryJsonOffsetBound) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString query = Q1_(R"(SELECT Unpickle(JsonDocument, "\x09\x00\x00\x00\x01\xf2\xb7\xff\x8a\xff\xff\xd2\xff");)");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::INTERNAL_ERROR, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(BinaryJsonOffsetNormal) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString query = Q1_(R"(select Unpickle(JsonDocument, Pickle(JsonDocument('{"a" : 5, "b" : 0.1, "c" : [1, 2, 3]}')));)");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(JsonNumberPrecision) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            SELECT
                JsonDocument("-0.5"),
                JsonDocument("0.5"),
                JsonDocument("-16777216"),
                JsonDocument("16777216"),
                JsonDocument("-9007199254740992"),
                JsonDocument("9007199254740992"),
                JsonDocument("-9223372036854775808"),
                JsonDocument("9223372036854775807"),
                JsonDocument("18446744073709551615");
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        // Cerr << FormatResultSetYson(result.GetResultSet(0)) << Endl;

        CompareYson(R"([[
            "-0.5";
            "0.5";
            "-16777216";
            "16777216";
            "-9007199254740992";
            "9007199254740992";
            "-9.223372036854776e+18";
            "9.223372036854776e+18";
            "1.844674407370955e+19"]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(JsonCast) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            SELECT
                CAST("" as JsonDocument)
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        Cerr << FormatResultSetYson(result.GetResultSet(0)) << Endl;

        CompareYson(R"([[
            #
        ]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(EvaluateExpr1) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            SELECT EvaluateExpr(2+2);
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[
            4]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(EvaluateExpr2) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            SELECT EvaluateExpr(12+20);
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[
            32]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(EvaluateExpr3) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            SELECT EvaluateExpr( EvaluateExpr(2+2) + EvaluateExpr(5*5) );
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[
            29]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(EvaluateExprPgNull) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            SELECT EvaluateExpr( CAST(NULL AS PgInt8) );
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[#]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(EvaluateExprYsonAndType) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            SELECT FormatType(EvaluateType(TypeHandle(TypeOf(1)))), EvaluateExpr("[1;2;3]"y);
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[Int32;"[1;2;3]"]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(EvaluateIf) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            EVALUATE IF true DO BEGIN
                 SELECT 1;
            END DO;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[1]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(EvaluateFor) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            EVALUATE FOR $i in [1] DO BEGIN
                 SELECT $i;
            END DO;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[1]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(FromBytes) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            SELECT
                FromBytes("\xd2\x02\x96\x49\x00\x00\x00\x00", Uint64); -- 1234567890ul
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[[1234567890u]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(Closure) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            $lambda = ($x, $y) -> { RETURN $x + $y };
            $makeClosure = ($y) -> {
                RETURN EvaluateCode(LambdaCode(($x) -> {
                    RETURN FuncCode("Apply", QuoteCode($lambda), $x, ReprCode($y))
                }))
            };

            $closure = $makeClosure(2);
            SELECT $closure(1); -- 3
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[3]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(Discard) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            DISCARD SELECT 1;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_OPERATION));
    }

    Y_UNIT_TEST(AnsiIn) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            $list = AsList(
                Just(1),
                Just(2),
                NULL
            );

            SELECT 1 in $list;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetIssues().Size() == 0, result.GetIssues().ToString());

        CompareYson(R"([[[%true]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(UuidPrimaryKeyDisabled) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetEnableUuidAsPrimaryKey(false)
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE test(
                    key uuid NOT NULL,
                    val int,
                    PRIMARY KEY (key)
                );
            )");
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                CREATE TABLE test(
                    key uuid,
                    val int,
                    PRIMARY KEY (key)
                );
            )");
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                CREATE TABLE test(
                    key int,
                    val uuid,
                    PRIMARY KEY (key),
                    INDEX val_index GLOBAL SYNC ON (val)
                );
            )");
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                CREATE TABLE test(
                    key int,
                    val uuid,
                    PRIMARY KEY (key)
                );
            )");
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                ALTER TABLE test ADD INDEX val_index GLOBAL SYNC ON (val);
            )");
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(UuidPrimaryKey) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TVector<TString> testUuids = {
            "5b99a330-04ef-4f1a-9b64-ba6d5f44eafe",
            "afcbef30-9ac3-481a-aa6a-8d9b785dbb0a",
            "b91cd23b-861c-4cc1-9119-801a4dac1cb9",
            "65df9ecc-a97d-47b2-ae56-3c023da6ee8c",
        };

        {
            const auto query = Q_(R"(
                CREATE TABLE test(
                    key uuid NOT NULL,
                    val int,
                    PRIMARY KEY (key)
                );
            )");
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            int val = 0;
            for (const auto& uuid : testUuids) {
                const auto query = Sprintf("\
                    INSERT INTO test (key, val)\n\
                    VALUES (Uuid(\"%s\"), %u);\n\
                ", uuid.Data(), val++);
                auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }
        }
        {
            const auto query = Q_(R"(
                INSERT INTO test (key, val)
                VALUES (Uuid("invalid-uuid"), 1000);
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }
        {
            const auto query = Q_(R"(
                SELECT * FROM test ORDER BY val ASC;
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            TResultSetParser parser(result.GetResultSetParser(0));
            for (size_t i = 0; parser.TryNextRow(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("key").GetUuid().ToString(), testUuids[i]);
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("val").GetOptionalInt32().GetRef(), i);
            }
        }
        {
            int val = 0;
            for (const auto& uuid : testUuids) {
                const auto query = Sprintf("\
                    SELECT (val) FROM test WHERE key=CAST(\"%s\" as uuid);\n\
                ", uuid.Data());
                auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

                TResultSetParser parser(result.GetResultSetParser(0));
                UNIT_ASSERT(parser.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("val").GetOptionalInt32().GetRef(), val++);
                UNIT_ASSERT_VALUES_EQUAL(parser.RowsCount(), 1);
            }
        }
        {
            const auto query = Q_(R"(
                SELECT * FROM test ORDER BY key;
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            TResultSetParser parser(result.GetResultSetParser(0));
            for (size_t i = 0; parser.TryNextRow(); ++i) {
                UNIT_ASSERT_EQUAL(parser.ColumnParser("key").GetUuid().ToString(), testUuids[i]);
            }
        }
        {
            const auto query = Q_(R"(
                SELECT * FROM test ORDER BY key DESC;
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            TResultSetParser parser(result.GetResultSetParser(0));
            for (size_t i = 0; parser.TryNextRow(); ++i) {
                UNIT_ASSERT_EQUAL(parser.ColumnParser("key").GetUuid().ToString(), testUuids[testUuids.size() - 1 - i]);
            }
        }
        {
            auto params = db.GetParamsBuilder()
                .AddParam("$rows")
                    .BeginList()
                    .AddListItem()
                        .BeginStruct()
                            .AddMember("Key").Uuid(TUuidValue("5b99a330-04ef-4f1a-9b64-ba6d5f44eafe"))
                            .AddMember("Value").OptionalInt32(0)
                        .EndStruct()
                    .AddListItem()
                        .BeginStruct()
                            .AddMember("Key").Uuid(TUuidValue("afcbef30-9ac3-481a-aa6a-8d9b785dbb0a"))
                            .AddMember("Value").OptionalInt32(1)
                        .EndStruct()
                    .AddListItem()
                        .BeginStruct()
                            .AddMember("Key").Uuid(TUuidValue("b91cd23b-861c-4cc1-9119-801a4dac1cb9"))
                            .AddMember("Value").OptionalInt32(2)
                        .EndStruct()
                    .AddListItem()
                        .BeginStruct()
                            .AddMember("Key").Uuid(TUuidValue("65df9ecc-a97d-47b2-ae56-3c023da6ee8c"))
                            .AddMember("Value").OptionalInt32(3)
                        .EndStruct()
                    .EndList()
                    .Build()
                .Build();

            const auto query = Q_(R"(
                DECLARE $rows AS
                    List<Struct<
                        Key: Uuid,
                        Value: Int32?>>;

                SELECT * FROM AS_TABLE($rows) ORDER BY Key;
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            TResultSetParser parser(result.GetResultSetParser(0));
            for (size_t i = 0; parser.TryNextRow(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Key").GetUuid().ToString(), testUuids[i]);
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Value").GetOptionalInt32().GetRef(), i);
            }
        }
    }

    Y_UNIT_TEST(TestUuidPrimaryKeyPrefixSearch) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TVector<TString> testUuids = {
            "5b99a330-04ef-4f1a-9b64-ba6d5f44eafe",
            "afcbef30-9ac3-481a-aa6a-8d9b785dbb0a",
            "b91cd23b-861c-4cc1-9119-801a4dac1cb9",
            "65df9ecc-a97d-47b2-ae56-3c023da6ee8c",
        };

        {
            const auto query = Q_(R"(
                CREATE TABLE test(
                    key uuid NOT NULL,
                    val int,
                    PRIMARY KEY (key)
                );
            )");
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            int val = 0;
            for (const auto& uuid : testUuids) {
                const auto query = Sprintf("\
                    INSERT INTO test (key, val)\n\
                    VALUES (Uuid(\"%s\"), %u);\n\
                ", uuid.Data(), val++);
                auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }
        }
        {
            int val = 0;
            for (const auto& uuid : testUuids) {
                const auto query = Sprintf("SELECT * FROM test WHERE key=Uuid(\"%s\");", uuid.Data());
                auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

                TResultSetParser parser(result.GetResultSetParser(0));
                UNIT_ASSERT(parser.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("val").GetOptionalInt32().GetRef(), val++);
                UNIT_ASSERT_VALUES_EQUAL(parser.RowsCount(), 1);
            }
        }
    }

    Y_UNIT_TEST(TestUuidDefaultColumn) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            const auto query = Q_(R"(
                CREATE TABLE test(
                    key int NOT NULL,
                    val uuid NOT NULL DEFAULT Uuid("65df9ecc-a97d-47b2-ae56-3c023da6ee8c"),
                    PRIMARY KEY (key)
                );
            )");
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            const auto query = "INSERT INTO test (key) VALUES (0);";
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(UuidPrimaryKeyBulkUpsert) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        auto kikimr = TKikimrRunner{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TVector<TString> testUuids = {
            "5b99a330-04ef-4f1a-9b64-ba6d5f44eafe",
            "afcbef30-9ac3-481a-aa6a-8d9b785dbb0a",
            "b91cd23b-861c-4cc1-9119-801a4dac1cb9",
            "65df9ecc-a97d-47b2-ae56-3c023da6ee8c",
        };

        {
            const auto query = Q_(R"(
                CREATE TABLE test(
                    key uuid NOT NULL,
                    val int,
                    PRIMARY KEY (key)
                );
            )");
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (size_t i = 0; i < testUuids.size(); ++i) {
                rows.AddListItem()
                    .BeginStruct()
                        .AddMember("key").Uuid(TUuidValue(testUuids[i]))
                        .AddMember("val").Int32(i)
                    .EndStruct();
            }
            rows.EndList();

            auto upsertResult = db.BulkUpsert("/Root/test", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        }
        {
            const auto query = Q_(R"(
                SELECT * FROM test ORDER BY val ASC;
            )");
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            TResultSetParser parser(result.GetResultSetParser(0));
            for (size_t i = 0; parser.TryNextRow(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("key").GetUuid().ToString(), testUuids[i]);
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("val").GetOptionalInt32().GetRef(), i);
            }
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
