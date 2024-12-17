#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

TKikimrRunner GetKikimrRunnerWithStats() {
    static TString STATS = R"(
            {
                "/Root/FJ_Table_1":
                {
                    "n_rows": 4
                },
                "/Root/FJ_Table_2":
                {
                    "n_rows": 2
                },
                "/Root/FJ_Table_3":
                {
                    "n_rows": 4
                },
                 "/Root/FJ_Table_4":
                {
                    "n_rows": 3
                }
            }
        )";

    TVector<NKikimrKqp::TKqpSetting> settings;

    NKikimrKqp::TKqpSetting setting;
    setting.SetName("OptOverrideStatistics");
    setting.SetValue(STATS);
    settings.push_back(setting);

    TKikimrSettings serverSettings;
    serverSettings.SetKqpSettings(settings);

    return TKikimrRunner(serverSettings);
}

static void CreateSampleTables(TSession session) {
    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/FJ_Table_1` (
                Key Int32, Fk2 Int32, Fk3 Int32, Value String,
                PRIMARY KEY (Key)
            );
            CREATE TABLE `/Root/FJ_Table_2` (
                Key Int32, Fk3 Int32, Fk1 Int32, Value String,
                PRIMARY KEY (Key)
            );
            CREATE TABLE `/Root/FJ_Table_3` (
                Key Int32, Fk1 Int32, Fk2 Int32, Value String,
                PRIMARY KEY (Key)
            );
            CREATE TABLE `/Root/FJ_Table_4` (
                Key Int32, Value String,
                PRIMARY KEY (Key)
            );
        )").GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteDataQuery(R"(
            REPLACE INTO `/Root/FJ_Table_1` (Key, Fk2, Fk3, Value) VALUES
                (1, 101, 1001, "Value11"),
                (2, 102, 1002, "Value12"),
                (3, 103, 1003, "Value13"),
                (4, 104, 1004, "Value14");
            REPLACE INTO `/Root/FJ_Table_2` (Key, Fk3, Fk1, Value) VALUES
                (101, 1001, 1, "Value21"),
                (102, 1002, 2, "Value22");
            REPLACE INTO `/Root/FJ_Table_3` (Key, Fk1, Fk2, Value) VALUES
                (1001, 1, 101, "Value31"),
                (1002, 2, 102, "Value32"),
                (1003, 3, 103, "Value33"),
                (1005, 5, 105, "Value35");
            REPLACE INTO `/Root/FJ_Table_4` (Key, Value) VALUES
                (1,    "Value4_1"),
                (101,  "Value4_101"),
                (1001, "Value4_1001");
        )", TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());
}

static TParams NoParams = TParamsBuilder().Build();

Y_UNIT_TEST_SUITE(KqpFlipJoin) {

    // simple inner join, only 2 tables
    Y_UNIT_TEST(Inner_1) {
        auto kikimr = GetKikimrRunnerWithStats();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        // join on key-column of left table and non-key column of right one
        const TString query = R"(
            SELECT t1.Value, t2.Value
            FROM `/Root/FJ_Table_1` AS t1
                INNER JOIN `/Root/FJ_Table_2` AS t2 ON t1.Key = t2.Fk1
            ORDER BY t1.Value, t2.Value
        )";

        auto result = ExecQueryAndTestResult(session, query, NoParams,
            R"([[["Value11"];["Value21"]];[["Value12"];["Value22"]]])");

        Cerr << result.GetQueryPlan() << Endl;

        AssertTableReads(result, "/Root/FJ_Table_1", 2);
        AssertTableReads(result, "/Root/FJ_Table_2", 2);
    }

    // hierarchy of joins, flip on the last layer
    Y_UNIT_TEST(Inner_2) {
        auto kikimr = GetKikimrRunnerWithStats();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        const TString query = R"(
            SELECT t1.Value, t2.Value, t3.Value
            FROM `/Root/FJ_Table_3` AS t1
                INNER JOIN `/Root/FJ_Table_2` AS t2 ON t1.Key = t2.Fk3
                INNER JOIN `/Root/FJ_Table_4` AS t3 ON t2.Key = t3.Key
            ORDER BY t1.Value, t2.Value, t3.Value
        )";

        auto result = ExecQueryAndTestResult(session, Q_(query), NoParams,
            R"([[["Value31"];["Value21"];["Value4_101"]]])");

        AssertTableReads(result, "/Root/FJ_Table_2", 2);
        AssertTableReads(result, "/Root/FJ_Table_3", 1);
        AssertTableReads(result, "/Root/FJ_Table_4", 1);
    }

    // hierarchy of joins, flip on the top layer
    Y_UNIT_TEST(Inner_3) {
        auto kikimr = GetKikimrRunnerWithStats();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        const TString query = Q_(R"(
            $join = (
                SELECT t1.Value AS Value1, t2.Value AS Value2, t1.Fk3 AS Fk
                FROM `/Root/FJ_Table_1` AS t1
                    INNER JOIN `/Root/FJ_Table_2` AS t2 ON t1.Fk2 = t2.Key
            );
            SELECT t.Value1, t.Value2, t3.Value
            FROM `/Root/FJ_Table_3` AS t3
                INNER JOIN $join AS t ON t3.Key = t.Fk
            ORDER BY t.Value1, t.Value2, t3.Value
        )");

        auto result = ExecQueryAndTestResult(session, query, NoParams,
            R"([[["Value11"];["Value21"];["Value31"]];[["Value12"];["Value22"];["Value32"]]])");

        AssertTableReads(result, "/Root/FJ_Table_1", 4);
        AssertTableReads(result, "/Root/FJ_Table_2", 2);
        AssertTableReads(result, "/Root/FJ_Table_3", 2);
    }

    // simple left semi join, only 2 tables
    Y_UNIT_TEST(LeftSemi_1) {
        auto kikimr = GetKikimrRunnerWithStats();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        const TString query = Q_(
            R"(
                SELECT t1.Value
                FROM `/Root/FJ_Table_1` AS t1
                    LEFT SEMI JOIN `/Root/FJ_Table_2` AS t2 ON t1.Key = t2.Fk1
                ORDER BY t1.Value
            )");

        auto result = ExecQueryAndTestResult(session, query, NoParams, R"([[["Value11"]];[["Value12"]]])");
        
        AssertTableReads(result, "/Root/FJ_Table_1", 2);
        AssertTableReads(result, "/Root/FJ_Table_2", 2);
    }

    // hierarchy of joins, flip on the last layer
    Y_UNIT_TEST(LeftSemi_2) {
        auto kikimr = GetKikimrRunnerWithStats();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        const TString query = Q_(
            R"(
                SELECT t1.Key, t1.Value
                FROM `/Root/FJ_Table_1` AS t1
                    LEFT SEMI JOIN `/Root/FJ_Table_2` AS t2 ON t1.Key = t2.Fk1
                    LEFT SEMI JOIN `/Root/FJ_Table_3` AS t3 ON t1.Key = t3.Fk1
                ORDER BY t1.Key, t1.Value
            )");

        auto result = ExecQueryAndTestResult(session, query, NoParams, R"([[[1];["Value11"]];[[2];["Value12"]]])");
        
        AssertTableReads(result, "/Root/FJ_Table_1", 3);
        AssertTableReads(result, "/Root/FJ_Table_2", 2);
        AssertTableReads(result, "/Root/FJ_Table_3", 4);
    }

    // hierarchy of joins, flip on the top layer
    Y_UNIT_TEST(LeftSemi_3) {
        auto kikimr = GetKikimrRunnerWithStats();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        const TString query = Q_(
            R"(
                $join = (
                    SELECT t1.Value AS Value1, t2.Value AS Value2, t1.Fk3 AS Fk
                    FROM `/Root/FJ_Table_1` AS t1
                        INNER JOIN `/Root/FJ_Table_2` AS t2 ON t1.Fk2 = t2.Key
                );
                SELECT t3.Value
                    FROM `/Root/FJ_Table_3` AS t3
                    LEFT SEMI JOIN $join AS t ON t3.Key = t.Fk
                ORDER BY t3.Value
            )");

        auto result = ExecQueryAndTestResult(session, query, NoParams, R"([[["Value31"]];[["Value32"]]])");

        AssertTableReads(result, "/Root/FJ_Table_1", 4);
        AssertTableReads(result, "/Root/FJ_Table_2", 2);
        AssertTableReads(result, "/Root/FJ_Table_3", 2);
    }

    // simple right semi join, only 2 tables
    Y_UNIT_TEST(RightSemi_1) {
        auto kikimr = GetKikimrRunnerWithStats();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        const TString query = Q_(R"(
            SELECT t2.Value
            FROM `/Root/FJ_Table_1` AS t1
                RIGHT SEMI JOIN `/Root/FJ_Table_2` AS t2 ON t1.Key = t2.Fk1
            ORDER BY t2.Value
        )");

        auto result = ExecQueryAndTestResult(session, query, NoParams, R"([[["Value21"]];[["Value22"]]])");

        AssertTableReads(result, "/Root/FJ_Table_1", 4);
        AssertTableReads(result, "/Root/FJ_Table_2", 2);
    }

    // hierarchy of joins, flip on the last layer
    Y_UNIT_TEST(RightSemi_2) {
        auto kikimr = GetKikimrRunnerWithStats();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        const TString query = Q_(R"(
            SELECT t3.Key, t3.Value
            FROM `/Root/FJ_Table_1` AS t1
                RIGHT SEMI JOIN `/Root/FJ_Table_2` AS t2 ON t1.Key = t2.Fk1
                RIGHT SEMI JOIN `/Root/FJ_Table_3` AS t3 ON t2.Key = t3.Fk2
            ORDER BY t3.Key, t3.Value
        )");

        auto result = ExecQueryAndTestResult(session, query, NoParams,
            R"([[[1001];["Value31"]];[[1002];["Value32"]]])");

        AssertTableReads(result, "/Root/FJ_Table_1", 4);
        AssertTableReads(result, "/Root/FJ_Table_2", 2);
        AssertTableReads(result, "/Root/FJ_Table_3", 4);
    }

    // hierarchy of joins, flip on the top layer
    Y_UNIT_TEST(RightSemi_3) {
        auto kikimr = GetKikimrRunnerWithStats();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        const TString query = Q_(R"(
            $join = (
                SELECT t1.Value AS Value1, t2.Value AS Value2, t1.Fk3 AS Fk3
                FROM `/Root/FJ_Table_1` AS t1
                    INNER JOIN `/Root/FJ_Table_2` AS t2 ON t1.Fk2 = t2.Key
            );
            SELECT t.Value1, t.Value2
            FROM `/Root/FJ_Table_3` AS t3
                RIGHT SEMI JOIN $join AS t ON t3.Key = t.Fk3
            ORDER BY t.Value1, t.Value2
        )");

        auto result = ExecQueryAndTestResult(session, query, NoParams,
            R"([[["Value11"];["Value21"]];[["Value12"];["Value22"]]])");

        AssertTableReads(result, "/Root/FJ_Table_1", 4);
        AssertTableReads(result, "/Root/FJ_Table_2", 2);
        AssertTableReads(result, "/Root/FJ_Table_3", 4);
    }

    // simple right join, only 2 tables
    Y_UNIT_TEST(Right_1) {
        auto kikimr = GetKikimrRunnerWithStats();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        const TString query = R"(
            SELECT t2.Value
            FROM `/Root/FJ_Table_1` AS t1
                RIGHT JOIN `/Root/FJ_Table_2` AS t2 ON t1.Key = t2.Fk1
            ORDER BY t2.Value
        )";

        auto result = ExecQueryAndTestResult(session, query, NoParams, R"([[["Value21"]];[["Value22"]]])");
            
        AssertTableReads(result, "/Root/FJ_Table_1", 4);
        AssertTableReads(result, "/Root/FJ_Table_2", 2);
    }

    // hierarchy of joins, flip on the last layer
    Y_UNIT_TEST(Right_2) {
        auto kikimr = GetKikimrRunnerWithStats();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        const TString query = R"(
            SELECT t3.Key, t3.Value
            FROM `/Root/FJ_Table_1` AS t1
                RIGHT JOIN `/Root/FJ_Table_2` AS t2 ON t1.Key = t2.Fk1
                RIGHT JOIN `/Root/FJ_Table_3` AS t3 ON t2.Key = t3.Fk2
            ORDER BY t3.Key, t3.Value
        )";

        auto result = ExecQueryAndTestResult(session, query, NoParams,
            R"([[[1001];["Value31"]];[[1002];["Value32"]];[[1003];["Value33"]];[[1005];["Value35"]]])");

        AssertTableReads(result, "/Root/FJ_Table_1", 4);
        AssertTableReads(result, "/Root/FJ_Table_2", 2);
        AssertTableReads(result, "/Root/FJ_Table_3", 4);
    }

    // hierarchy of joins, flip on the top layer
    Y_UNIT_TEST(Right_3) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        const TString query = R"(
            $join = (
                SELECT t1.Value AS Value1, t2.Value AS Value2, t1.Fk3 AS Fk3
                FROM `/Root/FJ_Table_1` AS t1
                    INNER JOIN `/Root/FJ_Table_2` AS t2 ON t1.Fk2 = t2.Key
            );
            SELECT t.Value1, t.Value2
            FROM `/Root/FJ_Table_3` AS t3
                RIGHT JOIN $join AS t ON t3.Key = t.Fk3
            ORDER BY t.Value1, t.Value2
        )";

        auto result = ExecQueryAndTestResult(session, query, NoParams,
            R"([[["Value11"];["Value21"]];[["Value12"];["Value22"]]])");

        AssertTableReads(result, "/Root/FJ_Table_1", 4);
        AssertTableReads(result, "/Root/FJ_Table_2", 2);
        AssertTableReads(result, "/Root/FJ_Table_3", 4);
    }

    // simple right only join, only 2 tables
    Y_UNIT_TEST(RightOnly_1) {
        auto kikimr = GetKikimrRunnerWithStats();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        const TString query = R"(
            SELECT t2.Value
            FROM `/Root/FJ_Table_3` AS t1
                RIGHT ONLY JOIN `/Root/FJ_Table_2` AS t2 ON t1.Key = t2.Fk3
            ORDER BY t2.Value
        )";

        auto result = ExecQueryAndTestResult(session, query, NoParams, R"([])");

        AssertTableReads(result, "/Root/FJ_Table_2", 2);
        AssertTableReads(result, "/Root/FJ_Table_3", 4);
    }

    // hierarchy of joins, flip on the last layer
    Y_UNIT_TEST(RightOnly_2) {
        auto kikimr = GetKikimrRunnerWithStats();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        const TString query = R"(
            $join = (
                SELECT t2.Key AS Key, t2.Fk1 AS Fk1, t2.Fk2 AS Fk2, t2.Value AS Value
                FROM `/Root/FJ_Table_1` AS t1
                    RIGHT ONLY JOIN `/Root/FJ_Table_3` AS t2 ON t1.Key = t2.Fk1
            );
            SELECT t3.Key, t3.Value
            FROM $join AS t
                INNER JOIN `/Root/FJ_Table_2` AS t3 ON t.Fk2 = t3.Key
            ORDER BY t3.Key, t3.Value
        )";

        auto result = ExecQueryAndTestResult(session, query, NoParams, R"([])");

        AssertTableReads(result, "/Root/FJ_Table_1", 4);
        AssertTableReads(result, "/Root/FJ_Table_2", 0);
        AssertTableReads(result, "/Root/FJ_Table_3", 4);
    }

    // hierarchy of joins, flip on the top layer
    Y_UNIT_TEST(RightOnly_3) {
        auto kikimr = GetKikimrRunnerWithStats();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        const TString query = R"(
            $join = (
                SELECT t1.Value AS Value1, t2.Value AS Value2, t1.Fk3 AS Fk3
                FROM `/Root/FJ_Table_1` AS t1
                    INNER JOIN `/Root/FJ_Table_2` AS t2 ON t1.Fk2 = t2.Key
            );
            SELECT t.Value1, t.Value2
            FROM `/Root/FJ_Table_4` AS t3
                RIGHT ONLY JOIN $join AS t ON t3.Key = t.Fk3
            ORDER BY t.Value1, t.Value2
        )";

        auto result = ExecQueryAndTestResult(session, query, NoParams,
            R"([[["Value12"];["Value22"]]])");

        AssertTableReads(result, "/Root/FJ_Table_1", 4);
        AssertTableReads(result, "/Root/FJ_Table_2", 2);
        AssertTableReads(result, "/Root/FJ_Table_4", 3);
    }

} // Y_UNIT_TEST_SUITE

} // NKikimr::NKqp
