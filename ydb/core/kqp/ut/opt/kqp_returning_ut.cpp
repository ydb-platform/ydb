#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/core/kqp/runtime/kqp_read_actor.h>
#include <ydb/core/tx/datashard/datashard_impl.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

inline NKikimrConfig::TAppConfig GetAppConfig(bool enableIndexStreamWrite) {
    auto app = NKikimrConfig::TAppConfig();
    app.MutableTableServiceConfig()->SetEnableIndexStreamWrite(enableIndexStreamWrite);
    return app;
}

Y_UNIT_TEST_SUITE(KqpReturning) {

Y_UNIT_TEST_TWIN(ReturningTwice, EnableIndexStreamWrite) {
    TKikimrRunner kikimr(TKikimrSettings(GetAppConfig(EnableIndexStreamWrite)));

    auto client = kikimr.GetTableClient();
    auto session = client.CreateSession().GetValueSync().GetSession();

    const auto queryCreate = Q_(R"(
        CREATE TABLE IF NOT EXISTS tasks (
            hashed_key          Uint32,
            queue_name          String,
            task_id             String,
            worker_id           Int32,
            running             Bool,
            eta                 Timestamp,
            lock_timeout        Timestamp,
            num_fails           Int32,
            num_reschedules     Int32,
            body                String,
            first_fail          Timestamp,
            idempotency_run_id  String,
            PRIMARY KEY (hashed_key, queue_name, task_id)
        );

        CREATE TABLE IF NOT EXISTS tasks_eta_002 (
            eta                 Timestamp,
            hashed_key          Uint32,
            queue_name          String,
            task_id             String,
            PRIMARY KEY (eta, hashed_key, queue_name, task_id)
        ) WITH (
            AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1,
            AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 1
        );

        CREATE TABLE IF NOT EXISTS tasks_processing_002 (
            expiration_ts       Timestamp,
            hashed_key          Uint32,
            queue_name          String,
            task_id             String,
            PRIMARY KEY (expiration_ts, hashed_key, queue_name, task_id)
        ) WITH (
            AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1,
            AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 1
        );
        )");

    auto resultCreate = session.ExecuteSchemeQuery(queryCreate).GetValueSync();
    UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());

    {
        const auto query = Q_(R"(
            --!syntax_v1
            DECLARE $eta AS Timestamp;
            DECLARE $expiration_ts AS Timestamp;
            DECLARE $limit AS Int32;

            $to_move = (
                SELECT $expiration_ts AS expiration_ts, eta, hashed_key, queue_name, task_id
                FROM tasks_eta_002
                WHERE eta <= $eta
                ORDER BY eta, hashed_key, queue_name, task_id
                LIMIT $limit
            );

            UPSERT INTO tasks_processing_002 (expiration_ts, hashed_key, queue_name, task_id)
            SELECT expiration_ts, hashed_key, queue_name, task_id FROM $to_move
            RETURNING expiration_ts, hashed_key, queue_name, task_id;

            UPSERT INTO tasks (hashed_key, queue_name, task_id, running, lock_timeout)
            SELECT hashed_key, queue_name, task_id, True as running, $expiration_ts AS lock_timeout FROM $to_move;

            DELETE FROM tasks_eta_002 ON
            SELECT eta, hashed_key, queue_name, task_id FROM $to_move;
        )");

        auto params = TParamsBuilder()
            .AddParam("$eta").Timestamp(TInstant::Zero()).Build()
            .AddParam("$expiration_ts").Timestamp(TInstant::Zero()).Build()
            .AddParam("$limit").Int32(1).Build()
            .Build();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Full);

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params, execSettings).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());

        size_t eta_table_access = 0;
        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        for (auto phase : stats.query_phases()) {
            for (auto table : phase.table_access()) {
                if (table.name() == "/Root/tasks_eta_002") {
                    eta_table_access++;
                }
            }
        }
        Cerr << "access count " << eta_table_access << Endl;
        UNIT_ASSERT_EQUAL(eta_table_access, 1);
        //Cerr << stats.Utf8DebugString() << Endl;
    }
}

Y_UNIT_TEST_TWIN(ReplaceSerial, EnableIndexStreamWrite) {
    TKikimrRunner kikimr(TKikimrSettings(GetAppConfig(EnableIndexStreamWrite)));

    auto client = kikimr.GetTableClient();
    auto session = client.CreateSession().GetValueSync().GetSession();

    const auto queryCreate = Q_(R"(
        --!syntax_v1
        CREATE TABLE ReturningTable (
        key Serial,
        value Int32,
        PRIMARY KEY (key));
        )");

    auto resultCreate = session.ExecuteSchemeQuery(queryCreate).GetValueSync();
    UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());

    {
        const auto query = Q_(R"(
            --!syntax_v1
            REPLACE INTO ReturningTable (key, value) VALUES (2, 10) RETURNING key, value;
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());

        CompareYson(R"([[2;[10]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        const auto query = Q_(R"(
            --!syntax_v1
            REPLACE INTO ReturningTable (value) VALUES(1), (2), (3) RETURNING key, value;
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());

        CompareYson(R"([[1;[1]];[2;[2]];[3;[3]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        const auto query = Q_(R"(
            --!syntax_v1
            select key, value from ReturningTable order by key asc;
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());

        CompareYson(R"([[1;[1]];[2;[2]];[3;[3]]])", FormatResultSetYson(result.GetResultSet(0)));
    }
}

Y_UNIT_TEST_TWIN(ReturningSerial, EnableIndexStreamWrite) {
    auto serverSettings = TKikimrSettings(GetAppConfig(EnableIndexStreamWrite)).SetWithSampleTables(false);
    TKikimrRunner kikimr(serverSettings);

    auto client = kikimr.GetTableClient();
    auto session = client.CreateSession().GetValueSync().GetSession();

    const auto queryCreate = Q_(R"(
        --!syntax_v1
        CREATE TABLE ReturningTable (
        key Serial,
        value Int32,
        PRIMARY KEY (key));

        CREATE TABLE ReturningTableExtraValue (
        key Serial,
        value Int32,
        value2 Int32 default 2,
        PRIMARY KEY (key)
        );
        )");

    auto resultCreate = session.ExecuteSchemeQuery(queryCreate).GetValueSync();
    UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());

   {
        const auto query = Q_(R"(
            --!syntax_v1
            INSERT INTO ReturningTable (key) VALUES(20000) RETURNING *
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());

        CompareYson(R"([[20000;#]])", FormatResultSetYson(result.GetResultSet(0)));
    }


   {
        const auto query = Q_(R"(
            --!syntax_v1
            UPSERT INTO ReturningTable (key) VALUES(20000) RETURNING *
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([[20000;#]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        const auto query = Q_(R"(
            --!syntax_v1
            UPSERT INTO ReturningTable (key, value) VALUES(20000, 100) RETURNING *
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([[20000;[100]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        const auto query = Q_(R"(
            --!syntax_v1
            UPSERT INTO ReturningTable (key) VALUES(20000) RETURNING *
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([[20000;[100]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        const auto query = Q_(R"(
            --!syntax_v1
            INSERT INTO ReturningTable (value) VALUES(2) RETURNING key;
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([[1]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        const auto query = Q_(R"(
            --!syntax_v1
            INSERT INTO ReturningTable (value) VALUES(2) RETURNING key, value;
            INSERT INTO ReturningTableExtraValue (value) VALUES(3) RETURNING key, value;
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([[2;[2]]])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[1;[3]]])", FormatResultSetYson(result.GetResultSet(1)));
    }

    {
        const auto query = Q_(R"(
            --!syntax_v1
            INSERT INTO ReturningTable (value) VALUES(2) RETURNING *;
            INSERT INTO ReturningTableExtraValue (value) VALUES(4) RETURNING *;
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([[3;[2]]])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[2;[4];[2]]])", FormatResultSetYson(result.GetResultSet(1)));
    }

    {
        const auto query = Q_(R"(
            --!syntax_v1
            INSERT INTO ReturningTable (value) VALUES(2) RETURNING fake;
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        Cerr << result.GetIssues().ToString(true) << Endl;
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(true), "Column not found: fake");
    }

    {
        const auto query = Q_(R"(
            --!syntax_v1
            UPDATE ReturningTable SET  value = 3 where key = 1 RETURNING *;
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([[1;[3]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        const auto query = Q_(R"(
            --!syntax_v1
            UPDATE ReturningTableExtraValue SET  value2 = 3 where key = 2 RETURNING *;
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([[2;[4];[3]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        const auto query = Q_(R"(
            --!syntax_v1
            DELETE FROM ReturningTableExtraValue WHERE key = 2 RETURNING key, value, value2;
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([[2;[4];[3]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        const auto query = Q_(R"(
            --!syntax_v1
            UPSERT INTO ReturningTableExtraValue (value, value2) VALUES (4, 5) RETURNING key;
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([[3]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        const auto query = Q_(R"(
            --!syntax_v1
            DELETE FROM ReturningTable WHERE key <= 3 RETURNING key, value
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(
            EnableIndexStreamWrite ? R"([[1;[3]];[2;[2]];[3;[2]]])" : R"([[2;[2]];[3;[2]];[1;[3]]])",
            FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        const auto query = Q_(R"(
            --!syntax_v1
            DELETE FROM ReturningTable10 WHERE key <= 3 RETURNING *;
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(!result.IsSuccess());
    }
}

TString ExecuteReturningQuery(TKikimrRunner& kikimr, bool queryService, TString query) {
    if (queryService) {
        auto qdb = kikimr.GetQueryClient();
        auto qSession = qdb.GetSession().GetValueSync().GetSession();
        auto result = qSession.ExecuteQuery(
            query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        return FormatResultSetYson(result.GetResultSet(0));
    }

    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return FormatResultSetYson(result.GetResultSet(0));
}

TString ExecuteReturningQueryWithParams(TKikimrRunner& kikimr, bool queryService, TString query, const TParams& params) {
    if (queryService) {
        auto qdb = kikimr.GetQueryClient();
        auto qSession = qdb.GetSession().GetValueSync().GetSession();
        auto settings = NYdb::NQuery::TExecuteQuerySettings()
            .Syntax(NYdb::NQuery::ESyntax::YqlV1);
        auto result = qSession.ExecuteQuery(
            query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), params, settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        return FormatResultSetYson(result.GetResultSet(0));
    }

    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return FormatResultSetYson(result.GetResultSet(0));
}

Y_UNIT_TEST_QUAD(ReturningWorks, QueryService, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    CreateSampleTablesWithIndex(session, true);
    CompareYson(
        R"([[[101];[101];["Payload1"]];])",
        ExecuteReturningQuery(kikimr, QueryService, R"(
            UPSERT INTO `/Root/SecondaryKeys`  (Key, Fk, Value) VALUES (101,    101,    "Payload1") RETURNING *;
        )")
    );
    CompareYson(
        R"(
            [[#;#;["Payload8"]];
            [[1];[1];["Payload1"]];
            [[2];[2];["Payload2"]];
            [[5];[5];["Payload5"]];
            [#;[7];["Payload7"]];
            [[101];[101];["Payload1"]]
        ])",
        ExecuteReturningQuery(kikimr, QueryService, "SELECT * FROM `/Root/SecondaryKeys` ORDER BY Key, Fk;")
    );
}

Y_UNIT_TEST_QUAD(ReturningWorksIndexedUpsert, QueryService, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    CreateSampleTablesWithIndex(session, true);
    CompareYson(R"([
        [[110];[110];["Payload5"]];
    ])", ExecuteReturningQuery(kikimr, QueryService, R"(
        $v1 = (SELECT Key + 100 as Key, Fk + 100 as Fk, Value FROM `/Root/SecondaryKeys` WHERE Key IS NOT NULL AND Fk IS NOT NULL);
        $v2 = (SELECT Key + 105 as Key, Fk + 105 as Fk, Value FROM `/Root/SecondaryKeys` WHERE Key IS NOT NULL AND Fk IS NOT NULL);
        UPSERT INTO `/Root/SecondaryKeys`
        SELECT * FROM (SELECT * FROM $v1 UNION ALL SELECT * FROM $v2) WHERE Key > 107 RETURNING *;
    )"));
    CompareYson(
        R"(
            [[#;#;["Payload8"]];
            [[1];[1];["Payload1"]];
            [[2];[2];["Payload2"]];
            [[5];[5];["Payload5"]];
            [#;[7];["Payload7"]];
            [[110];[110];["Payload5"]]
        ])",
        ExecuteReturningQuery(kikimr, QueryService, "SELECT * FROM `/Root/SecondaryKeys` ORDER BY Key, Fk;")
    );
}

Y_UNIT_TEST_QUAD(ReturningWorksIndexedDelete, QueryService, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    CreateSampleTablesWithIndex(session, true);
    CompareYson(R"([
        [[5];[5];["Payload5"]];
    ])", ExecuteReturningQuery(kikimr, QueryService, R"(
        $v1 = (SELECT Key, Fk, Value FROM `/Root/SecondaryKeys` WHERE Key IS NOT NULL AND Fk IS NOT NULL AND Key >= 1);
        $v2 = (SELECT Key, Fk, Value FROM `/Root/SecondaryKeys` WHERE Key IS NOT NULL AND Fk IS NOT NULL AND Key <= 5);
        DELETE FROM `/Root/SecondaryKeys` ON
        SELECT * FROM (SELECT * FROM $v1 UNION ALL SELECT * FROM $v2) WHERE Key >= 5 RETURNING *;
    )"));
    CompareYson(
        R"(
            [[#;#;["Payload8"]];
            [[1];[1];["Payload1"]];
            [[2];[2];["Payload2"]];
            [#;[7];["Payload7"]];
        ])",
        ExecuteReturningQuery(kikimr, QueryService, "SELECT * FROM `/Root/SecondaryKeys` ORDER BY Key, Fk;")
    );
}

Y_UNIT_TEST_QUAD(ReturningWorksIndexedDeleteV2, QueryService, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    CreateSampleTablesWithIndex(session, true);
    CompareYson(R"([
        [[1];[1];["Payload1"]];
    ])", ExecuteReturningQuery(kikimr, QueryService, R"(
        DELETE FROM `/Root/SecondaryKeys` WHERE Key = 1 RETURNING *;
    )"));
    CompareYson(
        R"(
            [[#;#;["Payload8"]];
            [[2];[2];["Payload2"]];
            [[5];[5];["Payload5"]];
            [#;[7];["Payload7"]];
        ])",
        ExecuteReturningQuery(kikimr, QueryService, "SELECT * FROM `/Root/SecondaryKeys` ORDER BY Key, Fk;")
    );
}


Y_UNIT_TEST_QUAD(ReturningWorksIndexedInsert, QueryService, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    CreateSampleTablesWithIndex(session, true);

    CompareYson(R"([
        [[101];[101];["Payload1"]];
    ])", ExecuteReturningQuery(kikimr, QueryService, R"(
        $v1 = (SELECT Key + 100 as Key, Fk + 100 as Fk, Value FROM `/Root/SecondaryKeys` WHERE Key IS NOT NULL AND Fk IS NOT NULL);
        $v2 = (SELECT Key + 205 as Key, Fk + 205 as Fk, Value FROM `/Root/SecondaryKeys` WHERE Key IS NOT NULL AND Fk IS NOT NULL);
        INSERT INTO `/Root/SecondaryKeys`
        SELECT * FROM (SELECT * FROM $v1 UNION ALL SELECT * FROM $v2 ) WHERE Key < 102 RETURNING *;
    )"));

    CompareYson(
        R"(
            [[#;#;["Payload8"]];
            [[1];[1];["Payload1"]];
            [[2];[2];["Payload2"]];
            [[5];[5];["Payload5"]];
            [#;[7];["Payload7"]];
            [[101];[101];["Payload1"]]
        ])",
        ExecuteReturningQuery(kikimr, QueryService, "SELECT * FROM `/Root/SecondaryKeys` ORDER BY Key, Fk;")
    );
}

Y_UNIT_TEST_QUAD(ReturningWorksIndexedReplace, QueryService, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    CreateSampleTablesWithIndex(session, true);

    CompareYson(R"([
        [[101];[101];["Payload1"]];
    ])", ExecuteReturningQuery(kikimr, QueryService, R"(
        $v1 = (SELECT Key + 100 as Key, Fk + 100 as Fk, Value FROM `/Root/SecondaryKeys` WHERE Key IS NOT NULL AND Fk IS NOT NULL);
        $v2 = (SELECT Key + 205 as Key, Fk + 205 as Fk, Value FROM `/Root/SecondaryKeys` WHERE Key IS NOT NULL AND Fk IS NOT NULL);
        REPLACE INTO `/Root/SecondaryKeys`
        SELECT * FROM (SELECT * FROM $v1 UNION ALL SELECT * FROM $v2 ) WHERE Key < 102 RETURNING *;
    )"));

    CompareYson(
        R"(
            [[#;#;["Payload8"]];
            [[1];[1];["Payload1"]];
            [[2];[2];["Payload2"]];
            [[5];[5];["Payload5"]];
            [#;[7];["Payload7"]];
            [[101];[101];["Payload1"]]
        ])",
        ExecuteReturningQuery(kikimr, QueryService, "SELECT * FROM `/Root/SecondaryKeys` ORDER BY Key, Fk;")
    );
}

Y_UNIT_TEST_QUAD(ReturningWorksIndexedOperationsWithDefault, QueryService, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    {
        auto res = session.ExecuteSchemeQuery(R"(
            --!syntax_v1
            CREATE TABLE `/Root/SecondaryKeys` (
                Key Serial,
                Fk Int32,
                Value String,
                PRIMARY KEY (Key),
                INDEX Index GLOBAL ON (Fk)
            );
        )").GetValueSync();
    }

    CompareYson(R"([
        [1;[1];["Payload"]];
    ])", ExecuteReturningQuery(kikimr, QueryService, R"(
        REPLACE INTO `/Root/SecondaryKeys` (Fk, Value) VALUES (1, "Payload") RETURNING Key, Fk, Value;
    )"));

    CompareYson(
        R"([
            [1;[1];["Payload"]];
        ])",
        ExecuteReturningQuery(kikimr, QueryService, "SELECT Key, Fk, Value FROM `/Root/SecondaryKeys` ORDER BY Key, Fk;")
    );
}

Y_UNIT_TEST_TWIN(ReturningColumnsOrder, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));

    auto client = kikimr.GetTableClient();
    auto session = client.CreateSession().GetValueSync().GetSession();
    auto db = kikimr.GetQueryClient();

    const auto queryCreate = Q_(R"(
        CREATE TABLE test1 (id Int32, v Text, PRIMARY KEY(id));
        )");

    auto resultCreate = session.ExecuteSchemeQuery(queryCreate).GetValueSync();
    UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());

    {
        const auto query = Q_(R"(
            UPSERT INTO test1 (id, v) VALUES (1, '321') RETURNING id, v;
            REPLACE INTO test1 (id, v) VALUES (1, '111') RETURNING v, id;
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());
        CompareYson(R"([[[1];["321"]]])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[["111"];[1]]])", FormatResultSetYson(result.GetResultSet(1)));
    }

    auto settings = NYdb::NQuery::TExecuteQuerySettings()
        .Syntax(NYdb::NQuery::ESyntax::YqlV1)
        .ConcurrentResultSets(false);
    {
        auto result = db.ExecuteQuery(R"(
            UPSERT INTO test1 (id, v) VALUES (1, '321') RETURNING id, v;
            REPLACE INTO test1 (id, v) VALUES (1, '111') RETURNING v, id;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[1];["321"]]])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[["111"];[1]]])", FormatResultSetYson(result.GetResultSet(1)));
    }
    {
        auto it = db.StreamExecuteQuery(R"(
            UPSERT INTO test1 (id, v) VALUES (2, '321') RETURNING id, v;
            REPLACE INTO test1 (id, v) VALUES (2, '111') RETURNING v, id;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        Cerr << StreamResultToYson(it);
    }

}

Y_UNIT_TEST_TWIN(Random, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));

    auto client = kikimr.GetQueryClient();
    auto settings = NYdb::NQuery::TExecuteQuerySettings()
        .Syntax(NYdb::NQuery::ESyntax::YqlV1)
        .ConcurrentResultSets(false);

    {
        auto result = client.ExecuteQuery("CREATE TABLE example (key Uint64, value String, PRIMARY KEY (key));",
            NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
    }

    {
        auto result = client.ExecuteQuery(
            R"(
            UPSERT INTO example (key, value) VALUES (1, CAST(RandomUuid(1) AS String)) RETURNING *;
            SELECT * FROM example;
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
        CompareYson(FormatResultSetYson(result.GetResultSet(0)), FormatResultSetYson(result.GetResultSet(1)));
    }
}

Y_UNIT_TEST_QUAD(ReturningTypes, QueryService, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));

    auto client = kikimr.GetTableClient();
    auto session = client.CreateSession().GetValueSync().GetSession();

    {
        const auto query = Q_(R"(
            --!syntax_v1
            DELETE FROM KeyValue WHERE Key >= 2u RETURNING Key, Value;
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());
        CompareYson(R"([[[2u];["Two"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        const auto query = Q_(R"(
            --!syntax_v1
            DELETE FROM KeyValue WHERE Key = 1u RETURNING Key, Value;
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());
        CompareYson(R"([[[1u];["One"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }
}

Y_UNIT_TEST_QUAD(ReturningUpsertAsTableListNotNullOnly, QueryService, EnableIndexStreamWrite) {
    // Test for issue #27021: Query fails when using RETURNING CLAUSE with UPSERT
    // to table with only NOT NULL fields and query parameters of type List
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));

    auto client = kikimr.GetTableClient();
    auto session = client.CreateSession().GetValueSync().GetSession();

    // Create table with only NOT NULL fields
    const auto queryCreate = Q_(R"(
        --!syntax_v1
        CREATE TABLE test_table (
            id Uint64 NOT NULL,
            value Utf8 NOT NULL,
            PRIMARY KEY (id)
        );
    )");

    auto resultCreate = session.ExecuteSchemeQuery(queryCreate).GetValueSync();
    UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());

    {
        // Test case from issue #27021
        const auto query = Q_(R"(
            --!syntax_v1
            DECLARE $data AS List<Struct<id: UInt64, value: Utf8>>;

            UPSERT INTO test_table
            SELECT * FROM AS_TABLE($data)
            RETURNING *;
        )");

        auto paramsBuilder = TParamsBuilder();
        auto& dataParam = paramsBuilder.AddParam("$data");

        dataParam.BeginList();
        dataParam.AddListItem()
            .BeginStruct()
            .AddMember("id")
                .Uint64(1)
            .AddMember("value")
                .Utf8("test1")
            .EndStruct();
        dataParam.AddListItem()
            .BeginStruct()
            .AddMember("id")
                .Uint64(2)
            .AddMember("value")
                .Utf8("test2")
            .EndStruct();
        dataParam.EndList();
        dataParam.Build();

        auto params = paramsBuilder.Build();

        // This should succeed, but currently fails with infinite loop error
        CompareYson(R"([[1u;"test1"];[2u;"test2"]])",
            ExecuteReturningQueryWithParams(kikimr, QueryService, query, params));
    }

    {
        // Test with explicit field names in SELECT clause
        const auto query = Q_(R"(
            --!syntax_v1
            DECLARE $data AS List<Struct<id: UInt64, value: Utf8>>;

            UPSERT INTO test_table
            SELECT id, value FROM AS_TABLE($data)
            RETURNING *;
        )");

        auto paramsBuilder = TParamsBuilder();
        auto& dataParam = paramsBuilder.AddParam("$data");

        dataParam.BeginList();
        dataParam.AddListItem()
            .BeginStruct()
            .AddMember("id")
                .Uint64(3)
            .AddMember("value")
                .Utf8("test3")
            .EndStruct();
        dataParam.EndList();
        dataParam.Build();

        auto params = paramsBuilder.Build();

        CompareYson(R"([[3u;"test3"]])",
            ExecuteReturningQueryWithParams(kikimr, QueryService, query, params));
    }

    {
        // Test DELETE with RETURNING using AS_TABLE with List parameter (same issue #27021)
        // First insert some data without RETURNING
        const auto insertQuery = Q_(R"(
            --!syntax_v1
            DECLARE $data AS List<Struct<id: UInt64, value: Utf8>>;

            UPSERT INTO test_table
            SELECT * FROM AS_TABLE($data);
        )");

        auto insertParamsBuilder = TParamsBuilder();
        auto& insertDataParam = insertParamsBuilder.AddParam("$data");

        insertDataParam.BeginList();
        insertDataParam.AddListItem()
            .BeginStruct()
            .AddMember("id")
                .Uint64(10)
            .AddMember("value")
                .Utf8("delete1")
            .EndStruct();
        insertDataParam.EndList();
        insertDataParam.Build();

        auto insertParams = insertParamsBuilder.Build();

        // Execute insert without RETURNING
        if (QueryService) {
            auto qdb = kikimr.GetQueryClient();
            auto qSession = qdb.GetSession().GetValueSync().GetSession();
            auto settings = NYdb::NQuery::TExecuteQuerySettings()
                .Syntax(NYdb::NQuery::ESyntax::YqlV1);
            auto insertResult = qSession.ExecuteQuery(
                insertQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), insertParams, settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), EStatus::SUCCESS, insertResult.GetIssues().ToString());
        } else {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto insertResult = session.ExecuteDataQuery(insertQuery, TTxControl::BeginTx().CommitTx(), insertParams).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), EStatus::SUCCESS, insertResult.GetIssues().ToString());
        }

        // Now DELETE with RETURNING using AS_TABLE
        const auto deleteQuery = Q_(R"(
            --!syntax_v1
            DECLARE $data AS List<Struct<id: UInt64>>;

            DELETE FROM test_table ON
            SELECT * FROM AS_TABLE($data)
            RETURNING *;
        )");

        auto deleteParamsBuilder = TParamsBuilder();
        auto& deleteDataParam = deleteParamsBuilder.AddParam("$data");

        deleteDataParam.BeginList();
        deleteDataParam.AddListItem()
            .BeginStruct()
            .AddMember("id")
                .Uint64(10)
            .EndStruct();
        deleteDataParam.EndList();
        deleteDataParam.Build();

        auto deleteParams = deleteParamsBuilder.Build();

        // This should succeed, but currently fails with infinite loop error
        CompareYson(R"([[10u;"delete1"]])",
            ExecuteReturningQueryWithParams(kikimr, QueryService, deleteQuery, deleteParams));
    }
}

Y_UNIT_TEST_QUAD(ReturningUpsertAsTableListWithNullable, QueryService, EnableIndexStreamWrite) {
    // Test that nullable columns work correctly (this should work even before the fix)
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));

    auto client = kikimr.GetTableClient();
    auto session = client.CreateSession().GetValueSync().GetSession();

    const auto queryCreate = Q_(R"(
        --!syntax_v1
        CREATE TABLE test_table_nullable (
            id Uint64 NOT NULL,
            value Utf8,
            PRIMARY KEY (id)
        );
    )");

    auto resultCreate = session.ExecuteSchemeQuery(queryCreate).GetValueSync();
    UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());

    {
        const auto query = Q_(R"(
            --!syntax_v1
            DECLARE $data AS List<Struct<id: UInt64, value: Utf8?>>;

            UPSERT INTO test_table_nullable
            SELECT * FROM AS_TABLE($data)
            RETURNING *;
        )");

        auto paramsBuilder = TParamsBuilder();
        auto& dataParam = paramsBuilder.AddParam("$data");

        dataParam.BeginList();
        dataParam.AddListItem()
            .BeginStruct()
            .AddMember("id")
                .Uint64(1)
            .AddMember("value")
                .OptionalUtf8("test1")
            .EndStruct();
        dataParam.EndList();
        dataParam.Build();

        auto params = paramsBuilder.Build();

        CompareYson(R"([[1u;["test1"]]])",
            ExecuteReturningQueryWithParams(kikimr, QueryService, query, params));
    }

    {
        // Test DELETE with RETURNING using AS_TABLE with List parameter (with nullable columns)
        // First insert some data without RETURNING
        const auto insertQuery = Q_(R"(
            --!syntax_v1
            DECLARE $data AS List<Struct<id: UInt64, value: Utf8?>>;

            UPSERT INTO test_table_nullable
            SELECT * FROM AS_TABLE($data);
        )");

        auto insertParamsBuilder = TParamsBuilder();
        auto& insertDataParam = insertParamsBuilder.AddParam("$data");

        insertDataParam.BeginList();
        insertDataParam.AddListItem()
            .BeginStruct()
            .AddMember("id")
                .Uint64(20)
            .AddMember("value")
                .OptionalUtf8("delete_nullable1")
            .EndStruct();
        insertDataParam.EndList();
        insertDataParam.Build();

        auto insertParams = insertParamsBuilder.Build();

        // Execute insert without RETURNING
        if (QueryService) {
            auto qdb = kikimr.GetQueryClient();
            auto qSession = qdb.GetSession().GetValueSync().GetSession();
            auto settings = NYdb::NQuery::TExecuteQuerySettings()
                .Syntax(NYdb::NQuery::ESyntax::YqlV1);
            auto insertResult = qSession.ExecuteQuery(
                insertQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), insertParams, settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), EStatus::SUCCESS, insertResult.GetIssues().ToString());
        } else {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto insertResult = session.ExecuteDataQuery(insertQuery, TTxControl::BeginTx().CommitTx(), insertParams).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), EStatus::SUCCESS, insertResult.GetIssues().ToString());
        }

        // Now DELETE with RETURNING using AS_TABLE
        const auto deleteQuery = Q_(R"(
            --!syntax_v1
            DECLARE $data AS List<Struct<id: UInt64>>;

            DELETE FROM test_table_nullable ON
            SELECT * FROM AS_TABLE($data)
            RETURNING *;
        )");

        auto deleteParamsBuilder = TParamsBuilder();
        auto& deleteDataParam = deleteParamsBuilder.AddParam("$data");

        deleteDataParam.BeginList();
        deleteDataParam.AddListItem()
            .BeginStruct()
            .AddMember("id")
                .Uint64(20)
            .EndStruct();
        deleteDataParam.EndList();
        deleteDataParam.Build();

        auto deleteParams = deleteParamsBuilder.Build();

        CompareYson(R"([[20u;["delete_nullable1"]]])",
            ExecuteReturningQueryWithParams(kikimr, QueryService, deleteQuery, deleteParams));
    }
}

Y_UNIT_TEST_TWIN(ReturningDeleteUpdate, EnableIndexStreamWrite) {
    auto settings = TKikimrSettings(GetAppConfig(EnableIndexStreamWrite)).SetWithSampleTables(false);
    TKikimrRunner kikimr(settings);

    auto client = kikimr.GetQueryClient();

    {
        auto result = client.ExecuteQuery(R"(
            CREATE TABLE test (
                c1 Uint64 NOT NULL,
                c2 Uint64 NOT NULL,
                c3 Uint64 NOT NULL,
                PRIMARY KEY (c1)
            );)",
            NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        auto result = client.ExecuteQuery(
            R"(
            DELETE FROM test WHERE c1 = 0 RETURNING *;
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        auto result = client.ExecuteQuery(
            R"(
            DELETE FROM test WHERE c1 = 0 RETURNING c1;
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        auto result = client.ExecuteQuery(
            R"(
            DELETE FROM test WHERE c1 = 0 RETURNING c1, c2;
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        auto result = client.ExecuteQuery(
            R"(
            DELETE FROM test ON (c1) VALUES (0) RETURNING c1;
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        auto result = client.ExecuteQuery(
            R"(
            UPDATE test SET c2 = 0 WHERE c1 = 0 RETURNING *;
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        auto result = client.ExecuteQuery(
            R"(
            UPDATE test SET c2 = 0 WHERE c1 = 0 RETURNING c1;
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        auto result = client.ExecuteQuery(
            R"(
            UPDATE test ON (c1, c2) VALUES (0, 0) RETURNING c1;
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        auto result = client.ExecuteQuery(
            R"(
            UPDATE test ON (c1, c2) VALUES (0, 0) RETURNING *;
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }
}

Y_UNIT_TEST(ReturningWithUpsertOnColumnShard) {
    TKikimrSettings serverSettings;
    serverSettings.SetWithSampleTables(false);
    TKikimrRunner kikimr(serverSettings);

    auto client = kikimr.GetTableClient();
    auto session = client.CreateSession().GetValueSync().GetSession();

    const auto queryCreate = Q_(R"(
        CREATE TABLE `/Root/ColumnShardTest` (
            Col1 Uint64 NOT NULL,
            Col2 String NOT NULL,
            Col3 Int32 NOT NULL,
            PRIMARY KEY (Col1)
        )
        WITH (STORE = COLUMN);
    )");

    auto resultCreate = session.ExecuteSchemeQuery(queryCreate).GetValueSync();
    UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());

    auto db = kikimr.GetQueryClient();

    {
        const auto query = Q_(R"(
            UPSERT INTO `/Root/ColumnShardTest` (Col1, Col2, Col3) VALUES (1u, "test", 10) RETURNING *;
        )");

        auto resultFuture = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx());
        resultFuture.Wait();
        auto result = resultFuture.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
    }

    {
        const auto query = Q_(R"(
            UPSERT INTO `/Root/ColumnShardTest` (Col1, Col2, Col3) VALUES (1u, "test", 10) RETURNING Col1, Col2;
        )");

        auto resultFuture = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx());
        resultFuture.Wait();
        auto result = resultFuture.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
    }
}

Y_UNIT_TEST(ReturningWithInsertOnColumnShard) {
    TKikimrSettings serverSettings;
    serverSettings.SetWithSampleTables(false);
    TKikimrRunner kikimr(serverSettings);

    auto client = kikimr.GetTableClient();
    auto session = client.CreateSession().GetValueSync().GetSession();

    const auto queryCreate = Q_(R"(
        CREATE TABLE `/Root/ColumnShardTest` (
            Col1 Uint64 NOT NULL,
            Col2 String NOT NULL,
            Col3 Int32 NOT NULL,
            PRIMARY KEY (Col1)
        )
        WITH (STORE = COLUMN);
    )");

    auto resultCreate = session.ExecuteSchemeQuery(queryCreate).GetValueSync();
    UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());

    auto db = kikimr.GetQueryClient();

    {
        const auto query = Q_(R"(
            INSERT INTO `/Root/ColumnShardTest` (Col1, Col2, Col3) VALUES (1u, "test", 10) RETURNING *;
        )");

        auto resultFuture = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx());
        resultFuture.Wait();
        auto result = resultFuture.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
    }

    {
        const auto query = Q_(R"(
            INSERT INTO `/Root/ColumnShardTest` (Col1, Col2, Col3) VALUES (1u, "test", 10) RETURNING Col1, Col2;
        )");

        auto resultFuture = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx());
        resultFuture.Wait();
        auto result = resultFuture.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
    }
}

Y_UNIT_TEST_TWIN(TwoReturningReplace, EnableStreamIndex) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableStreamIndex));
    {
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session, false);
    }

    {
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();
        auto result = session.ExecuteQuery(
            R"(
                REPLACE INTO `/Root/SecondaryKeys` (Key, Fk, Value) VALUES (1, 1, "Payload1") RETURNING Key, Fk, Value;
                REPLACE INTO `/Root/SecondaryKeys` (Key, Fk, Value) VALUES (1, 2, "Payload2") RETURNING Key, Fk, Value;
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1];[1];["Payload1"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([
            [[1];[2];["Payload2"]]
        ])", FormatResultSetYson(result.GetResultSet(1)));
    }

    CompareYson(
        R"([
            [[1];[2];["Payload2"]]
        ])",
        ExecuteReturningQuery(kikimr, true, "SELECT Key, Fk, Value FROM `/Root/SecondaryKeys` ORDER BY Key, Fk;")
    );
}

Y_UNIT_TEST_TWIN(ReturningBeforeReplace, EnableStreamIndex) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableStreamIndex));
    {
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session, false);
    }

    {
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();
        auto result = session.ExecuteQuery(
            R"(
                REPLACE INTO `/Root/SecondaryKeys` (Key, Fk, Value) VALUES (1, 1, "Payload1") RETURNING Key, Fk, Value;
                REPLACE INTO `/Root/SecondaryKeys` (Key, Fk, Value) VALUES (1, 2, "Payload2");
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1];[1];["Payload1"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    CompareYson(
        R"([
            [[1];[2];["Payload2"]]
        ])",
        ExecuteReturningQuery(kikimr, true, "SELECT Key, Fk, Value FROM `/Root/SecondaryKeys` ORDER BY Key, Fk;")
    );
}

Y_UNIT_TEST_TWIN(ReturningAfterReplace, EnableStreamIndex) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableStreamIndex));
    {
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session, false);
    }

    {
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();
        auto result = session.ExecuteQuery(
            R"(
                REPLACE INTO `/Root/SecondaryKeys` (Key, Fk, Value) VALUES (1, 1, "Payload1");
                REPLACE INTO `/Root/SecondaryKeys` (Key, Fk, Value) VALUES (1, 2, "Payload2") RETURNING Key, Fk, Value;
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1];[2];["Payload2"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    CompareYson(
        R"([
            [[1];[2];["Payload2"]]
        ])",
        ExecuteReturningQuery(kikimr, true, "SELECT Key, Fk, Value FROM `/Root/SecondaryKeys` ORDER BY Key, Fk;")
    );
}

Y_UNIT_TEST_TWIN(TwoReturningUpsert, EnableStreamIndex) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableStreamIndex));
    {
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session, false);
    }

    {
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();
        auto result = session.ExecuteQuery(
            R"(
                UPSERT INTO `/Root/SecondaryKeys` (Key, Fk, Value) VALUES (1, 1, "Payload1") RETURNING Key, Fk, Value;
                UPSERT INTO `/Root/SecondaryKeys` (Key, Fk) VALUES (1, 2) RETURNING Key, Fk, Value;
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1];[1];["Payload1"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([
            [[1];[2];["Payload1"]]
        ])", FormatResultSetYson(result.GetResultSet(1)));
    }

    CompareYson(
        R"([
            [[1];[2];["Payload1"]]
        ])",
        ExecuteReturningQuery(kikimr, true, "SELECT Key, Fk, Value FROM `/Root/SecondaryKeys` ORDER BY Key, Fk;")
    );
}

Y_UNIT_TEST_TWIN(ReturningBeforeUpsert, EnableStreamIndex) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableStreamIndex));
    {
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session, false);
    }

    {
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();
        auto result = session.ExecuteQuery(
            R"(
                UPSERT INTO `/Root/SecondaryKeys` (Key, Fk, Value) VALUES (1, 1, "Payload1") RETURNING Key, Fk, Value;
                UPSERT INTO `/Root/SecondaryKeys` (Key, Fk) VALUES (1, 2);
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1];[1];["Payload1"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    CompareYson(
        R"([
            [[1];[2];["Payload1"]]
        ])",
        ExecuteReturningQuery(kikimr, true, "SELECT Key, Fk, Value FROM `/Root/SecondaryKeys` ORDER BY Key, Fk;")
    );
}

Y_UNIT_TEST_TWIN(ReturningAfterUpsert, EnableStreamIndex) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableStreamIndex));
    {
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session, false);
    }

    {
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();
        auto result = session.ExecuteQuery(
            R"(
                UPSERT INTO `/Root/SecondaryKeys` (Key, Fk, Value) VALUES (1, 1, "Payload1");
                UPSERT INTO `/Root/SecondaryKeys` (Key, Fk) VALUES (1, 2) RETURNING Key, Fk, Value;
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1];[2];["Payload1"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    CompareYson(
        R"([
            [[1];[2];["Payload1"]]
        ])",
        ExecuteReturningQuery(kikimr, true, "SELECT Key, Fk, Value FROM `/Root/SecondaryKeys` ORDER BY Key, Fk;")
    );
}

Y_UNIT_TEST_TWIN(ReturningUpdateNewVsOldValues, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    {
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session, false);
    }

    auto db = kikimr.GetQueryClient();
    {
        auto result = db.ExecuteQuery(
            R"(
                UPSERT INTO `/Root/SecondaryKeys` (Key, Fk, Value) VALUES (1, 10, "Old");
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        auto result = db.ExecuteQuery(
            R"(
                UPDATE `/Root/SecondaryKeys` SET Value = "New" WHERE Key = 1
                RETURNING Key, Fk, Value;
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[1];[10];["New"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        auto result = db.ExecuteQuery(
            R"(
                UPDATE `/Root/SecondaryKeys` SET Fk = 20 WHERE Key = 1
                RETURNING Key, Fk, Value;
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[1];[20];["New"]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    CompareYson(
        R"([[[20];[1]]])",
        ExecuteReturningQuery(kikimr, true,
            "SELECT Fk, Key FROM `/Root/SecondaryKeys/Index/indexImplTable` ORDER BY Key;")
    );
}

Y_UNIT_TEST_TWIN(ReturningDeletePkOnlyWithIndex, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));
    {
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session, false);
    }

    auto db = kikimr.GetQueryClient();
    {
        auto result = db.ExecuteQuery(
            R"(
                UPSERT INTO `/Root/SecondaryKeys` (Key, Fk, Value) VALUES
                    (1, 10, "a"),
                    (2, 20, "b");
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        auto result = db.ExecuteQuery(
            R"(
                DELETE FROM `/Root/SecondaryKeys` WHERE Key = 1 RETURNING Key;
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[1]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    CompareYson(
        R"([[[20];[2]]])",
        ExecuteReturningQuery(kikimr, true,
            "SELECT Fk, Key FROM `/Root/SecondaryKeys/Index/indexImplTable` ORDER BY Key;")
    );

    {
        auto result = db.ExecuteQuery(
            R"(
                DELETE FROM `/Root/SecondaryKeys` WHERE Key = 1 RETURNING Key;
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
    }
}

Y_UNIT_TEST_TWIN(ReturningForUpsert, EnableStreamIndex) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableStreamIndex));

    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    {
        const std::string query = R"(
            CREATE TABLE TestTable (
                Key Int32,
                Value String DEFAULT "default_value",
                Value2 String,
                PRIMARY KEY (Key)
            );
        )";
        auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    const auto validateTable = [&](const TString& expected) {
        const std::string query = R"(
            SELECT Key, Value, Value2 FROM TestTable ORDER BY Key;
        )";
        auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(expected, FormatResultSetYson(result.GetResultSet(0)));
    };

    {
        const std::string query = R"(
            UPSERT INTO TestTable (Key) VALUES (1) RETURNING Key, Value, Value2;
        )";
        auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[1];["default_value"];#]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        validateTable(R"([
            [[1];["default_value"];#]
        ])");
    }

    {
        const std::string query = R"(
            UPSERT INTO TestTable (Key, Value) VALUES (2, "explicit") RETURNING Key, Value, Value2;
        )";
        auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[2];["explicit"];#]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        validateTable(R"([
            [[1];["default_value"];#];
            [[2];["explicit"];#]
        ])");
    }

    {
        const std::string query = R"(
            UPSERT INTO TestTable (Key, Value2) VALUES (2, "explicit2") RETURNING Key, Value, Value2;
        )";
        auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            [[2];["explicit"];["explicit2"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        validateTable(R"([
            [[1];["default_value"];#];
            [[2];["explicit"];["explicit2"]]
        ])");
    }
}

Y_UNIT_TEST_TWIN(SelectBeforeReturning, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));

    {
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto resultCreate = session.ExecuteSchemeQuery(Q_(R"(
            --!syntax_v1
            CREATE TABLE t1 (key Int32, val String, PRIMARY KEY(key));
            CREATE TABLE t2 (key Int32, val String, PRIMARY KEY(key));
        )")).GetValueSync();
        UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());

        auto resultInsert = session.ExecuteDataQuery(Q_(R"(
            --!syntax_v1
            INSERT INTO t1 (key, val) VALUES (1, "a");
        )"), TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(resultInsert.IsSuccess(), resultInsert.GetIssues().ToString());
    }

    {
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();
        auto result = session.ExecuteQuery(R"(
            SELECT key, val FROM t1 ORDER BY key;
            UPSERT INTO t2 (key, val) VALUES (10, "x") RETURNING key, val;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 2u);
        CompareYson(R"([[[1];["a"]]])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[[10];["x"]]])", FormatResultSetYson(result.GetResultSet(1)));
    }
}

Y_UNIT_TEST_TWIN(SelectAfterReturning, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));

    {
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto resultCreate = session.ExecuteSchemeQuery(Q_(R"(
            --!syntax_v1
            CREATE TABLE t1 (key Int32, val String, PRIMARY KEY(key));
            CREATE TABLE t2 (key Int32, val String, PRIMARY KEY(key));
        )")).GetValueSync();
        UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());

        auto resultInsert = session.ExecuteDataQuery(Q_(R"(
            --!syntax_v1
            INSERT INTO t1 (key, val) VALUES (1, "a");
        )"), TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(resultInsert.IsSuccess(), resultInsert.GetIssues().ToString());
    }

    {
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();
        auto result = session.ExecuteQuery(R"(
            UPSERT INTO t2 (key, val) VALUES (10, "x") RETURNING key, val;
            SELECT key, val FROM t1 ORDER BY key;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 2u);
        CompareYson(R"([[[10];["x"]]])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[[1];["a"]]])", FormatResultSetYson(result.GetResultSet(1)));
    }
}

Y_UNIT_TEST_TWIN(SelectBetweenReturnings, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));

    {
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto resultCreate = session.ExecuteSchemeQuery(Q_(R"(
            --!syntax_v1
            CREATE TABLE t1 (key Int32, val String, PRIMARY KEY(key));
            CREATE TABLE t2 (key Int32, val String, PRIMARY KEY(key));
            CREATE TABLE t3 (key Int32, val String, PRIMARY KEY(key));
        )")).GetValueSync();
        UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());

        auto resultInsert = session.ExecuteDataQuery(Q_(R"(
            --!syntax_v1
            INSERT INTO t1 (key, val) VALUES (1, "a");
        )"), TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(resultInsert.IsSuccess(), resultInsert.GetIssues().ToString());
    }

    {
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();
        auto result = session.ExecuteQuery(R"(
            UPSERT INTO t2 (key, val) VALUES (10, "x") RETURNING key, val;
            SELECT key, val FROM t1 ORDER BY key;
            UPSERT INTO t3 (key, val) VALUES (20, "y") RETURNING key, val;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 3u);
        CompareYson(R"([[[10];["x"]]])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[[1];["a"]]])", FormatResultSetYson(result.GetResultSet(1)));
        CompareYson(R"([[[20];["y"]]])", FormatResultSetYson(result.GetResultSet(2)));
    }
}

Y_UNIT_TEST_TWIN(MultipleSelectsAndReturnings, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));

    {
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto resultCreate = session.ExecuteSchemeQuery(Q_(R"(
            --!syntax_v1
            CREATE TABLE t1 (key Int32, val String, PRIMARY KEY(key));
            CREATE TABLE t2 (key Int32, val String, PRIMARY KEY(key));
        )")).GetValueSync();
        UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());
    }

    {
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();
        auto result = session.ExecuteQuery(R"(
            SELECT 1;
            UPSERT INTO t1 (key, val) VALUES (10, "x") RETURNING key, val;
            SELECT 2;
            UPSERT INTO t2 (key, val) VALUES (20, "y") RETURNING key, val;
            SELECT 3;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 5u);
        CompareYson(R"([[1]])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[[10];["x"]]])", FormatResultSetYson(result.GetResultSet(1)));
        CompareYson(R"([[2]])", FormatResultSetYson(result.GetResultSet(2)));
        CompareYson(R"([[[20];["y"]]])", FormatResultSetYson(result.GetResultSet(3)));
        CompareYson(R"([[3]])", FormatResultSetYson(result.GetResultSet(4)));
    }
}

Y_UNIT_TEST_TWIN(MultipleSelectsAndReturningsStream, EnableIndexStreamWrite) {
    auto kikimr = DefaultKikimrRunner({}, GetAppConfig(EnableIndexStreamWrite));

    {
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto resultCreate = session.ExecuteSchemeQuery(Q_(R"(
            --!syntax_v1
            CREATE TABLE t1 (key Int32, val String, PRIMARY KEY(key));
            CREATE TABLE t2 (key Int32, val String, PRIMARY KEY(key));
        )")).GetValueSync();
        UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());
    }

    {
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings()
            .Syntax(NYdb::NQuery::ESyntax::YqlV1)
            .ConcurrentResultSets(false);
        auto it = db.StreamExecuteQuery(R"(
            SELECT 1;
            UPSERT INTO t1 (key, val) VALUES (10, "x") RETURNING key, val;
            SELECT 2;
            UPSERT INTO t2 (key, val) VALUES (20, "y") RETURNING key, val;
            SELECT 3;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

        CompareYson(R"([
            [1];
            [[10];["x"]];
            [2];
            [[20];["y"]];
            [3]
        ])", StreamResultToYson(it));
    }
}

}

} // namespace NKikimr::NKqp
