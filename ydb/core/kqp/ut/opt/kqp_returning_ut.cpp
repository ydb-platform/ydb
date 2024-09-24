#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/core/kqp/runtime/kqp_read_actor.h>
#include <ydb/core/tx/datashard/datashard_impl.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpReturning) {

Y_UNIT_TEST(ReturningTwice) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableSequences(true);
    appConfig.MutableTableServiceConfig()->SetEnableColumnsWithDefault(true);
    auto serverSettings = TKikimrSettings().SetAppConfig(appConfig);
    TKikimrRunner kikimr(serverSettings);

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

Y_UNIT_TEST(ReturningSerial) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableSequences(true);
    appConfig.MutableTableServiceConfig()->SetEnableColumnsWithDefault(true);
    auto serverSettings = TKikimrSettings().SetAppConfig(appConfig);
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
            INSERT INTO ReturningTable (value) VALUES(2) RETURNING key;
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());

        CompareYson(R"([[1]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        const auto query = Q_(R"(
            --!syntax_v1
            INSERT INTO ReturningTable (value) VALUES(2) RETURNING key, value;
            INSERT INTO ReturningTableExtraValue (value) VALUES(3) RETURNING key, value;
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());
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
        UNIT_ASSERT(result.IsSuccess());
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
        UNIT_ASSERT(result.GetIssues().ToString(true) == "{ <main>: Error: Type annotation, code: 1030 subissue: { <main>:3:25: Error: At function: DataQueryBlocks, At function: TKiDataQueryBlock, At function: KiReturningList! subissue: { <main>:3:25: Error: Column not found: fake } } }");
    }

    {
        const auto query = Q_(R"(
            --!syntax_v1
            UPDATE ReturningTable SET  value = 3 where key = 1 RETURNING *;
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());
        CompareYson(R"([[1;[3]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        const auto query = Q_(R"(
            --!syntax_v1
            UPDATE ReturningTableExtraValue SET  value2 = 3 where key = 2 RETURNING *;
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());
        CompareYson(R"([[2;[4];[3]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        const auto query = Q_(R"(
            --!syntax_v1
            DELETE FROM ReturningTableExtraValue WHERE key = 2 RETURNING key, value, value2;
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());
        CompareYson(R"([[2;[4];[3]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        const auto query = Q_(R"(
            --!syntax_v1
            DELETE FROM ReturningTable WHERE key <= 3 RETURNING key, value;
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());
        CompareYson(R"([[2;[2]];[3;[2]];[1;[3]]])", FormatResultSetYson(result.GetResultSet(0)));
    }
}

Y_UNIT_TEST(ReturningColumnsOrder) {
    auto kikimr = DefaultKikimrRunner();

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

Y_UNIT_TEST(ReturningTypes) {
    auto kikimr = DefaultKikimrRunner();

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

}

} // namespace NKikimr::NKqp
