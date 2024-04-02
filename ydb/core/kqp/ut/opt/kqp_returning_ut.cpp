#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/core/kqp/runtime/kqp_read_actor.h>
#include <ydb/core/tx/datashard/datashard_impl.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpReturning) {

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

}

} // namespace NKikimr::NKqp
