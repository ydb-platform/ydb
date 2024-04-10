#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(PgCatalog) {
    Y_UNIT_TEST(PgType) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                select count(*) from pg_catalog.pg_type
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "no result sets");
            CompareYson(R"([
                ["193"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(InformationSchema) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                select column_name from information_schema.columns
                order by table_schema, table_name, column_name limit 5
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "no result sets");
            CompareYson(R"([
                ["authorization_identifier"];["fdwoptions"];["fdwowner"];["foreign_data_wrapper_catalog"];
                ["foreign_data_wrapper_language"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(CheckSetConfig) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto settings = NYdb::NQuery::TExecuteQuerySettings()
            .Syntax(NYdb::NQuery::ESyntax::Pg)
            .StatsMode(NYdb::NQuery::EStatsMode::Basic);

        {
            // Base checks
            auto serverSettings = TKikimrSettings()
                .SetAppConfig(appConfig)
                .SetKqpSettings({setting});

            TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
            auto db = kikimr.GetQueryClient();
            auto session = db.GetSession().GetValueSync().GetSession();

            auto query = Q_(R"(
                CREATE TABLE PgTable (
                key int4 PRIMARY KEY,
                value text
            ))");
            auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            query = Q_(R"(
                select * from PgTable;
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            query = Q_(R"(
                select set_config('search_path', 'public', false);
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            query = Q_(R"(
                select * from PgTable;
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            query = Q_(R"(
                select set_config('search_path', 'information_schema', false);
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            query = Q_(R"(
                select * from PgTable;
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT(result.GetIssues().ToString().Contains("Unsupported table: pgtable"));

            query = Q_(R"(
                select set_config('search_path', 'public', false);
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

            query = Q_(R"(
                select * from PgTable;
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), true);
        }

        {
            // Check for disabled ast cache
            appConfig.MutableTableServiceConfig()->SetEnableAstCache(false);
            auto serverSettings = TKikimrSettings()
                .SetAppConfig(appConfig)
                .SetKqpSettings({setting});
            TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
            auto db = kikimr.GetQueryClient();
            auto session = db.GetSession().GetValueSync().GetSession();

            auto query = Q_(R"(
                select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_type;
            )");
            auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

            query = Q_(R"(
                select set_config('search_path', 'pg_catalog', false);
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

            query = Q_(R"(
                select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_type;
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

            query = Q_(R"(
                select set_config('search_path', 'public', false);
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

            query = Q_(R"(
                select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_type;
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

            query = Q_(R"(
                select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_type;
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), true);

            query = Q_(R"(
                select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_catalog.pg_type;
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

            query = Q_(R"(
                select set_config('search_path', 'public', false);
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

            query = Q_(R"(
                select set_config('search_path', 'pg_catalog', false);
                select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_type;
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

            query = Q_(R"(
                select set_config('search_path', 'pg_catalog', false);
                select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_type;
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
        }

        {
            // Check for enabled ast cache
            appConfig.MutableTableServiceConfig()->SetEnableAstCache(true);
            auto serverSettings = TKikimrSettings()
                .SetAppConfig(appConfig)
                .SetKqpSettings({setting});
            TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
            auto db = kikimr.GetQueryClient();
            auto session = db.GetSession().GetValueSync().GetSession();

            auto query = Q_(R"(
                select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_type;
            )");
            auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

            query = Q_(R"(
                select set_config('search_path', 'pg_catalog', false);
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

            query = Q_(R"(
                select set_config('search_path', 'pg_catalog', false);
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

            query = Q_(R"(
                select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_type;
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

            query = Q_(R"(
                select set_config('search_path', 'public', false);
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

            query = Q_(R"(
                select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_type;
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

            query = Q_(R"(
                select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_type;
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), true);

            query = Q_(R"(
                select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_catalog.pg_type;
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), true);
        }

        {
            // Check enable per statement
            appConfig.MutableTableServiceConfig()->SetEnableAstCache(true);
            appConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
            auto serverSettings = TKikimrSettings()
                .SetAppConfig(appConfig)
                .SetKqpSettings({setting});

            TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
            auto db = kikimr.GetQueryClient();
            auto session = db.GetSession().GetValueSync().GetSession();

            auto query = Q_(R"(
                CREATE TABLE PgTable (
                key int4 PRIMARY KEY,
                value text
            ))");
            auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            query = Q_(R"(
                select set_config('search_path', 'pg_catalog', false);
                select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_type;
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            query = Q_(R"(
                select * from PgTable;
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT(result.GetIssues().ToString().Contains("Unsupported table: pgtable"));

            query = Q_(R"(
                select set_config('search_path', 'public', false);
                select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_type;
                select * from PgTable;
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            query = Q_(R"(
                select set_config('search_path', 'public', false);
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

            query = Q_(R"(
                select oid, typinput::int4 as typinput, typname, typnamespace, typtype from pg_catalog.pg_type;
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), true);

            query = Q_(R"(
                select * from PgTable;
            )");
            result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), true);
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
