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

    Y_UNIT_TEST(PgDatabase) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                SELECT datname FROM pg_database
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "no result sets");
            CompareYson(R"([
                ["template1"];["template0"];["postgres"];["Root"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT current_catalog;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "no result sets");
            CompareYson(R"([
                ["Root"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT current_catalog = current_database();
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "no result sets");
            CompareYson(R"([
                ["t"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(PgRoles) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                SELECT rolname FROM pg_roles
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "no result sets");
            CompareYson(R"([
                ["postgres"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(PgTables) {
        bool experimentalPg = false;
        if (auto* p = std::getenv("YDB_EXPERIMENTAL_PG")) {
            experimentalPg = true;
        }
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE table1 (
                    id int4 primary key,
                    value int4
                );
                CREATE TABLE table2 (
                    id varchar primary key
                );
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM pg_tables WHERE schemaname = 'public' AND hasindexes = true ORDER BY tablename;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "no result sets");
            CompareYson(R"([
                ["t";"f";"f";"f";"public";"table1";"root@builtin";#];
                ["t";"f";"f";"f";"public";"table2";"root@builtin";#]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            auto result = db.ExecuteQuery(R"(
                --!syntax_pg
                select
                min(schemaname) min_s,
                min(tablename) min_t,
                max(schemaname) max_s,
                max(tablename) max_t
                from pg_catalog.pg_tables;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "no result sets");
            CompareYson(R"([
                ["information_schema";"_pg_foreign_data_wrappers";"public";"views"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            auto result = db.ExecuteQuery(R"(
                select count(*)
                from pg_catalog.pg_tables;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "no result sets");
            CompareYson(
                Sprintf("[[\"%u\"]]", experimentalPg ? 214 : 211),
                FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT rel.oid, rel.relname AS name
                FROM pg_catalog.pg_class rel
                    WHERE rel.relkind IN ('r','s','t','p') AND rel.relnamespace = 2200::oid
                    AND NOT rel.relispartition
                        ORDER BY rel.relname;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "no result sets");
            CompareYson(R"([
                [#;"table1"];[#;"table2"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
        { //SuperSet
            auto result = db.ExecuteQuery(R"(
                SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relkind in ('r', 'p');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "no result sets");
            CompareYson(R"([
                ["table1"];["table2"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
        { //https://github.com/ydb-platform/ydb/issues/7286
            auto result = db.ExecuteQuery(R"(
                select tablename from pg_catalog.pg_tables where tablename='pg_proc'
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "no result sets");
            CompareYson(R"([
                ["pg_proc"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
