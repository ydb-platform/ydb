#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>
#include <ydb/core/kqp/ut/olap/helpers/writer.h>
#include <ydb/core/kqp/ut/olap/helpers/test_case.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/query_id.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/common/simple/settings.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_plan_conversion_utils.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_test_helpers.h>
#include <ydb/core/kqp/opt/rbo/traces/kqp_rbo_trace_output.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/aclib/aclib.h>
#include <yql/essentials/core/pg_settings/guc_settings.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/parser/pg_wrapper/interface/codec.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <ydb/public/lib/ut_helpers/ut_helpers_query.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/string/split.h>

#include <algorithm>
#include <array>
#include <ctime>
#include <optional>
#include <regex>
#include <fstream>
#include <utility>

// These headers import NKikimr::NKqp into the global namespace, so include them last
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_aggregation_builder.h>
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_join_builder.h>

#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_plan_helpers.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_query_helpers.h>

namespace NKikimr::NKqp {
namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYql::NNodes;
using namespace NStat;
using namespace NKikimr::NKqp::NYqlTests;

void TestFilter(bool columnTables) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
    appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());

    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    auto db = kikimr.GetTableClient();
    auto dbSession = db.CreateSession().GetValueSync().GetSession();

    TString schemaQ = R"(
        CREATE TABLE `/Root/foo` (
            id Int64 NOT NULL,
	            name String,
            b Int64,
            primary key(id)
        )
    )";

    if (columnTables) {
        schemaQ += R"(WITH (STORE = column))";
    }
    schemaQ += ";";

    auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
    UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

    NYdb::TValueBuilder rows;
    rows.BeginList();
    for (size_t i = 0; i < 10; ++i) {
        rows.AddListItem()
            .BeginStruct()
            .AddMember("id").Int64(i)
            .AddMember("name").String(std::to_string(i) + "_name")
            .AddMember("b").Int64(i)
            .EndStruct();
    }
    rows.EndList();

    auto resultUpsert = db.BulkUpsert("/Root/foo", rows.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

    std::vector<std::string> queries = {
         R"(
            PRAGMA YqlSelect = 'force';
            SELECT id as id2 FROM `/Root/foo` WHERE name != '3_name' order by id;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            SELECT id as id2 FROM `/Root/foo` WHERE name = '3_name' order by id;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            SELECT id, name FROM `/Root/foo` WHERE name = '3_name' order by id;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            SELECT id, b FROM `/Root/foo` WHERE b not in [1, 2] order by b;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            SELECT id, b FROM `/Root/foo` WHERE b in [1, 2] order by b;
        )",
        R"(
            PRAGMA YqlSelect = 'force';
            SELECT * FROM `/Root/foo` WHERE name = '3_name' order by id;
        )",
    };

    std::vector<std::string> results = {
        R"([[0];[1];[2];[4];[5];[6];[7];[8];[9]])",
        R"([[3]])",
        R"([[3;["3_name"]]])",
        R"([[0;[0]];[3;[3]];[4;[4]];[5;[5]];[6;[6]];[7;[7]];[8;[8]];[9;[9]]])",
        R"([[1;[1]];[2;[2]]])",
        R"([[3;["3_name"];[3]]])",
    };

    auto tableClient = kikimr.GetTableClient();
    auto session2 = tableClient.GetSession().GetValueSync().GetSession();

    for (ui32 i = 0; i < queries.size(); ++i) {
        const auto &query = queries[i];
        auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
    }
}

void TestMultipleSelects(bool columnTables) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackOnMultipleStatements(false);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
    appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());

    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    auto db = kikimr.GetTableClient();
    auto dbSession = db.CreateSession().GetValueSync().GetSession();

    TString schemaQ = R"(
        CREATE TABLE `/Root/foo` (
            id Int64 NOT NULL,
	            name String,
            b Int64,
            primary key(id)
        )
    )";

    if (columnTables) {
        schemaQ += R"(WITH (STORE = column))";
    }
    schemaQ += ";";

    auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
    UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

    NYdb::TValueBuilder rows;
    rows.BeginList();
    for (size_t i = 0; i < 10; ++i) {
        rows.AddListItem()
            .BeginStruct()
            .AddMember("id").Int64(i)
            .AddMember("name").String(std::to_string(i) + "_name")
            .AddMember("b").Int64(i)
            .EndStruct();
    }
    rows.EndList();

    auto resultUpsert = db.BulkUpsert("/Root/foo", rows.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

    std::vector<std::string> queries = {
         R"(
            SELECT id as id2 FROM `/Root/foo` WHERE name != '3_name' order by id;
            SELECT id FROM `/Root/foo`;
        )",

    };

    std::vector<std::string> results = {
        R"([[0];[1];[2];[4];[5];[6];[7];[8];[9]])",
    };

    auto queryClient = kikimr.GetQueryClient();
    auto session2 = queryClient.GetSession().GetValueSync().GetSession();

    for (ui32 i = 0; i < queries.size(); ++i) {
        const auto &query = queries[i];
        auto result = session2.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
    }
}

bool HasParam(const std::string& ast, const std::string& param) {
    auto txPos = ast.find("KqpPhysicalTx");
    if (txPos == std::string::npos) {
        return false;
    }

    return ast.find(param, txPos) != std::string::npos;
}

 void TestParams(bool columnTables) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);

    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    auto db = kikimr.GetTableClient();
    auto dbSession = db.CreateSession().GetValueSync().GetSession();

    TString schemaQ = R"(
        CREATE TABLE `/Root/foo` (
            id Int64 NOT NULL,
	            name String,
            b Int64,
            primary key(id)
        )
    )";

    if (columnTables) {
        schemaQ += R"(WITH (STORE = column))";
    }
    schemaQ += ";";

    auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
    UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

    NYdb::TValueBuilder rows;
    rows.BeginList();
    for (size_t i = 0; i < 10; ++i) {
        rows.AddListItem()
            .BeginStruct()
            .AddMember("id").Int64(i)
            .AddMember("name").String(std::to_string(i) + "_name")
            .AddMember("b").Int64(i)
            .EndStruct();
    }
    rows.EndList();

    auto resultUpsert = db.BulkUpsert("/Root/foo", rows.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

    std::vector<std::string> queries = {
        R"(
            declare $param as String;
            SELECT id as id2 FROM `/Root/foo` WHERE name != $param order by id;
        )",
        R"(
            declare $param1 as String;
            SELECT id as id2 FROM `/Root/foo` WHERE name == $param1 order by id;
        )",
    };

    auto queryClient = kikimr.GetQueryClient();
    std::vector<std::pair<std::string, std::string>> params = {{"$param", "0_name"}, {"$param1", "1_name"}};
    std::vector<std::string> results = {
          R"([[1];[2];[3];[4];[5];[6];[7];[8];[9]])",
          R"([[1]])"
    };

    for (ui32 i = 0; i < queries.size(); ++i) {
        const auto &query = queries[i];
        auto session = queryClient.GetSession().GetValueSync().GetSession();
        auto result =
            session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        auto ast = *result.GetStats()->GetAst();
        UNIT_ASSERT_C(HasParam(ast, params[i].first), "Params not specified in tx param bindings");

        // clang-format off
        auto qParams = TParamsBuilder()
            .AddParam(params[i].first)
                .String(params[i].second)
            .Build()
        .Build();
        // clang-format on
        result =
            session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), qParams, NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
    }
}

constexpr std::array<i64, 7> SqlInLookupValues = {-1, 0, 1, 2, 3, 10, 2147483648LL};

TKikimrSettings SqlInSettings(bool columnStore = false) {
    NKikimrConfig::TAppConfig appConfig;
    auto* config = appConfig.MutableTableServiceConfig();
    config->SetEnableNewRBO(true);
    config->SetEnableFallbackToYqlOptimizer(false);
    config->SetAllowOlapDataQuery(columnStore);
    return TKikimrSettings(appConfig).SetWithSampleTables(false);
}

TString SqlInFunction(bool ansi) {
    // YqlSelect does not support the ANSI IN pragma. Set the callable option
    // explicitly so both modes exercise the new RBO physical lowering.
    return TStringBuilder()
        << "$in = ($items, $lookup) -> { RETURN Yql::SqlIn($items, $lookup, "
        << (ansi ? "AsTuple(AsTuple(AsAtom('ansi')))" : "AsTuple()") << "); };\n";
}

void CreateSqlInTable(TKikimrRunner& kikimr) {
    auto client = kikimr.GetTableClient();
    auto session = client.CreateSession().GetValueSync().GetSession();
    const auto scheme = session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/sql_in` (
            id Int64 NOT NULL,
            value Int64,
            PRIMARY KEY (id)
        );
    )").GetValueSync();
    UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());

    TValueBuilder rows;
    rows.BeginList();
    for (i64 id : SqlInLookupValues) {
        rows.AddListItem().BeginStruct()
            .AddMember("id").Int64(id)
            .AddMember("value").OptionalInt64(id == 10 ? std::nullopt : std::optional<i64>(id))
            .EndStruct();
    }
    rows.EndList();
    const auto upsert = client.BulkUpsert("/Root/sql_in", rows.Build()).GetValueSync();
    UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());
}

std::optional<bool> ExpectedSqlIn(std::optional<double> lookup, const TVector<std::optional<i64>>& items, bool ansi) {
    if (!lookup) {
        return ansi && items.empty() ? std::optional<bool>(false) : std::nullopt;
    }
    if (std::find(items.begin(), items.end(), lookup) != items.end()) {
        return true;
    }
    if (ansi && std::find(items.begin(), items.end(), std::nullopt) != items.end()) {
        return std::nullopt;
    }
    return false;
}

TValue MakeSqlInListParameter(EPrimitiveType primitive, bool optional, const TVector<std::optional<i64>>& items) {
    TTypeBuilder type;
    type.BeginList();
    if (optional) {
        type.BeginOptional();
    }
    type.Primitive(primitive);
    if (optional) {
        type.EndOptional();
    }
    type.EndList();

    TValueBuilder value(type.Build());
    value.BeginList();
    for (const auto& item : items) {
        value.AddListItem();
        if (!item) {
            value.EmptyOptional();
            continue;
        }
        if (optional) {
            value.BeginOptional();
        }
        switch (primitive) {
            case EPrimitiveType::Int64: value.Int64(*item); break;
            case EPrimitiveType::Int32: value.Int32(*item); break;
            case EPrimitiveType::Uint64: value.Uint64(*item); break;
            default: UNIT_FAIL("Unexpected test parameter type");
        }
        if (optional) {
            value.EndOptional();
        }
    }
    return value.EndList().Build();
}

void TestConstantFolding(bool columnTables) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
    appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    auto db = kikimr.GetTableClient();
    auto dbSession = db.CreateSession().GetValueSync().GetSession();

    TString schemaQ = R"(
        CREATE TABLE `/Root/foo` (
            id Int64 NOT NULL,
	            name String,
            primary key(id)
        )
    )";

    if (columnTables) {
        schemaQ += R"(WITH (STORE = column))";
    }
    schemaQ += ";";

    auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
    UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

    NYdb::TValueBuilder rows;
    rows.BeginList();
    for (size_t i = 0; i < 10; ++i) {
        rows.AddListItem()
            .BeginStruct()
            .AddMember("id").Int64(i)
            .AddMember("name").String(std::to_string(i) + "_name")
            .EndStruct();
    }
    rows.EndList();

    auto resultUpsert = db.BulkUpsert("/Root/foo", rows.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

    auto tableClient = kikimr.GetTableClient();
    auto session2 = tableClient.GetSession().GetValueSync().GetSession();

    std::vector<std::string> queries = {
        R"(
            PRAGMA YqlSelect = 'force';
            SELECT id as id2 FROM `/Root/foo` WHERE id = 15 - 14 and 18 - 17 = 1;
        )"
    };

    std::vector<std::string> results = {
        R"([[1]])",
    };

    for (ui32 i = 0; i < queries.size(); ++i) {
        const auto &query = queries[i];
        auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
    }
}

void TestQueryClient(bool columnTables) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
    appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());

    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    auto db = kikimr.GetTableClient();
    auto dbSession = db.CreateSession().GetValueSync().GetSession();

    TString schemaQ = R"(
        CREATE TABLE `/Root/foo` (
            id Int64 NOT NULL,
	            name String,
            b Int64,
            primary key(id)
        )
    )";

    if (columnTables) {
        schemaQ += R"(WITH (STORE = column))";
    }
    schemaQ += ";";

    auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
    UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

    NYdb::TValueBuilder rows;
    rows.BeginList();
    for (size_t i = 0; i < 10; ++i) {
        rows.AddListItem()
            .BeginStruct()
            .AddMember("id").Int64(i)
            .AddMember("name").String(std::to_string(i) + "_name")
            .AddMember("b").Int64(i)
            .EndStruct();
    }
    rows.EndList();

    auto resultUpsert = db.BulkUpsert("/Root/foo", rows.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

    std::vector<std::string> queries = {
        R"(
            SELECT id, b FROM `/Root/foo` WHERE b not in [1, 2] order by b;
        )",
        R"(
            SELECT * FROM `/Root/foo` WHERE name = '3_name' order by id;
        )",
    };

    std::vector<std::string> results = {
        R"([[0;[0]];[3;[3]];[4;[4]];[5;[5]];[6;[6]];[7;[7]];[8;[8]];[9;[9]]])",
        R"([[3;["3_name"];[3]]])",
    };

    auto queryClient = kikimr.GetQueryClient();

    for (ui32 i = 0; i < queries.size(); ++i) {
        const auto& query = queries[i];
        auto session = queryClient.GetSession().GetValueSync().GetSession();

        // Explain.
        auto result =
            session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        // Execute.
        result =
            session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
    }
}

void TestLimit(bool columnTables) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);

    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    auto db = kikimr.GetTableClient();
    auto dbSession = db.CreateSession().GetValueSync().GetSession();

    TString schemaQ = R"(
        CREATE TABLE `/Root/foo` (
            id Int64 NOT NULL,
	            name String,
            b Int64,
            primary key(id)
        )
    )";

    if (true || columnTables) {
        schemaQ += R"(WITH (STORE = column))";
    }
    schemaQ += ";";

    auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
    UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

    NYdb::TValueBuilder rows;
    rows.BeginList();
    for (size_t i = 0; i < 10; ++i) {
        rows.AddListItem()
            .BeginStruct()
            .AddMember("id").Int64(i)
            .AddMember("name").String(std::to_string(i) + "_name")
            .AddMember("b").Int64(i)
            .EndStruct();
    }
    rows.EndList();

    auto resultUpsert = db.BulkUpsert("/Root/foo", rows.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

    std::vector<std::string> queries = {
        R"(
            SELECT id FROM `/Root/foo` order by id limit 1 + 2;
        )",
        R"(
            SELECT id FROM `/Root/foo` order by id limit 5;
        )",
        R"(
            SELECT id FROM `/Root/foo` order by id limit 5 offset 1;
        )",
    };

    std::vector<std::string> results = {
        R"([[0];[1];[2]])",
        R"([[0];[1];[2];[3];[4]])",
        R"([[1];[2];[3];[4];[5]])"
    };

    auto queryClient = kikimr.GetQueryClient();

    for (ui32 i = 0; i < queries.size(); ++i) {
        const auto& query = queries[i];
        auto session = queryClient.GetSession().GetValueSync().GetSession();
        auto result =
            session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        auto ast = *result.GetStats()->GetAst();
        UNIT_ASSERT_VALUES_EQUAL(CountNumberOfCallables(ast, "DqCnMerge"), 1);

        result =
            session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
    }
}

void InsertIntoAliasesRenames(NYdb::NTable::TTableClient &db, std::string tableName, int numRows) {
    NYdb::TValueBuilder rows;
    rows.BeginList();
    for (size_t i = 0; i < numRows; ++i) {
        rows.AddListItem()
            .BeginStruct()
            .AddMember("id").Int64(i)
            .AddMember("join_id").Int64(i + 1)
            .AddMember("c").Int64(i + 2)
            .EndStruct();
    }
    rows.EndList();
    auto resultUpsert = db.BulkUpsert(tableName, rows.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());
}

void AliasesRenamesTest(bool newRbo) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(newRbo);
    appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
    appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/foo_0` (
            id Int64 NOT NULL,
            join_id Int64 NOT NULL,
            c Int64,
            primary key(id)
        ) with (Store = Column);

        CREATE TABLE `/Root/foo_1` (
            id Int64	NOT NULL,
            join_id Int64 NOT NULL,
            c Int64,
            primary key(id)
        ) with (Store = Column);

        CREATE TABLE `/Root/foo_2` (
            id Int64 NOT NULL,
            join_id Int64 NOT NULL,
            c Int64,
            primary key(id)
        ) with (Store = Column);
    )").GetValueSync();

    std::vector<std::pair<std::string, int>> tables{{"/Root/foo_0", 4}, {"/Root/foo_1", 3}, {"/Root/foo_2", 2}};
    for (const auto &[table, rowsNum] : tables) {
        InsertIntoAliasesRenames(db, table, rowsNum);
    }
    db = kikimr.GetTableClient();
    auto session2 = db.CreateSession().GetValueSync().GetSession();

    auto result = session2.ExecuteDataQuery(R"(
        PRAGMA YqlSelect = 'force';
        PRAGMA AnsiImplicitCrossJoin;

        WITH cte as (
            SELECT a1.id2, join_id FROM (SELECT id as id2, join_id FROM `/Root/foo_0` as foo_0) as a1)

        SELECT X1.id2, X2.id2
        FROM
           (SELECT id2
            FROM `/Root/foo_1` as foo_1, cte
            WHERE foo_1.join_id = cte.join_id) as X1,

           (SELECT id2
            FROM `/Root/foo_2` as foo_2, cte
            WHERE foo_2.join_id = cte.join_id) as X2

        ORDER BY X1.id2, X2.id2;
    )", TTxControl::BeginTx().CommitTx()).GetValueSync();

    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([[0;0];[0;1];[1;0];[1;1];[2;0];[2;1]])");
}

} // namespace

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYqlTests;

Y_UNIT_TEST_SUITE(KqpRboYql) {

    Y_UNIT_TEST(Select) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);

        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            PRAGMA YqlSelect = 'force';
            SELECT 1 as a, 2 as b;
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(Filter, ColumnStore) {
        TestFilter(ColumnStore);
    }

    Y_UNIT_TEST_TWIN(MultipleSelects, ColumnStore) {
        TestMultipleSelects(ColumnStore);
    }

    Y_UNIT_TEST(PGInsertUpdate) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackOnDML(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto dbSession = db.CreateSession().GetValueSync().GetSession();

        TString schemaQ = R"(
            CREATE TABLE src (
                _q_000_f_000_type String,
                _q_000_f_000_rtref String,
                _q_000_f_000_rrref String,
                _q_000_f_001_type String,
                _q_000_f_001_rtref String,
                _q_000_f_001_rrref String,
                _q_000_f_002 String,
                _ydb_pk String NOT NULL,
                PRIMARY KEY (_ydb_pk)
            );

            CREATE TABLE dst (
                _q_000_f_000_type String,
                _q_000_f_000_rtref String,
                _q_000_f_000_rrref String,
                _q_000_f_001_type String,
                _q_000_f_001_rtref String,
                _q_000_f_001_rrref String,
                _q_000_f_002 Decimal(35,4),
                _ydb_pk Utf8 NOT NULL,
                PRIMARY KEY (_ydb_pk)
            );
        )";

        auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();
        auto dbSession2 = client.GetSession().GetValueSync().GetSession();

        auto insertSelectRes = dbSession2.ExecuteQuery(R"(
            INSERT INTO dst (`_q_000_f_000_type`, `_q_000_f_000_rtref`, `_q_000_f_000_rrref`, `_q_000_f_001_type`, `_q_000_f_001_rtref`, `_q_000_f_001_rrref`, `_q_000_f_002`, `_ydb_pk`) 
SELECT `_q_000_f_000_type` AS `_q_000_f_000_type`, `_q_000_f_000_rtref` AS `_q_000_f_000_rtref`, `_q_000_f_000_rrref` AS `_q_000_f_000_rrref`, `_q_000_f_001_type` AS `_q_000_f_001_type`, `_q_000_f_001_rtref` AS `_q_000_f_001_rtref`, `_q_000_f_001_rrref` AS `_q_000_f_001_rrref`, CAST(`_q_000_f_002` AS Decimal(35,4)) AS `_q_000_f_002`, CAST(RandomUuid(`_ydb_pk`) AS Utf8) AS `_ydb_pk` 
FROM (
    SELECT `t3`.`_q_000_f_000_type`, `t3`.`_q_000_f_000_rtref`, `t3`.`_q_000_f_000_rrref`, `t3`.`_q_000_f_001_type`, `t3`.`_q_000_f_001_rtref`, `t3`.`_q_000_f_001_rrref`, `t3`.`_q_000_f_002`, `t3`.`_ydb_pk`
    FROM src AS `t3`
) AS `direct_base_source`
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT(insertSelectRes.IsSuccess());

    }

    Y_UNIT_TEST_TWIN(InsertSelectColumnOrder, Distinct) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackOnDML(false);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        for (const TString table : {"src", "dst"}) {
            auto result = tableSession.ExecuteSchemeQuery(TStringBuilder() << "CREATE TABLE " << table << R"( (
                _q_001_f_001_type String,
                _q_001_f_001_rrref String,
                _ydb_pk Utf8,
                PRIMARY KEY (_ydb_pk)
            );)").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        auto rows = NYdb::TValueBuilder()
            .BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("_q_001_f_001_type").String("TYPE")
                    .AddMember("_q_001_f_001_rrref").String("REF")
                    .AddMember("_ydb_pk").Utf8("source")
                .EndStruct()
            .EndList().Build();
        auto seed = kikimr.GetTableClient().BulkUpsert("/Root/src", std::move(rows)).GetValueSync();
        UNIT_ASSERT_C(seed.IsSuccess(), seed.GetIssues().ToString());

        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        const TString select = Distinct ? "SELECT DISTINCT " : "SELECT ";
        const TVector<TString> queries = {
            TStringBuilder() << R"(
                INSERT INTO dst (_q_001_f_001_type, _q_001_f_001_rrref, _ydb_pk)
            )" << select << R"(
                _q_001_f_001_type, _q_001_f_001_rrref, CAST("target" AS Utf8)
                FROM src;
            )",
            TStringBuilder() << R"(
                INSERT INTO dst (_ydb_pk, _q_001_f_001_type, _q_001_f_001_rrref)
            )" << select << R"(
                CAST("aliased" AS Utf8) AS z, _q_001_f_001_type AS y, _q_001_f_001_rrref AS a
                FROM src;
            )",
            TStringBuilder() << R"(
                INSERT INTO dst (_q_001_f_001_type, _q_001_f_001_rrref, _ydb_pk)
            )" << select << R"(
                _q_001_f_001_type, _q_001_f_001_rrref, CAST("limited" AS Utf8)
                FROM src ORDER BY _q_001_f_001_type LIMIT 1;
            )",
            TStringBuilder() << R"(
                INSERT INTO dst (_ydb_pk, _q_001_f_001_type, _q_001_f_001_rrref)
            )" << select << R"(
                Unwrap(CAST("union" AS Utf8)) AS _ydb_pk, _q_001_f_001_type, _q_001_f_001_rrref
                FROM src
                UNION
            )" << select << R"(
                CAST("union" AS Utf8) AS _ydb_pk, _q_001_f_001_type, _q_001_f_001_rrref
                FROM src ORDER BY _ydb_pk LIMIT 1;
            )",
        };
        const auto before = GetNewRBOCompileCounters(kikimr);
        for (const auto& query : queries) {
            auto insert = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(insert.IsSuccess(), insert.GetIssues().ToString());
        }
        const auto after = GetNewRBOCompileCounters(kikimr);
        UNIT_ASSERT_C(after.first > before.first, "INSERT must use the new RBO");
        UNIT_ASSERT_VALUES_EQUAL(after.second, before.second);

        auto result = session.ExecuteQuery(R"(
            SELECT _q_001_f_001_type, _q_001_f_001_rrref, _ydb_pk FROM dst ORDER BY _ydb_pk;
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)),
            R"([[["TYPE"];["REF"];["aliased"]];[["TYPE"];["REF"];["limited"]];[["TYPE"];["REF"];["target"]];[["TYPE"];["REF"];["union"]]])");
    }

    Y_UNIT_TEST(InsertUpdate) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackOnDML(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto dbSession = db.CreateSession().GetValueSync().GetSession();

        TString schemaQ = R"(
            CREATE TABLE src (
                id Uint64 NOT NULL,
                v Int64,
                PRIMARY KEY (id)
            );

            CREATE TABLE dst (
                id Uint64 NOT NULL,
                v Int64,
                PRIMARY KEY (id)
            );

            ALTER TABLE `/Root/dst` ADD INDEX Index1 GLOBAL ON (id, v);
            ALTER TABLE `/Root/dst` ADD INDEX Index12 GLOBAL ON (v);

        )";

        auto schemaResult = dbSession.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();
        auto dbSession2 = client.GetSession().GetValueSync().GetSession();

        NYdb::TValueBuilder rows;
        rows.BeginList();
        rows.AddListItem()
            .BeginStruct()
            .AddMember("id").Uint64(1)
            .AddMember("v").Int64(10)
            .EndStruct();
        rows.AddListItem()
            .BeginStruct()
            .AddMember("id").Uint64(2)
            .AddMember("v").Int64(20)
            .EndStruct();
        rows.EndList();

        auto result = db.BulkUpsert("/Root/src", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto insertSelectRes = dbSession2.ExecuteQuery(R"(
            --PRAGMA YqlSelect = "disable";
            INSERT INTO dst (id, v)
            SELECT id, v
            FROM src;
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT(insertSelectRes.IsSuccess());

        auto selectRes = dbSession2.ExecuteQuery(R"(
            SELECT id, v
            FROM dst;
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(selectRes.GetResultSet(0)), R"([[1u;[10]];[2u;[20]]])");

        auto updateSelectRes = dbSession2.ExecuteQuery(R"(
            --PRAGMA YqlSelect = "disable";
            UPDATE dst ON
            SELECT id, v
            FROM src;
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT(updateSelectRes.IsSuccess());

        selectRes = dbSession2.ExecuteQuery(R"(
            SELECT id, v
            FROM dst;
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(selectRes.GetResultSet(0)), R"([[1u;[10]];[2u;[20]]])");

        auto deleteRes = dbSession2.ExecuteQuery(R"(
            --PRAGMA YqlSelect = "disable";
            DELETE FROM dst ON
            SELECT id, v
            FROM src;
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT(deleteRes.IsSuccess());

        selectRes = dbSession2.ExecuteQuery(R"(
            SELECT id, v
            FROM dst;
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(selectRes.GetResultSet(0)), R"([])");

    }

    Y_UNIT_TEST_TWIN(Params, ColumnStore) {
        TestParams(ColumnStore);
    }

    Y_UNIT_TEST_TWIN(AsTable, EnablePeepholeNewRbo) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetEnableNewRBOPhysicalStagePeephole(EnablePeepholeNewRbo);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();

        const auto params = TParamsBuilder()
            .AddParam("$param")
                .BeginList()
                    .AddListItem().BeginStruct()
                        .AddMember("id").Int32(1)
                        .AddMember("value").String("one")
                    .EndStruct()
                .EndList()
            .Build()
            .Build();

        const TString query = R"(
            DECLARE $param AS List<Struct<id:Int32,value:String>>;
            SELECT id, value FROM AS_TABLE($param);
        )";

        auto explain = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
        UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());
        UNIT_ASSERT_C(explain.GetStats() && explain.GetStats()->GetAst(), "Missing final AST");
        UNIT_ASSERT_STRING_CONTAINS(*explain.GetStats()->GetAst(),
            EnablePeepholeNewRbo ? "ToFlow $param" : "Iterator $param");
        UNIT_ASSERT_C(explain.GetStats()->GetPlan(), "Missing explain plan");
        const TString plan = *explain.GetStats()->GetPlan();
        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        const auto* source = FindOperatorByStringField(simplifiedPlan, "Name", "EmptySource");
        UNIT_ASSERT_C(source, plan);
        UNIT_ASSERT_VALUES_EQUAL(GetStringField(*source, "Parameter"), "$param");

        auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), params).ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([[1;"one"]])");

        const auto moreParams = TParamsBuilder()
            .AddParam("$param")
                .BeginList()
                    .AddListItem().BeginStruct()
                        .AddMember("id").Int32(3)
                        .AddMember("value").String("three")
                    .EndStruct()
                    .AddListItem().BeginStruct()
                        .AddMember("id").Int32(1)
                        .AddMember("value").String("one")
                    .EndStruct()
                    .AddListItem().BeginStruct()
                        .AddMember("id").Int32(2)
                        .AddMember("value").String("two")
                    .EndStruct()
                .EndList()
            .Build()
            .Build();

        const TString filteredQuery = R"(
            DECLARE $param AS List<Struct<id:Int32,value:String>>;
            SELECT p.id, p.value || "!" AS marked
            FROM AS_TABLE($param) AS p
            WHERE p.id >= 2
            ORDER BY p.id;
        )";

        auto filteredExplain = session.ExecuteQuery(filteredQuery, NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
        UNIT_ASSERT_C(filteredExplain.IsSuccess(), filteredExplain.GetIssues().ToString());
        UNIT_ASSERT_C(filteredExplain.GetStats() && filteredExplain.GetStats()->GetPlan(), "Missing filtered explain plan");
        const TString filteredPlan = *filteredExplain.GetStats()->GetPlan();
        const auto filteredSimplifiedPlan = GetSimplifiedPlan(filteredPlan);
        const auto* filteredSource = FindOperatorByStringField(filteredSimplifiedPlan, "Name", "EmptySource");
        UNIT_ASSERT_C(filteredSource, filteredPlan);
        UNIT_ASSERT_VALUES_EQUAL(GetStringField(*filteredSource, "Parameter"), "$param");
        UNIT_ASSERT_C(FindOperatorByStringField(filteredSimplifiedPlan, "Name", "Filter"), filteredPlan);

        result = session.ExecuteQuery(filteredQuery, NYdb::NQuery::TTxControl::NoTx(), moreParams).ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([[2;"two!"];[3;"three!"]])");

        result = session.ExecuteQuery(R"(
            DECLARE $param AS List<Struct<id:Int32,value:String>>;
            SELECT p.id FROM AS_TABLE($param) AS p WHERE p.id > 10;
        )", NYdb::NQuery::TTxControl::NoTx(), moreParams).ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), "[]");
    }

    Y_UNIT_TEST_TWIN(SqlInScalarListParameters, Ansi) {
        TKikimrRunner kikimr(SqlInSettings());
        CreateSqlInTable(kikimr);
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        for (const auto& [primitive, optional] : {
            std::pair{EPrimitiveType::Int64, false},
            std::pair{EPrimitiveType::Int64, true},
            std::pair{EPrimitiveType::Int32, false},
            std::pair{EPrimitiveType::Uint64, true},
        }) {
            TVector<TVector<std::optional<i64>>> lists = {{}, {1, 3, 3}, {7}, {0}};
            if (primitive == EPrimitiveType::Int32) {
                // 2147483648 must not wrap to a matching Int32 key.
                lists.push_back({-2147483648LL});
            }
            if (optional) {
                lists.push_back({1, std::nullopt});
                lists.push_back({std::nullopt});
            }
            for (const auto& items : lists) {
                const auto value = MakeSqlInListParameter(primitive, optional, items);
                const auto params = TParamsBuilder().AddParam("$items", value).Build();
                for (bool peephole : {true, false}) {
                    const TString query = TStringBuilder() << SqlInPeepholePragma(peephole)
                        << "DECLARE $items AS " << value.GetType().ToString() << ";\n"
                        << SqlInFunction(Ansi)
                        << R"(
                            SELECT id,
                                $in($items, id) AS found,
                                NOT $in($items, id) AS missing,
                                $in($items, value) AS nullable_found,
                                NOT $in($items, value) AS nullable_missing,
                                $in($items, CAST(id AS Double) + 0.5) AS fractional_found,
                                NOT $in($items, CAST(id AS Double) + 0.5) AS fractional_missing
                            FROM `/Root/sql_in`
                            ORDER BY id;
                        )";
                    const auto result = session.ExecuteDataQuery(
                        query, TTxControl::BeginTx().CommitTx(), params).GetValueSync();
                    UNIT_ASSERT_C(result.IsSuccess(), query << "\n" << result.GetIssues().ToString());
                    const auto& resultSet = result.GetResultSet(0);
                    UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), SqlInLookupValues.size());

                    // Check the result types, including the non-nullable ANSI case.
                    for (size_t column = 1; column < 7; ++column) {
                        const bool nullable = column == 3 || column == 4 || (Ansi && optional);
                        UNIT_ASSERT_VALUES_EQUAL_C(resultSet.GetColumnsMeta()[column].Type.ToString(),
                            nullable ? "Bool?" : "Bool", query);
                    }
                    TResultSetParser parser(resultSet);
                    for (i64 id : SqlInLookupValues) {
                        UNIT_ASSERT(parser.TryNextRow());
                        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(0).GetInt64(), id);
                        for (size_t column = 1; column < 7; ++column) {
                            const bool nullableValue = column == 3 || column == 4;
                            const auto lookup = nullableValue && id == 10 ? std::nullopt
                                : std::optional<double>(id + (column >= 5 ? 0.5 : 0.0));
                            auto expected = ExpectedSqlIn(lookup, items, Ansi);
                            if (column % 2 == 0 && expected) {
                                expected = !*expected;
                            }
                            const bool nullable = nullableValue || (Ansi && optional);
                            const auto actual = nullable ? parser.ColumnParser(column).GetOptionalBool()
                                : std::optional<bool>(parser.ColumnParser(column).GetBool());
                            UNIT_ASSERT_C(actual == expected,
                                query << "\nid=" << id << ", column=" << column << ", list size=" << items.size());
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST_TWIN(SqlInLiteralCollections, Ansi) {
        TKikimrRunner kikimr(SqlInSettings());
        CreateSqlInTable(kikimr);
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        TString baseline;
        for (bool peephole : {true, false}) {
            const TString query = SqlInPeepholePragma(peephole) + SqlInFunction(Ansi) + R"(
                SELECT id,
                    $in((1, 2), value) AS small_tuple,
                    NOT $in((1, 2, 3, 4, 5, 6), value) AS large_tuple,
                    $in([1, 2, 3, 4, 5, 6], value) AS large_list,
                    NOT $in(AsList(), value) AS empty_list,
                    $in((1, NULL), value) AS nullable_tuple,
                    $in(AsTuple(), value) AS empty_tuple,
                    $in([NULL], value) AS null_list,
                    $in([1, 2, 3, 4, 5, 6], NULL) AS null_lookup
                FROM `/Root/sql_in` ORDER BY id;
            )";
            const auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), query << "\n" << result.GetIssues().ToString());
            const TString actual = FormatResultSetYson(result.GetResultSet(0));
            if (peephole) {
                baseline = actual;
            } else {
                UNIT_ASSERT_VALUES_EQUAL(actual, baseline);
            }
        }
    }

    Y_UNIT_TEST_TWIN(SqlInParameterizedFilterAndAggregate, ColumnStore) {
        TKikimrRunner kikimr(SqlInSettings(ColumnStore));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();
        const TString schema = TStringBuilder() << R"(
            CREATE TABLE `/Root/inforg8025` (
                id Int64 NOT NULL,
                _period Int64,
                _fld8026rref String,
                _fld8027rref String,
                _fld8028rref String,
                _fld543 String,
                PRIMARY KEY (id)
            )
        )" << (ColumnStore ? "WITH (STORE = COLUMN);" : ";");
        const auto scheme = session.ExecuteSchemeQuery(schema).GetValueSync();
        UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());
        TValueBuilder rows;
        rows.BeginList();
        for (i64 id : {1, 2, 3, 4}) {
            rows.AddListItem().BeginStruct()
                .AddMember("id").Int64(id)
                .AddMember("_period").Int64(id)
                .AddMember("_fld8026rref").String("group")
                .AddMember("_fld8027rref").String("included")
                .AddMember("_fld8028rref").String(id == 3 ? "excluded" : "allowed")
                .AddMember("_fld543").String("tenant")
                .EndStruct();
        }
        const auto upsert = client.BulkUpsert("/Root/inforg8025", rows.EndList().Build()).GetValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());
        const auto params = TParamsBuilder()
            .AddParam("$const_0").Int64(3).Build()
            .AddParam("$const_1").BeginList().AddListItem().String("included").EndList().Build()
            .AddParam("$const_2").BeginList().AddListItem().OptionalString("excluded")
                .AddListItem().OptionalString(std::nullopt).EndList().Build()
            .AddParam("$const_3").String("tenant").Build()
            .AddParam("$const_4").String("group").Build()
            .Build();
        for (bool peephole : {true, false}) {
            const TString query = SqlInPeepholePragma(peephole) + R"(
                DECLARE $const_0 AS Int64;
                DECLARE $const_1 AS List<String>;
                DECLARE $const_2 AS List<String?>;
                DECLARE $const_3 AS String;
                DECLARE $const_4 AS String;

                SELECT t1._fld8026rref AS __ydb_aggregate_0, MAX(t1._period) AS __ydb_aggregate_1
                FROM `/Root/inforg8025` AS t1
                WHERE t1._period <= $const_0
                    AND t1._fld8027rref IN $const_1
                    AND t1._fld8028rref NOT IN $const_2
                    AND t1._fld543 = $const_3
                    AND t1._fld8026rref = $const_4
                GROUP BY t1._fld8026rref;
            )";
            const auto result = session.ExecuteDataQuery(
                query, TTxControl::BeginTx().CommitTx(), params).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[["group"];[2]]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST_TWIN(ConstantFolding, ColumnStore) {
        TestConstantFolding(ColumnStore);
    }

    Y_UNIT_TEST(OrderBy) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b String,
                c Int64,
                primary key(a)
            ) with (Store = Column);

            CREATE TABLE `/Root/t2` (
                a Int64	NOT NULL,
                b String,
                c Int64,
                primary key(a)
            ) with (Store = Column);
        )").GetValueSync();

        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();
        std::vector<std::pair<std::string, int>> tables{{"/Root/t1", 4}, {"/Root/t2", 3}};
        for (const auto &[table, rowsNum] : tables) {
            InsertIntoSchema0(db, table, rowsNum);
        }

        std::vector<std::string> queries = {
            R"(
                SELECT a FROM `/Root/t1`
                ORDER BY a DESC;
            )",
            R"(
                SELECT a, c FROM `/Root/t1`
                ORDER BY a DESC, c ASC;
            )",
            R"(
                SELECT a FROM `/Root/t1`
                UNION ALL
                SELECT a FROM `/Root/t2`
                ORDER BY a DESC;
            )",
            R"(
                SELECT a, a AS x FROM `/Root/t1`
                ORDER BY b DESC;
            )",
            R"(
                SELECT a, a AS x FROM `/Root/t1`
                ORDER BY b DESC
                LIMIT 1;
            )"
        };

        std::vector<std::string> results = {
            R"([[3];[2];[1];[0]])",
            R"([[3;[4]];[2;[3]];[1;[2]];[0;[1]]])",
            R"([[3];[2];[2];[1];[1];[0];[0]])",
            R"([[3;3];[2;2];[1;1];[0;0]])",
            R"([[3;3]])"
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

     Y_UNIT_TEST_TWIN(QueryClient, ColumnStore) {
        TestQueryClient(ColumnStore);
    }

    Y_UNIT_TEST_TWIN(Limit, ColumnStore) {
        TestLimit(ColumnStore);
    }

    Y_UNIT_TEST(AliasesRenames) {
        AliasesRenamesTest(true);
        //AliasesRenamesTest(false);
    }

    Y_UNIT_TEST(UnionAll) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b String,
                c Int64,
                primary key(a)
            ) with (Store = Column);

            CREATE TABLE `/Root/t2` (
                a Int64	NOT NULL,
                b String,
                c Int64,
                primary key(a)
            ) with (Store = Column);

            CREATE TABLE `/Root/t3` (
                a Int64 NOT NULL,
                b String,
                c Int64,
                primary key(a)
            ) with (Store = Column);
        )").GetValueSync();


        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();
        std::vector<std::pair<std::string, int>> tables{{"/Root/t1", 4}, {"/Root/t2", 3}, {"/Root/t3", 2}};
        for (const auto &[table, rowsNum] : tables) {
            InsertIntoSchema0(db, table, rowsNum);
        }

        const std::vector<std::string> queries = {
            R"(
                SELECT t1.a as a FROM `/Root/t1` as t1
                UNION ALL
                SELECT t2.a as a FROM `/Root/t2` as t2
                ORDER BY a;
            )",
            R"(
                SELECT t1.a as a FROM `/Root/t1` as t1
                UNION ALL
                SELECT t2.a as a FROM `/Root/t2` as t2
                UNION ALL
                SELECT t3.a as a FROM `/Root/t3` as t3
                ORDER BY a;
            )",
             R"(
                SELECT t1.a as a, t1.c as c FROM `/Root/t1` as t1
                UNION ALL
                SELECT t2.a as a, Cast(99 as Int64) as c FROM `/Root/t2` as t2
                ORDER BY a, c;
            )",
        };

        const std::vector<std::string> results = {
            R"([[0];[0];[1];[1];[2];[2];[3]])",
            R"([[0];[0];[0];[1];[1];[1];[2];[2];[3]])",
            R"([[0;[1]];[0;[99]];[1;[2]];[1;[99]];[2;[3]];[2;[99]];[3;[4]]])",
        };

        auto queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),  NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
            //Cout << FormatResultSetYson(result.GetResultSet(0)) << Endl;
        }
    }

    Y_UNIT_TEST(SetOps) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b String,
                c Int64,
                primary key(a)
            ) with (Store = Column);

            CREATE TABLE `/Root/t2` (
                a Int64	NOT NULL,
                b String,
                c Int64,
                primary key(a)
            ) with (Store = Column);
        )").GetValueSync();


        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();
        std::vector<std::pair<std::string, int>> tables{{"/Root/t1", 4}, {"/Root/t2", 3}};
        for (const auto &[table, rowsNum] : tables) {
            InsertIntoSchema0(db, table, rowsNum);
        }

        const std::vector<std::string> queries = {
            R"(
                SELECT t1.a as a FROM `/Root/t1` as t1 WHERE t1.a == 0
                INTERSECT
                SELECT t2.a as a FROM `/Root/t2` as t2
                ORDER BY a;
            )",
            R"(
                SELECT t1.a as a FROM `/Root/t1` as t1
                UNION
                SELECT t2.a as a FROM `/Root/t2` as t2
                ORDER BY a;
            )",
            R"(
                SELECT t1.a as a FROM `/Root/t1` as t1
                EXCEPT
                SELECT t2.a as a FROM `/Root/t2` as t2 WHERE t2.a IN (0,1)
                ORDER BY a;
            )",
        };

        const std::vector<std::string> results = {
            R"([[0]])",
            R"([[0];[1];[2];[3]])",
            R"([[2];[3]])"
        };

        auto queryClient = kikimr.GetQueryClient();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto &query = queries[i];
            auto session = queryClient.GetSession().GetValueSync().GetSession();
            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();

            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),  NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), results[i]);
        }
    }

}

} // namespace NKikimr::NKqp
