#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {
using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpNativeOlap) {
    Y_UNIT_TEST_TWIN(NullableStringFiltersMatchComputeSemantics, ScalarApply) {
        NKikimrConfig::TAppConfig config;
        auto* service = config.MutableTableServiceConfig();
        service->SetEnableNewRBO(true);
        service->SetNativeFrontendMode(NKikimrConfig::TTableServiceConfig::NATIVE_REQUIRED);
        service->SetEnableFallbackToYqlOptimizer(false);
        service->SetEnableOlapScalarApply(ScalarApply);
        service->SetEnableOlapPushdownRegexp(true);
        service->SetAllowOlapDataQuery(true);
        TKikimrRunner runner(TKikimrSettings(config).SetWithSampleTables(false));
        auto client = runner.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();
        const auto schema = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/Strings` (id Int32 NOT NULL, value String, PRIMARY KEY(id))
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());
        const TVector<std::optional<std::string>> values{std::nullopt, "abc", "xyz", "", "ab", "b"};
        TValueBuilder rows;
        rows.BeginList();
        for (std::size_t i = 0; i < values.size(); ++i)
            rows.AddListItem().BeginStruct().AddMember("id").Int32(i)
                .AddMember("value").OptionalString(values[i]).EndStruct();
        rows.EndList();
        const auto inserted = client.BulkUpsert("/Root/Strings", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
        struct TCase { TString Predicate; TVector<i32> Expected; };
        const TVector<TCase> cases{
            {"Re2::Match('a.*')(value)", {1, 4}}, {"NOT Re2::Match('a.*')(value)", {0, 2, 3, 5}},
            {"Re2::Grep('b', NULL)(value)", {1, 4, 5}}, {"NOT Re2::Grep('b', NULL)(value)", {0, 2, 3}},
            {"value LIKE '%b%'", {1, 4, 5}}, {"NOT (value LIKE '%b%')", {2, 3}},
            {"StartsWith(value, 'a')", {1, 4}}, {"NOT StartsWith(value, 'a')", {2, 3, 5}},
            {"EndsWith(value, 'b')", {4, 5}}, {"NOT EndsWith(value, 'b')", {1, 2, 3}},
            {"NOT Udf(String::Contains)(value, 'b')", {0, 2, 3}},
            {"NOT String::EndsWith(value, 'b')", {0, 1, 2, 3}},
            {"COALESCE(StartsWith(value, 'a'), true)", {0, 1, 4}},
            {"COALESCE(value LIKE '%b%', true)", {0, 1, 4, 5}},
        };
        for (const bool push : {false, true}) for (const auto& test : cases) {
            const TString sql = TStringBuilder() << "PRAGMA Kikimr.OptEnableOlapPushdown='" << (push ? "true" : "false")
                << "'; SELECT id FROM `/Root/Strings` WHERE " << test.Predicate << " ORDER BY id";
            const auto result = session.ExecuteDataQuery(sql, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), sql << ": " << result.GetIssues().ToString());
            UNIT_ASSERT_C(result.GetIssues().ToString().find("[native_frontend_fallback]") == std::string::npos,
                result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            const auto& rows = result.GetResultSet(0);
            UNIT_ASSERT_VALUES_EQUAL_C(rows.RowsCount(), test.Expected.size(), sql);
            UNIT_ASSERT_VALUES_EQUAL(rows.GetColumnsMeta().front().Type.ToString(),
                TTypeBuilder().Primitive(EPrimitiveType::Int32).Build().ToString());
            TResultSetParser parser(rows);
            for (const auto id : test.Expected) {
                UNIT_ASSERT(parser.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL_C(parser.ColumnParser("id").GetInt32(), id, sql);
            }
        }
    }
}
} // namespace NKikimr::NKqp
