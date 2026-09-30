#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpRelativePathPrefix) {
    Y_UNIT_TEST(RuntimeParameterDoesNotReuseAnotherRoot) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto scheme = kikimr.GetSchemeClient();
        for (const TString& folder : {"folder1", "folder2"}) {
            auto result = scheme.MakeDirectory(TString("/Root/") + folder).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        auto create = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/folder1/items` (id Uint64 NOT NULL, PRIMARY KEY (id));
            CREATE TABLE `/Root/folder2/items` (id Uint64 NOT NULL, PRIMARY KEY (id));
        )").ExtractValueSync();
        UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());
        auto write = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/folder1/items` (id) VALUES (1u);
            UPSERT INTO `/Root/folder2/items` (id) VALUES (2u);
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());

        const TString query = R"(
            DECLARE $prefix AS String;
            PRAGMA RelativePathPrefix = $prefix;
            SELECT id FROM items;
        )";
        for (const auto& [folder, id] : {std::pair<TString, ui64>{"folder1", 1}, {"folder2", 2}, {"folder1", 1}}) {
            auto params = db.GetParamsBuilder().AddParam("$prefix").String(folder).Build().Build();
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params,
                TExecDataQuerySettings().KeepInQueryCache(true).CollectQueryStats(ECollectQueryStatsMode::Basic)).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT(result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(TProtoAccessor::GetProto(*result.GetStats()).compilation().from_cache(), false);
            CompareYson(id == 1 ? "[[1u]]" : "[[2u]]", FormatResultSetYson(result.GetResultSet(0)));
        }

        auto absolutePrefixParams = db.GetParamsBuilder().AddParam("$prefix").String("/folder1").Build().Build();
        auto absolutePrefixResult = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), absolutePrefixParams).ExtractValueSync();
        UNIT_ASSERT(!absolutePrefixResult.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(absolutePrefixResult.GetIssues().ToString(), "RelativePathPrefix requires a relative path");

        auto relativePrefixParams = db.GetParamsBuilder().AddParam("$prefix").String("folder1").Build().Build();
        auto absoluteTableResult = session.ExecuteDataQuery(R"(
            DECLARE $prefix AS String;
            PRAGMA RelativePathPrefix = $prefix;
            SELECT id FROM `/Root/folder2/items`;
        )", TTxControl::BeginTx().CommitTx(), relativePrefixParams).ExtractValueSync();
        UNIT_ASSERT_C(absoluteTableResult.IsSuccess(), absoluteTableResult.GetIssues().ToString());
        CompareYson("[[2u]]", FormatResultSetYson(absoluteTableResult.GetResultSet(0)));

        auto callableResult = session.ExecuteDataQuery(R"(
            $make = ($x) -> ('folder' || $x);
            PRAGMA RelativePathPrefix = ($make('1'));
            SELECT id FROM items;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(callableResult.IsSuccess(), callableResult.GetIssues().ToString());
        CompareYson("[[1u]]", FormatResultSetYson(callableResult.GetResultSet(0)));

        auto namedParamResult = session.ExecuteDataQuery(R"(
            DECLARE $prefix AS String;
            $path = $prefix;
            PRAGMA RelativePathPrefix = $path;
            SELECT id FROM items;
        )", TTxControl::BeginTx().CommitTx(),
            db.GetParamsBuilder().AddParam("$prefix").String("folder2").Build().Build()).ExtractValueSync();
        UNIT_ASSERT_C(namedParamResult.IsSuccess(), namedParamResult.GetIssues().ToString());
        CompareYson("[[2u]]", FormatResultSetYson(namedParamResult.GetResultSet(0)));
    }
}

} // namespace NKikimr::NKqp
