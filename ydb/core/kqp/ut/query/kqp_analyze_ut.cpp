#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <thread>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NTable;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpAnalyze) {

using namespace NStat;

Y_UNIT_TEST_TWIN(AnalyzeTable, ColumnStore) {
    TTestEnv env(1, 1, true);

    CreateDatabase(env, "Database");

    TTableClient client(env.GetDriver());
    auto session = client.CreateSession().GetValueSync().GetSession();

    TString createTable = Sprintf(R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            )
        )", "Root/Database/Table");
    if (ColumnStore) {
        createTable += 
            R"(
                PARTITION BY HASH(Key)
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16
                )
            )";
    }

    auto result = session.ExecuteSchemeQuery(createTable).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    TValueBuilder rows;
    rows.BeginList();
    for (size_t i = 0; i < 1500; ++i) {
        auto key = TValueBuilder().Uint64(i).Build();
        auto value = TValueBuilder().OptionalString("Hello,world!").Build();
        
        rows.AddListItem();
            rows.BeginStruct();
                rows.AddMember("Key", key);
                rows.AddMember("Value", value);
            rows.EndStruct();
    }
    rows.EndList();
    
    result = client.BulkUpsert("Root/Database/Table", rows.Build()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    result = session.ExecuteSchemeQuery(
        Sprintf(R"(ANALYZE `Root/%s/%s`)", "Database", "Table")
    ).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    auto& runtime = *env.GetServer().GetRuntime();
    ui64 saTabletId;
    auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);
    
    auto countMin = ExtractCountMin(runtime, pathId, 2);
    TString value = "Hello,world!";
    auto stat = countMin->Probe(value.data(), value.size());
    UNIT_ASSERT_C(stat >= 1500, ToString(stat));
}

Y_UNIT_TEST(AnalyzeError) {
    TTestEnv env(1, 1);
    auto& runtime = *env.GetServer().GetRuntime();
    CreateDatabase(env, "Database");

    TTableClient client(env.GetDriver());
    auto session = env.RunInThreadPool([&] {
        return client.CreateSession().GetValueSync().GetSession();
    });

    {
        // Create table
        TString createTable = R"(
            CREATE TABLE `Root/Database/Table` (
                Key Uint64 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            )
        )";

        auto result = env.RunInThreadPool([&] {
            return session.ExecuteSchemeQuery(createTable).GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    // Simulate an ANALYZE error coming from the StatisticsAggregator tablet.
    auto observer = runtime.AddObserver<TEvStatistics::TEvAnalyzeResponse>(
        [&](TEvStatistics::TEvAnalyzeResponse::TPtr& ev) {
        auto& record = ev->Get()->Record;
        record.SetStatus(NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR);
        NYql::TIssue issue("mock issue");
        NYql::IssueToMessage(issue, record.AddIssues());
    });

    {
        // Run ANALYZE and check that the issue is reported.
        auto result = env.RunInThreadPool([&] {
            return session.ExecuteSchemeQuery("ANALYZE `Root/Database/Table`").GetValueSync();
        });
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_C(
            HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR,
                [](const auto& issue) {
                    return issue.GetMessage() == "mock issue";
                }),
            result.GetIssues().ToString());
    }
}

} // suite


} // namespace NKqp
} // namespace NKikimr
