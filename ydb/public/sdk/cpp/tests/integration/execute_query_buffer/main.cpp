#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <cstdlib>
#include <string>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

std::string GetEnvOrEmpty(const char* name) {
    const char* value = std::getenv(name);
    return value ? std::string(value) : std::string();
}

TDriver MakeDriver() {
    return TDriver(
        TDriverConfig()
            .SetEndpoint(GetEnvOrEmpty("YDB_ENDPOINT"))
            .SetDatabase(GetEnvOrEmpty("YDB_DATABASE"))
            .SetAuthToken(GetEnvOrEmpty("YDB_TOKEN")));
}

int32_t SumColumnV(const TExecuteQueryResult& result, const std::string& columnName = "v") {
    int32_t sum = 0;
    TResultSetParser parser(result.GetResultSet(0));
    while (parser.TryNextRow()) {
        sum += static_cast<int32_t>(parser.ColumnParser(columnName).GetInt32());
    }
    return sum;
}

uint64_t CountRows(const TExecuteQueryResult& result) {
    uint64_t count = 0;
    TResultSetParser parser(result.GetResultSet(0));
    while (parser.TryNextRow()) {
        ++count;
    }
    return count;
}

} // namespace

TEST(ExecuteQueryBufferIntegration, SinglePartBufferedExecuteQuery) {
    TQueryClient client(MakeDriver());

    const auto result = client.ExecuteQuery(
        "SELECT 1 AS v;",
        TTxControl::NoTx()).GetValueSync();

    ASSERT_TRUE(result.IsSuccess()) << result.GetIssues().ToString();
    ASSERT_EQ(result.GetResultSets().size(), 1u);
    ASSERT_EQ(SumColumnV(result), 1);
}

TEST(ExecuteQueryBufferIntegration, MultiPartBufferedExecuteQuery) {
    TQueryClient client(MakeDriver());

    const auto settings = TExecuteQuerySettings().OutputChunkMaxSize(100);
    const auto result = client.ExecuteQuery(
        "SELECT * FROM AS_TABLE(ListReplicate(AsStruct(1 AS v), 1000));",
        TTxControl::NoTx(),
        settings).GetValueSync();

    ASSERT_TRUE(result.IsSuccess()) << result.GetIssues().ToString();
    ASSERT_EQ(result.GetResultSets().size(), 1u);
    ASSERT_EQ(CountRows(result), 1000u);
    ASSERT_EQ(SumColumnV(result), 1000);
}

TEST(ExecuteQueryBufferIntegration, StatsOnlyBufferedExecuteQuery) {
    TQueryClient client(MakeDriver());

    const auto settings = TExecuteQuerySettings().StatsMode(EStatsMode::Basic);
    const auto result = client.ExecuteQuery(
        "SELECT 1 AS v;",
        TTxControl::NoTx(),
        settings).GetValueSync();

    ASSERT_TRUE(result.IsSuccess()) << result.GetIssues().ToString();
    ASSERT_TRUE(result.GetStats().has_value());
}
