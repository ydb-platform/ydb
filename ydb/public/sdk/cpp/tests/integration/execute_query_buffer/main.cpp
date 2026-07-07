#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

constexpr std::string_view MultiPartQuery = "SELECT * FROM AS_TABLE(ListReplicate(AsStruct(1 AS v), 1000));";

struct TRunArgs {
    TDriver Driver;
};

TRunArgs GetRunArgs() {
    const char* endpoint = std::getenv("YDB_ENDPOINT");
    const char* database = std::getenv("YDB_DATABASE");

    auto driverConfig = TDriverConfig()
        .SetEndpoint(endpoint)
        .SetDatabase(database)
        .SetAuthToken(std::getenv("YDB_TOKEN") ? std::getenv("YDB_TOKEN") : "");

    TDriver driver(driverConfig);

    return {std::move(driver)};
}

int32_t SumColumnV(const TExecuteQueryResult& result) {
    int32_t sum = 0;
    TResultSetParser parser(result.GetResultSet(0));
    while (parser.TryNextRow()) {
        sum += static_cast<int32_t>(parser.ColumnParser("v").GetInt32());
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

size_t CountResultSetStreamParts(TQueryClient& client, const TExecuteQuerySettings& settings) {
    size_t parts = 0;
    auto iterator = client.StreamExecuteQuery(
        std::string(MultiPartQuery),
        TTxControl::NoTx(),
        settings).GetValueSync();

    EXPECT_TRUE(iterator.IsSuccess()) << iterator.GetIssues().ToString();

    while (true) {
        auto part = iterator.ReadNext().GetValueSync();
        if (part.EOS()) {
            break;
        }

        EXPECT_TRUE(part.IsSuccess()) << part.GetIssues().ToString();
        if (part.HasResultSet()) {
            ++parts;
        }
    }

    return parts;
}

} // namespace

TEST(ExecuteQueryBuffer, SinglePartBufferedExecuteQuery) {
    ASSERT_NE(std::getenv("YDB_ENDPOINT"), nullptr);
    ASSERT_NE(std::getenv("YDB_DATABASE"), nullptr);

    auto [driver] = GetRunArgs();

    TQueryClient client(driver);

    const auto result = client.ExecuteQuery(
        "SELECT 1 AS v;",
        TTxControl::NoTx()).GetValueSync();

    ASSERT_TRUE(result.IsSuccess()) << result.GetIssues().ToString();
    ASSERT_EQ(result.GetResultSets().size(), 1u);
    ASSERT_EQ(SumColumnV(result), 1);

    driver.Stop(true);
}

TEST(ExecuteQueryBuffer, MultiPartBufferedExecuteQuery) {
    ASSERT_NE(std::getenv("YDB_ENDPOINT"), nullptr);
    ASSERT_NE(std::getenv("YDB_DATABASE"), nullptr);

    auto [driver] = GetRunArgs();

    TQueryClient client(driver);

    const auto settings = TExecuteQuerySettings().OutputChunkMaxSize(100);
    ASSERT_GT(CountResultSetStreamParts(client, settings), 1u);

    const auto result = client.ExecuteQuery(
        std::string(MultiPartQuery),
        TTxControl::NoTx(),
        settings).GetValueSync();

    ASSERT_TRUE(result.IsSuccess()) << result.GetIssues().ToString();
    ASSERT_EQ(result.GetResultSets().size(), 1u);
    ASSERT_EQ(CountRows(result), 1000u);
    ASSERT_EQ(SumColumnV(result), 1000);

    driver.Stop(true);
}

TEST(ExecuteQueryBuffer, SinglePartBufferedExecuteQueryWithStats) {
    ASSERT_NE(std::getenv("YDB_ENDPOINT"), nullptr);
    ASSERT_NE(std::getenv("YDB_DATABASE"), nullptr);

    auto [driver] = GetRunArgs();

    TQueryClient client(driver);

    const auto settings = TExecuteQuerySettings().StatsMode(EStatsMode::Basic);
    const auto result = client.ExecuteQuery(
        "SELECT 1 AS v;",
        TTxControl::NoTx(),
        settings).GetValueSync();

    ASSERT_TRUE(result.IsSuccess()) << result.GetIssues().ToString();
    ASSERT_TRUE(result.GetStats().has_value());

    driver.Stop(true);
}
