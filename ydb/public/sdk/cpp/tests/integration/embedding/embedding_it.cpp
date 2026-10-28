#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/embedding.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/params/params.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstdlib>
#include <format>
#include <string>
#include <vector>

namespace NYdb {
namespace {

void CheckEmbedding(NQuery::TQueryClient& client, const std::string& type,
    const TValue& values, const TValue& embedding) {
    SCOPED_TRACE(type);

    const auto query = std::format(R"(
        DECLARE $values AS List<{}>;
        DECLARE $embedding AS Bytes;
        SELECT $embedding = Untag(Knn::ToBinaryStringFloat(
            ListMap($values, ($value) -> (CAST($value AS Float)))
        ), "FloatVector");
    )", type);
    const auto params = TParamsBuilder()
        .AddParam("$values", values)
        .AddParam("$embedding", embedding)
        .Build();

    auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx(), params).ExtractValueSync();
    ASSERT_TRUE(result.IsSuccess()) << result.GetIssues().ToString();
    auto parser = result.GetResultSetParser(0);
    ASSERT_TRUE(parser.TryNextRow());
    EXPECT_TRUE(parser.ColumnParser(0).GetBool());
}

} // namespace

TEST(Embedding, MatchesKnnSerialization) {
    TDriver driver(TDriverConfig()
        .SetEndpoint(std::getenv("YDB_ENDPOINT"))
        .SetDatabase(std::getenv("YDB_DATABASE")));
    NQuery::TQueryClient client(driver);

    CheckEmbedding(client, "Float",
        TValueBuilder().EmptyList(TTypeBuilder().Primitive(EPrimitiveType::Float).Build()).Build(),
        NValueHelpers::Embedding(std::vector<float>{}));
    CheckEmbedding(client, "Int64",
        TValueBuilder().BeginList().AddListItem().Int64(-2).AddListItem().Int64(16777217).EndList().Build(),
        NValueHelpers::Embedding(std::array<std::int64_t, 2>{-2, 16777217}));
    CheckEmbedding(client, "Uint16",
        TValueBuilder().BeginList().AddListItem().Uint16(2).EndList().Build(),
        NValueHelpers::Embedding(std::array<std::uint16_t, 1>{2}));
    CheckEmbedding(client, "Float",
        TValueBuilder().BeginList().AddListItem().Float(-2.5f).AddListItem().Float(1.25f).EndList().Build(),
        NValueHelpers::Embedding(std::vector<float>{-2.5f, 1.25f}));
    CheckEmbedding(client, "Double",
        TValueBuilder().BeginList().AddListItem().Double(-2.5).AddListItem().Double(1.00000001).EndList().Build(),
        NValueHelpers::Embedding(std::vector<double>{-2.5, 1.00000001}));
}

} // namespace NYdb
