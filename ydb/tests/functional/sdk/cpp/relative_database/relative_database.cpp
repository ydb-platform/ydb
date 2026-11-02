#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/datetime/base.h>

#include <cstdlib>
#include <string>
#include <utility>
#include <vector>

using namespace NYdb;

namespace {

    template <typename TResult>
    TResult Success(NThreading::TFuture<TResult> future) {
        auto result = future.ExtractValue(TDuration::Seconds(30));
        NStatusHelpers::ThrowOnError(result);
        return result;
    }

    void AssertPayload(const TResultSet& resultSet, const std::string& expected) {
        TResultSetParser parser(resultSet);
        ASSERT_TRUE(parser.TryNextRow());
        const auto payload = parser.ColumnParser("Payload").GetOptionalUtf8();
        ASSERT_TRUE(payload.has_value());
        EXPECT_EQ(*payload, expected);
        EXPECT_FALSE(parser.TryNextRow());
    }

    void AssertReadTable(NTable::TSession& session, const std::string& path, const std::string& expected) {
        SCOPED_TRACE(path);
        auto iterator = Success(session.ReadTable(path));
        size_t rows = 0;
        while (true) {
            auto part = iterator.ReadNext().GetValueSync();
            if (part.EOS()) {
                break;
            }
            ASSERT_TRUE(part.IsSuccess()) << part.GetIssues().ToString();
            auto resultSet = part.ExtractPart();
            if (resultSet.RowsCount()) {
                AssertPayload(resultSet, expected);
                rows += resultSet.RowsCount();
            }
        }
        EXPECT_EQ(rows, 1u);
    }

} // namespace

TEST(RelativeDatabase, RepeatedSessionsAndSlashlessResources) {
    const char* endpoint = std::getenv("YDB_ENDPOINT");
    const char* configuredDatabase = std::getenv("YDB_DATABASE");
    ASSERT_NE(endpoint, nullptr);
    ASSERT_NE(configuredDatabase, nullptr);
    const std::string database = configuredDatabase;
    ASSERT_EQ(database, "/Root/mydb");
    const std::string root = "Root";
    const std::string boundary = "Root2";
    const auto makeDriver = [&](const std::string& spelling) {
        return TDriver(TDriverConfig().SetEndpoint(endpoint).SetDatabase(spelling).SetDiscoveryMode(EDiscoveryMode::Sync));
    };

    auto driver = makeDriver(database);
    NScheme::TSchemeClient scheme(driver);
    NTable::TTableClient table(driver);
    auto setupSession = Success(table.CreateSession()).GetSession();
    // A slashless path starting with the cluster root remains relative.
    Success(scheme.MakeDirectory(database + '/' + root));
    Success(scheme.MakeDirectory(database + '/' + root + "/mydb"));
    Success(scheme.MakeDirectory(database + '/' + boundary));
    const std::vector<std::pair<std::string, std::string>> tables{
        {database + "/Config", "target"},
        {database + '/' + database.substr(1) + "/Config", "repeated-root"},
        {database + '/' + boundary + "/Config", "root-boundary"},
    };
    for (const auto& [path, payload] : tables) {
        Success(setupSession.CreateTable(path, table.GetTableBuilder()
                                                   .AddNullableColumn("Id", EPrimitiveType::Uint64)
                                                   .AddNullableColumn("Payload", EPrimitiveType::Utf8)
                                                   .SetPrimaryKeyColumn("Id")
                                                   .Build()));
        TValueBuilder rows;
        rows.BeginList().AddListItem().BeginStruct().AddMember("Id").Uint64(1).AddMember("Payload").Utf8(payload).EndStruct().EndList();
        Success(table.BulkUpsert(path, rows.Build()));
    }

    for (const auto& spelling : {database, std::string("mydb")}) {
        SCOPED_TRACE(spelling);
        auto spellingDriver = makeDriver(spelling);
        NScheme::TSchemeClient spellingScheme(spellingDriver);
        Success(spellingScheme.ListDirectory(root));
        NTable::TTableClient spellingTable(spellingDriver);
        for (size_t iteration = 0; iteration < 3; ++iteration) {
            auto session = Success(spellingTable.CreateSession()).GetSession();
            for (const auto& path : {database + "/Config", std::string("Config")}) {
                AssertReadTable(session, path, "target");
            }
            AssertReadTable(session, database.substr(1) + "/Config", "repeated-root");
            AssertReadTable(session, boundary + "/Config", "root-boundary");
            const auto data = Success(session.ExecuteDataQuery("SELECT Payload FROM Config;",
                                                               NTable::TTxControl::BeginTx().CommitTx()));
            AssertPayload(data.GetResultSet(0), "target");
            Success(session.Close());

            NQuery::TQueryClient query(spellingDriver);
            auto querySession = Success(query.GetSession()).GetSession();
            const auto result = Success(querySession.ExecuteQuery("SELECT Payload FROM Config;",
                                                                  NQuery::TTxControl::BeginTx().CommitTx()));
            AssertPayload(result.GetResultSet(0), "target");
        }
    }

    for (const auto& item : tables) {
        Success(setupSession.DropTable(item.first));
    }
    Success(scheme.RemoveDirectory(database + '/' + root + "/mydb"));
    Success(scheme.RemoveDirectory(database + '/' + root));
    Success(scheme.RemoveDirectory(database + '/' + boundary));
    Success(setupSession.Close());
}
