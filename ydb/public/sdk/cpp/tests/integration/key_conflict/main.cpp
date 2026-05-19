#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/issue/yql_issue.h>
#include <library/cpp/testing/gtest/gtest.h>
#include <util/string/cast.h>
#include <format>

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NStatusHelpers;

namespace {

struct TRunArgs {
    TDriver Driver;
    std::string TablePath;
};

TRunArgs GetRunArgs() {
    const char* endpoint = std::getenv("YDB_ENDPOINT");
    const char* database = std::getenv("YDB_DATABASE");
    const char* testRoot = std::getenv("YDB_TEST_ROOT");

    auto driverConfig = TDriverConfig()
        .SetEndpoint(endpoint)
        .SetDatabase(database)
        .SetAuthToken(std::getenv("YDB_TOKEN") ? std::getenv("YDB_TOKEN") : "");

    TDriver driver(driverConfig);
    std::string tablePath = std::string(database) + "/" + testRoot + "/pk_conflict_it";

    return {std::move(driver), std::move(tablePath)};
}

void DropTableIfExists(TTableClient& client, const std::string& tablePath) {
    (void)client.RetryOperationSync([&tablePath](TSession session) {
        TStatus st = session.DropTable(tablePath).ExtractValueSync();
        if (st.IsSuccess()) {
            return st;
        }
        if (st.GetStatus() == EStatus::SCHEME_ERROR) {
            return TStatus(EStatus::SUCCESS, NYdb::NIssue::TIssues());
        }
        return st;
    });
}

TStatus CreatePkTable(TTableClient& client, const std::string& tablePath) {
    return client.RetryOperationSync([&tablePath](TSession session) {
        auto desc = TTableBuilder()
            .AddNonNullableColumn("id", EPrimitiveType::Uint64)
            .AddNullableColumn("payload", EPrimitiveType::Utf8)
            .SetPrimaryKeyColumn("id")
            .Build();

        return session.CreateTable(tablePath, std::move(desc)).ExtractValueSync();
    });
}

TStatus InsertRow(TSession& session, const std::string& tablePath, uint64_t id, const std::string& payload) {
    const size_t slash = tablePath.rfind('/');
    const std::string parent = tablePath.substr(0, slash);
    const std::string tableName = tablePath.substr(slash + 1);

    const auto q = std::format(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("{}");
        DECLARE $id AS Uint64;
        DECLARE $payload AS Utf8;
        INSERT INTO {} (id, payload) VALUES ($id, $payload);
    )", parent, tableName);

    auto params = TParamsBuilder()
        .AddParam("$id")
            .Uint64(id)
            .Build()
        .AddParam("$payload")
            .Utf8(payload)
            .Build()
        .Build();

    return session.ExecuteDataQuery(
        q,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        std::move(params)).ExtractValueSync();
}

} // namespace

TEST(KeyConflict, DuplicatePrimaryKeyInsertIsDetectableViaIssueCode) {
    ASSERT_NE(std::getenv("YDB_ENDPOINT"), nullptr);
    ASSERT_NE(std::getenv("YDB_DATABASE"), nullptr);
    ASSERT_NE(std::getenv("YDB_TEST_ROOT"), nullptr);

    auto [driver, tablePath] = GetRunArgs();

    TTableClient client(driver);

    DropTableIfExists(client, tablePath);

    TStatus created = CreatePkTable(client, tablePath);
    ASSERT_TRUE(created.IsSuccess()) << ToString(created);

    TStatus firstInsert = client.RetryOperationSync([&](TSession session) {
        return InsertRow(session, tablePath, 1, "first");
    });
    ASSERT_TRUE(firstInsert.IsSuccess()) << ToString(firstInsert);

    TStatus secondInsert = client.RetryOperationSync([&](TSession session) {
        return InsertRow(session, tablePath, 1, "second");
    });
    ASSERT_FALSE(secondInsert.IsSuccess()) << "duplicate PK insert must fail, got: " << ToString(secondInsert);

    ASSERT_TRUE(StatusContainsIssueWithCode(secondInsert, NIssue::CONSTRAINT_VIOLATION))
        << "expected constraint violation issue, got: " << ToString(secondInsert);

    DropTableIfExists(client, tablePath);
    driver.Stop(true);
}