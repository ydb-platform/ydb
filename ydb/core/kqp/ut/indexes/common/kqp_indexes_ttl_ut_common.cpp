#include <ydb/core/kqp/ut/indexes/common/kqp_indexes_ttl_ut_common.h>

#include <format>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

constexpr const char* AlterSetTtlQuery = R"(
    ALTER TABLE TestTable SET (TTL = Interval("PT1H") ON Stamp);
)";

void ExecuteQueryExpectSuccess(TQueryClient& db, const std::string& query) {
    auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void ExecuteQueryExpectFailure(TQueryClient& db, const std::string& query, const std::string& expectedError) {
    auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), expectedError);
}

std::string MakeCreateTable(const TTtlNotAllowedIndexTestConfig& config, bool withIndex, const char* ttlInterval = nullptr) {
    std::string query = std::format(
        "CREATE TABLE TestTable (\n"
        "    Key Uint64,\n"
        "    Text {},\n"
        "    Stamp Timestamp,\n",
        config.TextColumnType);
    if (withIndex) {
        query += "    ";
        query += config.IndexInCreateTable;
        query += "\n";
    }
    query += "    PRIMARY KEY (Key)\n";
    if (ttlInterval) {
        query += std::format(") WITH (\n    TTL = Interval(\"{}\") ON Stamp\n);\n", ttlInterval);
    } else {
        query += ");\n";
    }
    return query;
}

} // namespace

void TestTtlNotAllowedBoth(TQueryClient db, const TTtlNotAllowedIndexTestConfig& config) {
    ExecuteQueryExpectFailure(db, MakeCreateTable(config, true, "PT1H"), config.ExpectedError);
}

void TestTtlNotAllowedAlterTtl(TQueryClient db, const TTtlNotAllowedIndexTestConfig& config) {
    ExecuteQueryExpectSuccess(db, MakeCreateTable(config, true));
    ExecuteQueryExpectFailure(db, AlterSetTtlQuery, config.ExpectedError);
}

void TestTtlNotAllowedAlterIndex(TQueryClient db, const TTtlNotAllowedIndexTestConfig& config) {
    ExecuteQueryExpectSuccess(db, MakeCreateTable(config, false, "PT0S"));
    ExecuteQueryExpectFailure(db, config.AlterAddIndex, config.ExpectedError);
}

void TestTtlNotAllowedAlterTtlIndex(TQueryClient db, const TTtlNotAllowedIndexTestConfig& config) {
    ExecuteQueryExpectSuccess(db, MakeCreateTable(config, false));
    ExecuteQueryExpectSuccess(db, AlterSetTtlQuery);
    ExecuteQueryExpectFailure(db, config.AlterAddIndex, config.ExpectedError);
}

void TestTtlNotAllowedAlterIndexTtl(TQueryClient db, const TTtlNotAllowedIndexTestConfig& config) {
    ExecuteQueryExpectSuccess(db, MakeCreateTable(config, false));
    ExecuteQueryExpectSuccess(db, config.AlterAddIndex);
    ExecuteQueryExpectFailure(db, AlterSetTtlQuery, config.ExpectedError);
}

} // namespace NKikimr::NKqp
