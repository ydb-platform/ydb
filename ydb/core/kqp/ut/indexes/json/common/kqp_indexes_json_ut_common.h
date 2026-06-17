#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/indexes/json/common/kqp_indexes_json_corpus.h>
#include <ydb/core/kqp/ut/indexes/json/common/kqp_indexes_json_predicate.h>

#include <ydb/library/json_index/json_index.h>

#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace NKikimr::NKqp {

inline std::string strSuffix(const std::string& s) {
    return std::string("\0\3", 2) + s;
}

inline std::string numSuffix(double v) {
    std::string s;
    s.push_back('\0');
    s.push_back('\4');
    s.append(reinterpret_cast<const char*>(&v), sizeof(double));
    return s;
}

extern const std::string trueSuffix;
extern const std::string falseSuffix;
extern const std::string nullSuffix;

extern const std::string kFirstLongSqlInValue;
extern const std::string kSecondLongSqlInValue;

TKikimrRunner Kikimr(bool enableJsonIndex = true, bool enableJsonIndexAutoSelect = false);

void CreateTestTable(NYdb::NQuery::TQueryClient& db, const std::string& type = "Json", bool withIndex = false);

NYdb::TResultSet ReadIndex(NYdb::NQuery::TQueryClient& db, const char* table = "indexImplTable");

void TestAddJsonIndex(const std::string& type, bool nullable);

void FillTestTable(NYdb::NQuery::TQueryClient& db, const std::string& tableName, const std::string& jsonType);

void ValidatePredicate(NYdb::NQuery::TQueryClient& db, const std::string& predicate,
    NYdb::TParams params = NYdb::TParamsBuilder().Build(), const std::string& suffix = "ORDER BY Key");

void ValidateError(NYdb::NQuery::TQueryClient& db, const std::string& predicate,
    const std::string& errorMessage = "Failed to extract jsonpath tokens from the predicate");

void ValidateError(NYdb::NQuery::TQueryClient& db, const std::string& predicate, NYdb::TParams params,
    const std::string& errorMessage = "Failed to extract jsonpath tokens from the predicate");

void TestSelectJsonWithIndex(const std::string& jsonType, const std::optional<bool>& jsonExistsStrict,
    const std::function<void(NYdb::NQuery::TQueryClient&, const std::function<std::string(const std::string&)>&)>& body,
    bool enableJsonIndexAutoSelect = false);

void FillDataColumn(NYdb::NQuery::TQueryClient& db);

void ValidateTokens(NYdb::NQuery::TQueryClient& db, const std::string& predicate,
    std::vector<NJsonIndex::TToken> expected, NYdb::TParams params,
    const std::string& defaultOperator = "and");

void ValidateTokens(NYdb::NQuery::TQueryClient& db, const std::string& predicate, std::vector<std::string> expected,
    const std::string& defaultOperator = "and");

NYdb::NQuery::TExecuteQueryResult WriteJsonIndexWithKeys(NYdb::NQuery::TQueryClient& db, const std::string& stmt,
    const std::string& tableName, const std::string& jsonType, const std::vector<std::pair<ui64, ui64>>& values,
    bool withReturning = false);

void ValidateAutoSelect(NYdb::NQuery::TQueryClient& db, const std::string& predicate,
    const TString& indexName = "json_idx", const std::string& tableName = "TestTable");

void ValidateNoAutoSelect(NYdb::NQuery::TQueryClient& db, const std::string& predicate,
    const TString& indexName = "json_idx", const std::string& tableName = "TestTable");

void ValidateAutoSelectWithDecl(NYdb::NQuery::TQueryClient& db, const std::string& declares,
    const std::string& predicate, const TString& indexName = "json_idx",
    const std::string& tableName = "TestTable");

void ValidateNoAutoSelectWithDecl(NYdb::NQuery::TQueryClient& db, const std::string& declares,
    const std::string& predicate, const TString& indexName = "json_idx",
    const std::string& tableName = "TestTable");

void TestJsonIndexAlterTableWithIntegerPk(const std::string& pkType);

constexpr ui64 CorpusSeed(ui32 index) noexcept {
    constexpr ui64 base = 0x4A504A4A4A500000ULL;
    return base + static_cast<ui64>(index) * 2u;
}

struct TTestJsonCorpusOptions {
    bool IsJsonDocument = false;
    bool IsStrict = false;
    size_t RowCount = 1000;
    size_t MaxPredicates = 500;
    ui64 Seed = 0xC0DE;
};

void TestJsonCorpus(TTestJsonCorpusOptions tOpts, TPredicateBuilderOptions pOpts);

}  // namespace NKikimr::NKqp
