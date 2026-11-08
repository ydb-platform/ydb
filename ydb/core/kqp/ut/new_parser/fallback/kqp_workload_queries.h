#pragma once

#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_workload_helpers.h>

namespace NKikimr::NKqp::NYqlTests {

static constexpr std::array<ui32, 3> BenchmarkQueryCount{22, 99, 43};

static constexpr std::array<ui32, 13> TpchPerfQueryIds{1, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 19};

inline TString LoadBenchmarkQuery(EBenchType type, ui32 queryId) {
    const TString toDecimal = R"($to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };)";
    const TString toDecimalMax = R"($to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };)";
    const TString round = R"($round = ($x,$y) -> {return $x;};)";
    return toDecimal + "\n" + toDecimalMax + "\n" + round + "\n"
        + GetFullPath(BenchmarkQueryPath[type], ToString(queryId) + ".yql");
}

inline TString PrepareTpchPerfQuery(std::string query, const std::string& constants) {
    const std::string tablePrefix = "/Root/";
    Replace(query, "{path}", tablePrefix);
    Replace(query, "{% include 'header.sql.jinja' %}", R"(PRAGMA YqlSelect = 'force';)");
    const std::regex pattern(R"(\{\{\s*([a-zA-Z0-9_]+)\s*\}\})");
    query = std::regex_replace(query, pattern, "`" + tablePrefix + "$1`");
    return constants + "\n" + query;
}

} // namespace NKikimr::NKqp::NYqlTests
