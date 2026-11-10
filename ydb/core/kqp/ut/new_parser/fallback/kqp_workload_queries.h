#pragma once

#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_workload_helpers.h>

namespace NKikimr::NKqp::NYqlTests {

// Keep frontend-specific query enumeration here, leaving the original RBO
// workload helpers and tests unchanged. Enum order matches EBenchType; query
// files are read from the existing fixture tree rather than copied or rewritten.
static constexpr std::array<ui32, 3> BenchmarkQueryCount{22, 99, 43};

// The performance corpus contains a selected TPCH subset with its own template
// inputs. Use its explicit IDs rather than assuming all benchmark queries exist.
static constexpr std::array<ui32, 13> TpchPerfQueryIds{1, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 19};

// Retain the helper declarations used by the original workload runner, even
// for queries that do not reference them. These are compatibility inputs: an
// unsupported preamble is allowed to exercise frontend fallback in this suite.
inline TString LoadBenchmarkQuery(EBenchType type, ui32 queryId) {
    const TString toDecimal = R"($to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };)";
    const TString toDecimalMax = R"($to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };)";
    const TString round = R"($round = ($x,$y) -> {return $x;};)";
    return toDecimal + "\n" + toDecimalMax + "\n" + round + "\n"
        + GetFullPath(BenchmarkQueryPath[type], ToString(queryId) + ".yql");
}

// Expand the same table placeholders and header as the original performance
// fixture, then prepend its constants. This adapts fixture templates to a test
// database; it must not simplify SQL constructs to make native parsing succeed.
inline TString PrepareTpchPerfQuery(std::string query, const std::string& constants) {
    const std::string tablePrefix = "/Root/";
    Replace(query, "{path}", tablePrefix);
    Replace(query, "{% include 'header.sql.jinja' %}", R"(PRAGMA YqlSelect = 'force';)");
    const std::regex pattern(R"(\{\{\s*([a-zA-Z0-9_]+)\s*\}\})");
    query = std::regex_replace(query, pattern, "`" + tablePrefix + "$1`");
    return constants + "\n" + query;
}

} // namespace NKikimr::NKqp::NYqlTests
