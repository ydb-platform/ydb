#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <library/cpp/testing/common/env.h>
#include <array>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <regex>

namespace NKikimr::NKqp::NYqlTests {

using namespace NYdb;
using namespace NYdb::NTable;

inline void Replace(std::string& s, const std::string& from, const std::string& to) {
    size_t pos = 0;
    while ((pos = s.find(from, pos)) != std::string::npos) {
        s.replace(pos, from.size(), to);
        pos += to.size();
    }
}

inline TString GetFullPath(const TString& prefix, const TString& filePath) {
    TString fullPath = ArcadiaFromCurrentLocation("ydb/core/kqp/ut/rbo/kqp_rbo_yql_workload_helpers.h", prefix + filePath);

    std::ifstream file(fullPath);

    if (!file.is_open()) {
        throw std::runtime_error("can't open + " + fullPath + " " + std::filesystem::current_path());
    }

    std::stringstream buffer;
    buffer << file.rdbuf();

    return buffer.str();
}

inline void CreateTablesFromPath(NYdb::NTable::TSession session, const TString& pathPrefix, const TString& schemaPath, bool useColumnStore) {
    std::string query = GetFullPath(pathPrefix, schemaPath);
    if (useColumnStore) {
        std::regex pattern(R"(CREATE TABLE [^\(]+ \([^;]*\))", std::regex::multiline);
        query = std::regex_replace(query, pattern, "$& WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16);");
    }

    auto res = session.ExecuteSchemeQuery(TString(query)).GetValueSync();
    res.GetIssues().PrintTo(Cerr);
    UNIT_ASSERT(res.IsSuccess());
}

inline void CreateTablesFromPath(NYdb::NTable::TSession session, const TString& schemaPath, bool useColumnStore) {
    CreateTablesFromPath(session, "../join/data/", schemaPath, useColumnStore);
}

enum EBenchType { TPCH = 0, TPCDS, CLICKBENCH };

static constexpr std::array<const char*, 3> BenchmarkSchemaPathPrefix{R"(data/)", R"(data/)", R"(data/)"};

static constexpr std::array<const char*, 3> BenchmarkSchemaPath{R"(schema/tpch.sql)", R"(schema/tpcds.sql)", R"(schema/clickbench.sql)"};

static constexpr std::array<const char*, 3> BenchmarkQueryPath{R"(data/yql-tpch/q)", R"(data/yql-tpcds/q)", R"(data/yql-clickbench/q)"};

inline NKikimrKqp::TKqpSetting MakeTPCHStatsSetting() {
    NKikimrKqp::TKqpSetting statsSetting;
    statsSetting.SetName("OptOverrideStatistics");
    statsSetting.SetValue(GetFullPath("../join/data/", "stats/tpch1000s.json"));
    return statsSetting;
}

} // namespace NKikimr::NKqp::NYqlTests
