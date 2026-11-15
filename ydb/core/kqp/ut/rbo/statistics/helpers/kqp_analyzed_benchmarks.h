#pragma once

#include <library/cpp/json/json_value.h>

#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

namespace NKikimr::NKqp {

// Owns the server, driver and session for a generated and fully analyzed dataset.
// Keep this object alive while using its session or runtime.
class TAnalyzedBenchmark {
public:
    TAnalyzedBenchmark(const TString& workload, double scale, bool columnStore);

    // Switch compilation pipelines and invalidate cached plans on every node.
    void SetEnableNewRbo(bool enabled);

    NYdb::NQuery::TSession& GetSession() { return Session; }
    TTestActorRuntime& GetRuntime() { return *Env.GetServer().GetRuntime(); }
    const TString& GetTablesPath() const { return TablesPath; }
    TString GetTablePath(const TString& table) const { return TablesPath + "/" + table; }
    ui64 GetRowCount(const TString& table) const { return RowCounts.at(table); }
    ui64 GetByteSize(const TString& table) const {
        return BaseStatistics.GetMapSafe().at(GetTablePath(table)).GetMapSafe().at("byte_size").GetUIntegerSafe();
    }

private:
    // Must precede Env so mismatched snapshots fail before any actors start.
    NJson::TJsonValue BaseStatistics;
    NStat::TTestEnv Env;
    TString Database;
    TString TablesPath;
    NYdb::TDriver Driver;
    NYdb::NQuery::TQueryClient QueryClient;
    NYdb::NQuery::TSession Session;
    THashMap<TString, ui64> RowCounts;
};

std::unique_ptr<TAnalyzedBenchmark> LoadTpchAndAnalyze(double scale, bool columnStore = false);
std::unique_ptr<TAnalyzedBenchmark> LoadTpcdsAndAnalyze(double scale, bool columnStore = false);

} // namespace NKikimr::NKqp
