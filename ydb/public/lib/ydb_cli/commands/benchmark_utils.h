#pragma once

#include <library/cpp/json/json_value.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/library/accessor/accessor.h>

#include <util/generic/map.h>
#include <vector>

namespace NYdb::NConsoleClient::BenchmarkUtils {

struct TTestInfo {
    TDuration ColdTime;
    TDuration Min;
    TDuration Max;

    TDuration RttMin;
    TDuration RttMax;
    double RttMean = 0;

    double Mean = 0;
    double Median = 0;
    double Std = 0;
    TDuration UnixBench;
    std::vector<TDuration> ClientTimings; // timings captured by the client application. these timings include time RTT between server and the client application.
    std::vector<TDuration> ServerTimings; // query timings measured by the server.

    explicit TTestInfo(std::vector<TDuration>&& clientTimings, std::vector<TDuration>&& serverTimings);
    void operator +=(const TTestInfo& other);
    void operator /=(const ui32 count);
};

class TQueryBenchmarkResult {
public:
    using TRawResults = TMap<ui64, TVector<NYdb::TResultSet>>;

private:
    YDB_READONLY_DEF(TString, ErrorInfo);
    YDB_READONLY_DEF(TRawResults, RawResults);
    YDB_READONLY_DEF(TDuration, ServerTiming);
    YDB_READONLY_DEF(TString, QueryPlan);
    YDB_READONLY_DEF(TString, PlanAst);
    TQueryBenchmarkResult() = default;
public:
    static TQueryBenchmarkResult Result(TRawResults&& rawResults,
        const TDuration& serverTiming, const TString& queryPlan, const TString& planAst)
    {
        TQueryBenchmarkResult result;
        result.RawResults = std::move(rawResults);
        result.ServerTiming = serverTiming;
        result.QueryPlan = queryPlan;
        result.PlanAst = planAst;
        return result;
    }

    static TQueryBenchmarkResult Error(const TString& error, const TString& queryPlan, const TString& planAst) {
        TQueryBenchmarkResult result;
        result.ErrorInfo = error;
        result.QueryPlan = queryPlan;
        result.PlanAst = planAst;
        return result;
    }

    bool IsExpected(std::string_view expected) const;
    TString CalcHash() const;

    operator bool() const {
        return !ErrorInfo;
    }

private:
    bool IsExpected(TStringBuf expected, size_t resultSetIndex) const;
};

struct TQueryBenchmarkDeadline {
    TInstant Deadline = TInstant::Max();
    TString Name;
};

struct TQueryBenchmarkSettings {
    TQueryBenchmarkDeadline Deadline;
    std::optional<TString> PlanFileName;
    bool WithProgress = false;
};

TString FullTablePath(const TString& database, const TString& table);
bool HasCharsInString(const TString& str);
TQueryBenchmarkResult Execute(const TString & query, NTable::TTableClient & client, const TQueryBenchmarkSettings& settings);
TQueryBenchmarkResult Execute(const TString & query, NQuery::TQueryClient & client, const TQueryBenchmarkSettings& settings);
TQueryBenchmarkResult Explain(const TString & query, NTable::TTableClient & client, const TQueryBenchmarkDeadline& deadline);
TQueryBenchmarkResult Explain(const TString & query, NQuery::TQueryClient & client, const TQueryBenchmarkDeadline& deadline);
NJson::TJsonValue GetQueryLabels(ui32 queryId);
NJson::TJsonValue GetSensorValue(TStringBuf sensor, TDuration& value, ui32 queryId);
NJson::TJsonValue GetSensorValue(TStringBuf sensor, double value, ui32 queryId);

} // NYdb::NConsoleClient::BenchmarkUtils
