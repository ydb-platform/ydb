#pragma once

#include <library/cpp/json/json_value.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/library/accessor/accessor.h>

#include <util/generic/map.h>

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
    YDB_READONLY_DEF(TString, DiffErrors);
    YDB_READONLY_DEF(TString, DiffWarrnings);
    TQueryBenchmarkResult() = default;
public:
    static TQueryBenchmarkResult Result(TRawResults&& rawResults,
        const TDuration& serverTiming, const TString& queryPlan, const TString& planAst, TStringBuf expected)
    {
        TQueryBenchmarkResult result;
        result.RawResults = std::move(rawResults);
        result.ServerTiming = serverTiming;
        result.QueryPlan = queryPlan;
        result.PlanAst = planAst;
        result.CompareWithExpected(expected);
        return result;
    }

    static TQueryBenchmarkResult Error(const TString& error, const TString& queryPlan, const TString& planAst) {
        TQueryBenchmarkResult result;
        result.ErrorInfo = error;
        result.QueryPlan = queryPlan;
        result.PlanAst = planAst;
        return result;
    }

    TString CalcHash() const;

    operator bool() const {
        return !ErrorInfo;
    }

private:
    void CompareWithExpected(TStringBuf expected);
    void CompareWithExpected(TStringBuf expected, size_t resultSetIndex);
};

struct TQueryBenchmarkDeadline {
    TInstant Deadline = TInstant::Max();
    TString Name;
};

struct TQueryBenchmarkSettings {
    TQueryBenchmarkDeadline Deadline;
    std::optional<TString> PlanFileName;
    bool WithProgress = false;
    NYdb::NRetry::TRetryOperationSettings RetrySettings;
};

TString FullTablePath(const TString& database, const TString& table);
bool HasCharsInString(const TString& str);
TQueryBenchmarkResult Execute(const TString& query, TStringBuf expected, NQuery::TQueryClient & client, const TQueryBenchmarkSettings& settings);
TQueryBenchmarkResult Explain(const TString& query, NQuery::TQueryClient & client, const TQueryBenchmarkSettings& settings);
NJson::TJsonValue GetQueryLabels(TStringBuf queryId);
NJson::TJsonValue GetSensorValue(TStringBuf sensor, TDuration& value, TStringBuf queryId);
NJson::TJsonValue GetSensorValue(TStringBuf sensor, double value, TStringBuf queryId);
size_t GetBenchmarkTableWidth();

} // NYdb::NConsoleClient::BenchmarkUtils
