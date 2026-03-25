#pragma once

#include "tx_mode.h"

#include <library/cpp/json/json_value.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/library/accessor/accessor.h>

#include <util/generic/map.h>

namespace NYdb::NConsoleClient::BenchmarkUtils {

using NYdb::NConsoleClient::ETxMode;

// Default number of rows to store per result index for output and comparison
constexpr size_t DefaultMaxRowsPerResultIndex = 100;

struct TTiming {
    TDuration Total = TDuration::Zero(); // timings captured by the client application. these timings include time RTT between server and the client application
    TDuration Server = TDuration::Zero(); // total query timings measured by the server
    TDuration Compilation = TDuration::Zero(); // compitation timing
};

struct TTestInfo {
    TDuration ColdTime;
    TDuration Min;
    TDuration Max;

    TDuration RttMin;
    TDuration RttMax;
    double RttMean = 0;

    TDuration CompilationMin;
    TDuration CompilationMax;
    double CompilationMean = 1;

    double Mean = 0;
    double Median = 0;
    double Std = 0;
    TDuration UnixBench;
    std::vector<TTiming> Timings;

    explicit TTestInfo(std::vector<TTiming>&& timings);
    void operator +=(const TTestInfo& other);
    void operator /=(const ui32 count);
};

// Stores result sets for one result index along with total row count
struct TResultData {
    TVector<NYdb::TResultSet> ResultSets;  // Stored result set parts (limited by MaxRowsPerResultIndex)
    size_t TotalRowsRead = 0;              // Total rows read from all parts (including non-stored)
};

class TQueryBenchmarkResult {
public:
    using TRawResults = TMap<ui64, TResultData>;

private:
    YDB_READONLY_DEF(TString, ErrorInfo);
    YDB_READONLY_DEF(TRawResults, RawResults);
    YDB_ACCESSOR_DEF(TTiming, Timing);
    YDB_READONLY_DEF(TString, QueryPlan);
    YDB_READONLY_DEF(TString, PlanAst);
    YDB_READONLY_DEF(TString, ExecStats);
    YDB_READONLY_DEF(TString, DiffErrors);
    YDB_READONLY_DEF(TString, DiffWarrnings);
    TQueryBenchmarkResult() = default;
public:
    static TQueryBenchmarkResult Result(TRawResults&& rawResults,
        const TTiming& timing, const TString& queryPlan, const TString& planAst,
        const TString& execStats, TStringBuf expected)
    {
        TQueryBenchmarkResult result;
        result.RawResults = std::move(rawResults);
        result.Timing = timing;
        result.QueryPlan = queryPlan;
        result.PlanAst = planAst;
        result.ExecStats = execStats;
        result.CompareWithExpected(expected);
        return result;
    }

    static TQueryBenchmarkResult Error(const TString& error, const TString& queryPlan, const TString& planAst, const TString& execStats) {
        TQueryBenchmarkResult result;
        result.ErrorInfo = error;
        result.QueryPlan = queryPlan;
        result.PlanAst = planAst;
        result.ExecStats = execStats;
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
    ETxMode TxMode = ETxMode::SerializableRW;
    NYdb::NRetry::TRetryOperationSettings RetrySettings;
    // Maximum number of rows to store per result index.
    // Used both for CompareWithExpected and for output.
    // Should be max(DefaultMaxRowsPerResultIndex, lines in expected).
    size_t MaxRowsPerResultIndex = DefaultMaxRowsPerResultIndex;
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
