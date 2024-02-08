#pragma once

#include <library/cpp/json/json_value.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_query/client.h>

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
    std::vector<TDuration> ClientTimings; // timings captured by the client application. these timings include time RTT between server and the client application.
    std::vector<TDuration> ServerTimings; // query timings measured by the server.

    explicit TTestInfo(std::vector<TDuration>&& clientTimings, std::vector<TDuration>&& serverTimings);
};

class TQueryResultInfo {
protected:
    std::vector<std::vector<NYdb::TValue>> Result;
    TVector<NYdb::TColumn> Columns;
public:
    std::map<TString, ui32> GetColumnsRemap() const {
        std::map<TString, ui32> result;
        ui32 idx = 0;
        for (auto&& i : Columns) {
            result.emplace(i.Name, idx++);
        }
        return result;
    }

    const std::vector<std::vector<NYdb::TValue>>& GetResult() const {
        return Result;
    }
    const TVector<NYdb::TColumn>& GetColumns() const {
        return Columns;
    }
};

class TQueryBenchmarkResult {
private:
    TString ErrorInfo;
    TString YSONResult;
    TQueryResultInfo QueryResult;
    TDuration ServerTiming;
    TQueryBenchmarkResult() = default;
public:
    static TQueryBenchmarkResult Result(const TString& yson, const TQueryResultInfo& queryResult, const TDuration& serverTiming) {
        TQueryBenchmarkResult result;
        result.YSONResult = yson;
        result.QueryResult = queryResult;
        result.ServerTiming = serverTiming;
        return result;
    }
    static TQueryBenchmarkResult Error(const TString& error) {
        TQueryBenchmarkResult result;
        result.ErrorInfo = error;
        return result;
    }
    bool operator!() const {
        return !!ErrorInfo;
    }
    const TString& GetErrorInfo() const {
        return ErrorInfo;
    }
    TDuration GetServerTiming() const {
        return ServerTiming;
    }
    const TString& GetYSONResult() const {
        return YSONResult;
    }
    const TQueryResultInfo& GetQueryResult() const {
        return QueryResult;
    }
};

TString FullTablePath(const TString& database, const TString& table);
void ThrowOnError(const TStatus& status);
bool HasCharsInString(const TString& str);
TQueryBenchmarkResult Execute(const TString & query, NTable::TTableClient & client);
TQueryBenchmarkResult Execute(const TString & query, NQuery::TQueryClient & client);
NJson::TJsonValue GetQueryLabels(ui32 queryId);
NJson::TJsonValue GetSensorValue(TStringBuf sensor, TDuration& value, ui32 queryId);
NJson::TJsonValue GetSensorValue(TStringBuf sensor, double value, ui32 queryId);

} // NYdb::NConsoleClient::BenchmarkUtils
