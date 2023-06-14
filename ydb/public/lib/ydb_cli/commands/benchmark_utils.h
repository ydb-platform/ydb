#pragma once

#include <library/cpp/json/json_value.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <vector>

namespace NYdb::NConsoleClient::BenchmarkUtils {

struct TTestInfo {
    TDuration ColdTime;
    TDuration Min;
    TDuration Max;
    double Mean = 0;
    double Std = 0;
    std::vector<TDuration> Timings;

    explicit TTestInfo(std::vector<TDuration>&& timings);
};

class TQueryBenchmarkResult {
private:
    TString ErrorInfo;
    TString YSONResult;
    TString CSVResult;
    TQueryBenchmarkResult() = default;
public:
    static TQueryBenchmarkResult Result(const TString& yson, const TString& csv) {
        TQueryBenchmarkResult result;
        result.YSONResult = yson;
        result.CSVResult = csv;
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
    const TString& GetYSONResult() const {
        return YSONResult;
    }
    const TString& GetCSVResult() const {
        return CSVResult;
    }
};

TString FullTablePath(const TString& database, const TString& table);
void ThrowOnError(const TStatus& status);
bool HasCharsInString(const TString& str);
TQueryBenchmarkResult Execute(const TString & query, NTable::TTableClient & client);
NJson::TJsonValue GetQueryLabels(ui32 queryId);
NJson::TJsonValue GetSensorValue(TStringBuf sensor, TDuration& value, ui32 queryId);
NJson::TJsonValue GetSensorValue(TStringBuf sensor, double value, ui32 queryId);

} // NYdb::NConsoleClient::BenchmarkUtils
