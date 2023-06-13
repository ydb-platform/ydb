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

TString FullTablePath(const TString& database, const TString& table);
void ThrowOnError(const TStatus& status);
bool HasCharsInString(const TString& str);
std::pair<TString, TString> ResultToYson(NYdb::NTable::TScanQueryPartIterator& it);
std::pair<TString, TString> Execute(const TString& query, NTable::TTableClient& client);
NJson::TJsonValue GetQueryLabels(ui32 queryId);
NJson::TJsonValue GetSensorValue(TStringBuf sensor, TDuration& value, ui32 queryId);
NJson::TJsonValue GetSensorValue(TStringBuf sensor, double value, ui32 queryId);

} // NYdb::NConsoleClient::BenchmarkUtils