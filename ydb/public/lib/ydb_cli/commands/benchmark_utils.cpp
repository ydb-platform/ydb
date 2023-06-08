#include "benchmark_utils.h"

#include <util/string/split.h>
#include <util/stream/file.h>
#include <util/folder/pathsplit.h>
#include <util/folder/path.h>

#include <library/cpp/json/json_writer.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <vector>

namespace NYdb::NConsoleClient::BenchmarkUtils {

using namespace NYdb;
using namespace NYdb::NTable;

TTestInfo::TTestInfo(std::vector<TDuration>&& timings)
    : Timings(std::move(timings))
{

    if (Timings.empty()) {
        return;
    }

    ColdTime = Timings[0];

    if (Timings.size() > 1) {
        ui32 sum = 0;
        for (size_t j = 1; j < Timings.size(); ++j) {
            if (Max < Timings[j]) {
                Max = Timings[j];
            }
            if (!Min || Min > Timings[j]) {
                Min = Timings[j];
            }
            sum += Timings[j].MilliSeconds();
        }
        Mean = (double) sum / (double) (Timings.size() - 1);
        if (Timings.size() > 2) {
            double variance = 0;
            for (size_t j = 1; j < Timings.size(); ++j) {
                variance += (Mean - Timings[j].MilliSeconds()) * (Mean - Timings[j].MilliSeconds());
            }
            variance = variance / (double) (Timings.size() - 2);
            Std = sqrt(variance);
        }
    }
}

TString FullTablePath(const TString& database, const TString& table) {
    TPathSplitUnix prefixPathSplit(database);
    prefixPathSplit.AppendComponent(table);
    return prefixPathSplit.Reconstruct();
}


void ThrowOnError(const TStatus& status) {
    if (!status.IsSuccess()) {
        ythrow yexception() << "Operation failed with status " << status.GetStatus() << ": "
                            << status.GetIssues().ToString();
    }
}

bool HasCharsInString(const TString& str) {
    for (auto c : str) {
        if (std::isalpha(c)) {
            return true;
        }
    }
    return false;
}

std::pair<TString, TString> ResultToYson(NTable::TScanQueryPartIterator& it) {
    TStringStream out;
    TStringStream err_out;
    NYson::TYsonWriter writer(&out, NYson::EYsonFormat::Text, ::NYson::EYsonType::Node, true);
    writer.OnBeginList();

    for (;;) {
        auto streamPart = it.ReadNext().GetValueSync();
        if (!streamPart.IsSuccess()) {
            if (!streamPart.EOS()) {
                err_out << streamPart.GetIssues().ToString() << Endl;
            }
            break;
        }

        if (streamPart.HasResultSet()) {
            auto result = streamPart.ExtractResultSet();
            auto columns = result.GetColumnsMeta();

            NYdb::TResultSetParser parser(result);
            while (parser.TryNextRow()) {
                writer.OnListItem();
                writer.OnBeginList();
                for (ui32 i = 0; i < columns.size(); ++i) {
                    writer.OnListItem();
                    FormatValueYson(parser.GetValue(i), writer);
                }
                writer.OnEndList();
                out << "\n";
            }
        }
    }

    writer.OnEndList();
    return {out.Str(), err_out.Str()};
}

std::pair<TString, TString> Execute(const TString& query, NTable::TTableClient& client) {
    TStreamExecScanQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Full);
    auto it = client.StreamExecuteScanQuery(query, settings).GetValueSync();
    ThrowOnError(it);
    return ResultToYson(it);
}

NJson::TJsonValue GetQueryLabels(ui32 queryId) {
    NJson::TJsonValue labels(NJson::JSON_MAP);
    labels.InsertValue("query", Sprintf("Query%02u", queryId));
    return labels;
}

NJson::TJsonValue GetSensorValue(TStringBuf sensor, TDuration& value, ui32 queryId) {
    NJson::TJsonValue sensorValue(NJson::JSON_MAP);
    sensorValue.InsertValue("sensor", sensor);
    sensorValue.InsertValue("value", value.MilliSeconds());
    sensorValue.InsertValue("labels", GetQueryLabels(queryId));
    return sensorValue;
}

NJson::TJsonValue GetSensorValue(TStringBuf sensor, double value, ui32 queryId) {
    NJson::TJsonValue sensorValue(NJson::JSON_MAP);
    sensorValue.InsertValue("sensor", sensor);
    sensorValue.InsertValue("value", value);
    sensorValue.InsertValue("labels", GetQueryLabels(queryId));
    return sensorValue;
}

} // NYdb::NConsoleClient::BenchmarkUtils