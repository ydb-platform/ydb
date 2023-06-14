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

class IQueryResultScanner {
private:
    TString ErrorInfo;
public:
    virtual ~IQueryResultScanner() = default;
    virtual void OnStart(const TVector<NYdb::TColumn>& columns) = 0;
    virtual void OnBeforeRow() = 0;
    virtual void OnAfterRow() = 0;
    virtual void OnRowItem(const NYdb::TColumn& c, const NYdb::TValue& value) = 0;
    virtual void OnFinish() = 0;
    void OnError(const TString& info) {
        ErrorInfo = info;
    }
    const TString& GetErrorInfo() const {
        return ErrorInfo;
    }

    bool Scan(NTable::TScanQueryPartIterator& it) {
        for (;;) {
            auto streamPart = it.ReadNext().GetValueSync();
            if (!streamPart.IsSuccess()) {
                if (!streamPart.EOS()) {
                    OnError(streamPart.GetIssues().ToString());
                    return false;
                }
                break;
            }

            if (streamPart.HasResultSet()) {
                auto result = streamPart.ExtractResultSet();
                auto columns = result.GetColumnsMeta();

                OnStart(columns);
                NYdb::TResultSetParser parser(result);
                while (parser.TryNextRow()) {
                    OnBeforeRow();
                    for (ui32 i = 0; i < columns.size(); ++i) {
                        OnRowItem(columns[i], parser.GetValue(i));
                    }
                    OnAfterRow();
                }
                OnFinish();
            }
        }
        return true;
    }
};

class TQueryResultScannerComposite: public IQueryResultScanner {
private:
    std::vector<std::shared_ptr<IQueryResultScanner>> Scanners;
public:
    void AddScanner(std::shared_ptr<IQueryResultScanner> scanner) {
        Scanners.emplace_back(scanner);
    }

    virtual void OnStart(const TVector<NYdb::TColumn>& columns) override {
        for (auto&& i : Scanners) {
            i->OnStart(columns);
        }
    }
    virtual void OnBeforeRow() override {
        for (auto&& i : Scanners) {
            i->OnBeforeRow();
        }
    }
    virtual void OnAfterRow() override {
        for (auto&& i : Scanners) {
            i->OnAfterRow();
        }
    }
    virtual void OnRowItem(const NYdb::TColumn& c, const NYdb::TValue& value) override {
        for (auto&& i : Scanners) {
            i->OnRowItem(c, value);
        }
    }
    virtual void OnFinish() override {
        for (auto&& i : Scanners) {
            i->OnFinish();
        }
    }
};

class TYSONResultScanner: public IQueryResultScanner {
private:
    TStringStream ResultString;
    mutable std::unique_ptr<NYson::TYsonWriter> Writer;
public:
    TYSONResultScanner() {
    }
    TString GetResult() const {
        Writer.reset();
        return ResultString.Str();
    }
    virtual void OnStart(const TVector<NYdb::TColumn>& /*columns*/) override {
        Writer = std::make_unique<NYson::TYsonWriter>(&ResultString, NYson::EYsonFormat::Text, ::NYson::EYsonType::Node, true);
        Writer->OnBeginList();
    }
    virtual void OnBeforeRow() override {
        Writer->OnListItem();
        Writer->OnBeginList();
    }
    virtual void OnAfterRow() override {
        Writer->OnEndList();
    }
    virtual void OnRowItem(const NYdb::TColumn& /*c*/, const NYdb::TValue& value) override {
        Writer->OnListItem();
        FormatValueYson(value, *Writer);
    }
    virtual void OnFinish() override {
        Writer->OnEndList();
    }
};

class TCSVResultScanner: public IQueryResultScanner, public TQueryResultInfo {
public:
    TCSVResultScanner() {
    }
    virtual void OnStart(const TVector<NYdb::TColumn>& columns) override {
        Columns = columns;
    }
    virtual void OnBeforeRow() override {
        Result.emplace_back(std::vector<NYdb::TValue>());
    }
    virtual void OnAfterRow() override {
    }
    virtual void OnRowItem(const NYdb::TColumn& /*c*/, const NYdb::TValue& value) override {
        Result.back().emplace_back(value);
    }
    virtual void OnFinish() override {
    }
};

TQueryBenchmarkResult Execute(const TString& query, NTable::TTableClient& client) {
    TStreamExecScanQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Full);
    auto it = client.StreamExecuteScanQuery(query, settings).GetValueSync();
    ThrowOnError(it);

    std::shared_ptr<TYSONResultScanner> scannerYson = std::make_shared<TYSONResultScanner>();
    std::shared_ptr<TCSVResultScanner> scannerCSV = std::make_shared<TCSVResultScanner>();
    TQueryResultScannerComposite composite;
    composite.AddScanner(scannerYson);
    composite.AddScanner(scannerCSV);
    if (!composite.Scan(it)) {
        return TQueryBenchmarkResult::Error(composite.GetErrorInfo());
    } else {
        return TQueryBenchmarkResult::Result(scannerYson->GetResult(), *scannerCSV);
    }
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
