#include "benchmark_utils.h"

#include <util/string/split.h>
#include <util/stream/file.h>
#include <util/folder/pathsplit.h>
#include <util/folder/path.h>

#include <library/cpp/json/json_writer.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <vector>
#include <algorithm>

namespace NYdb::NConsoleClient::BenchmarkUtils {

using namespace NYdb;
using namespace NYdb::NTable;

TTestInfo::TTestInfo(std::vector<TDuration>&& clientTimings, std::vector<TDuration>&& serverTimings)
    : ClientTimings(std::move(clientTimings))
    , ServerTimings(std::move(serverTimings))
{

    if (ClientTimings.empty()) {
        return;
    }

    Y_ABORT_UNLESS(ClientTimings.size() == ServerTimings.size());

    ColdTime = ServerTimings[0];

    {
        ui32 sum = 0;
        for (const auto& timing : ServerTimings) {
            if (Max < timing) {
                Max = timing;
            }
            if (!Min || Min > timing) {
                Min = timing;
            }
            sum += timing.MilliSeconds();
        }

        Mean = static_cast<double>(sum) / static_cast<double>(ServerTimings.size());
        double variance = 0;
        for (const auto& timing : ServerTimings) {
            double diff = (Mean - timing.MilliSeconds());
            variance += diff * diff;
        }
        variance = variance / static_cast<double>(ServerTimings.size());
        Std = sqrt(variance);
    }

    double totalDiff = 0;
    for(size_t idx = 0; idx < ServerTimings.size(); ++idx) {
        TDuration diff = ClientTimings[idx] - ServerTimings[idx];
        totalDiff += diff.MilliSeconds();
        if (idx == 0 || diff < RttMin) {
            RttMin = diff;
        }

        if (idx == 0 || diff > RttMax) {
            RttMax = diff;
        }
    }

    RttMean = totalDiff / static_cast<double>(ServerTimings.size());

    auto serverTimingsCopy = ServerTimings;
    auto centerElement = serverTimingsCopy.begin() + ServerTimings.size() / 2;
    std::nth_element(serverTimingsCopy.begin(), centerElement, serverTimingsCopy.end());

    if (ServerTimings.size() % 2 == 0) {
        auto maxLessThanCenterElement = std::max_element(serverTimingsCopy.begin(), centerElement);
        Median = (centerElement->MilliSeconds() + maxLessThanCenterElement->MilliSeconds()) / 2.0;
    } else {
        Median = centerElement->MilliSeconds();
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
    TDuration ServerTiming;
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
    TDuration GetServerTiming() const {
        return ServerTiming;
    }

    template <typename TIterator>
    bool Scan(TIterator& it) {
        for (;;) {
            auto streamPart = it.ReadNext().GetValueSync();
            if (!streamPart.IsSuccess()) {
                if (!streamPart.EOS()) {
                    OnError(streamPart.GetIssues().ToString());
                    return false;
                }
                break;
            }

            if constexpr (std::is_same_v<TIterator, NTable::TScanQueryPartIterator>) {
                if (streamPart.HasQueryStats()) {
                    ServerTiming = streamPart.GetQueryStats().GetTotalDuration();
                }
            } else {
                const auto& stats = streamPart.GetStats();
                if (stats) {
                    ServerTiming = stats->GetTotalDuration();
                }
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
    settings.CollectQueryStats(ECollectQueryStatsMode::Basic);
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
        return TQueryBenchmarkResult::Result(scannerYson->GetResult(), *scannerCSV, composite.GetServerTiming());
    }
}

TQueryBenchmarkResult Execute(const TString& query, NQuery::TQueryClient& client) {
    NQuery::TExecuteQuerySettings settings;
    settings.StatsMode(NQuery::EStatsMode::Basic);
    auto it = client.StreamExecuteQuery(
        query,
        NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
        settings).GetValueSync();
    ThrowOnError(it);

    std::shared_ptr<TYSONResultScanner> scannerYson = std::make_shared<TYSONResultScanner>();
    std::shared_ptr<TCSVResultScanner> scannerCSV = std::make_shared<TCSVResultScanner>();
    TQueryResultScannerComposite composite;
    composite.AddScanner(scannerYson);
    composite.AddScanner(scannerCSV);
    if (!composite.Scan(it)) {
        return TQueryBenchmarkResult::Error(composite.GetErrorInfo());
    } else {
        return TQueryBenchmarkResult::Result(scannerYson->GetResult(), *scannerCSV, composite.GetServerTiming());
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
