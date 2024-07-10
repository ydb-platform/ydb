#include "benchmark_utils.h"

#include <util/string/split.h>
#include <util/stream/file.h>
#include <util/folder/pathsplit.h>
#include <util/folder/path.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/string_utils/csv/csv.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <ydb/public/api/protos/ydb_query.pb.h>

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
    Sort(serverTimingsCopy);
    auto centerElement = serverTimingsCopy.begin() + ServerTimings.size() / 2;
    std::nth_element(serverTimingsCopy.begin(), centerElement, serverTimingsCopy.end());

    if (ServerTimings.size() % 2 == 0) {
        auto maxLessThanCenterElement = std::max_element(serverTimingsCopy.begin(), centerElement);
        Median = (centerElement->MilliSeconds() + maxLessThanCenterElement->MilliSeconds()) / 2.0;
    } else {
        Median = centerElement->MilliSeconds();
    }
    const auto ubCount = std::max<size_t>(1, serverTimingsCopy.size() * 2 / 3);
    double ub = 1;
    for (ui32 i = 0; i < ubCount; ++i) {
        ub *= serverTimingsCopy[i].MillisecondsFloat();
    }
    UnixBench = TDuration::MilliSeconds(pow(ub, 1. / ubCount));
}

void TTestInfo::operator /=(const ui32 count) {
    ColdTime /= count;
    Min /= count;
    Max /= count;
    RttMin /= count;
    RttMax /= count;
    RttMean /= count;
    Mean /= count;
    Median /= count;
    UnixBench /= count;
}

void TTestInfo::operator +=(const TTestInfo& other) {
    ColdTime += other.ColdTime;
    Min += other.Min;
    Max += other.Max;
    RttMin += other.RttMin;
    RttMax += other.RttMax;
    RttMean += other.RttMean;
    Mean += other.Mean;
    Median += other.Median;
    UnixBench += other.UnixBench;
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
    YDB_READONLY_DEF(TString, ErrorInfo);
    YDB_READONLY_DEF(TDuration, ServerTiming);
    YDB_READONLY_DEF(TString, QueryPlan);
    YDB_READONLY_DEF(TString, PlanAst);

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
                    ServerTiming += streamPart.GetQueryStats().GetTotalDuration();
                    QueryPlan = streamPart.GetQueryStats().GetPlan().GetOrElse("");
                    PlanAst = streamPart.GetQueryStats().GetAst().GetOrElse("");
                }
            } else {
                const auto& stats = streamPart.GetStats();
                if (stats) {
                    ServerTiming += stats->GetTotalDuration();
                    QueryPlan = stats->GetPlan().GetOrElse("");
                    PlanAst = stats->GetAst().GetOrElse("");
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
        return TQueryBenchmarkResult::Result(
            scannerYson->GetResult(),
            *scannerCSV,
            composite.GetServerTiming(),
            composite.GetQueryPlan(),
            composite.GetPlanAst()
            );
    }
}

TQueryBenchmarkResult Execute(const TString& query, NQuery::TQueryClient& client) {
    NQuery::TExecuteQuerySettings settings;
    settings.StatsMode(NQuery::EStatsMode::Full);
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
        return TQueryBenchmarkResult::Result(
            scannerYson->GetResult(),
            *scannerCSV,
            composite.GetServerTiming(),
            composite.GetQueryPlan(),
            composite.GetPlanAst()
            );
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

template <class T>
bool CompareValueImpl(const T& valResult, TStringBuf vExpected) {
    T valExpected;
    if (!TryFromString<T>(vExpected, valExpected)) {
        Cerr << "cannot parse expected as " << typeid(valResult).name() << "(" << vExpected << ")" << Endl;
        return false;
    }
    return valResult == valExpected;
}

bool CompareValue(const NYdb::TValue& v, TStringBuf vExpected) {
    const auto& vp = v.GetProto();
    if (vp.has_bool_value()) {
        return CompareValueImpl<bool>(vp.bool_value(), vExpected);
    }
    if (vp.has_int32_value()) {
        return CompareValueImpl<i32>(vp.int32_value(), vExpected);
    }
    if (vp.has_uint32_value()) {
        return CompareValueImpl<ui32>(vp.uint32_value(), vExpected);
    }
    if (vp.has_int64_value()) {
        return CompareValueImpl<i64>(vp.int64_value(), vExpected);
    }
    if (vp.has_uint64_value()) {
        return CompareValueImpl<ui64>(vp.uint64_value(), vExpected);
    }
    if (vp.has_float_value()) {
        return CompareValueImpl<float>(vp.float_value(), vExpected);
    }
    if (vp.has_double_value()) {
        return CompareValueImpl<double>(vp.double_value(), vExpected);
    }
    if (vp.has_text_value()) {
        return CompareValueImpl<TString>(TString(vp.text_value().data(), vp.text_value().size()), vExpected);
    }
    if (vp.has_null_flag_value()) {
        return vExpected == "";
    }
    Cerr << "unexpected type for comparision: " << vp.DebugString() << Endl;
    return false;
}


bool TQueryResultInfo::IsExpected(std::string_view expected) const {
    if (expected.empty()) {
        return true;
    }
    const auto expectedLines = StringSplitter(expected).Split('\n').SkipEmpty().ToList<TString>();
    if (Result.size() + 1 != expectedLines.size()) {
        Cerr << "has diff: incorrect lines count (" << Result.size() << " in result, but " << expectedLines.size() << " expected with header)" << Endl;
        return false;
    }

    std::vector<ui32> columnIndexes;
    {
        const std::map<TString, ui32> columns = GetColumnsRemap();
        auto copy = expectedLines.front();
        NCsvFormat::CsvSplitter splitter(copy);
        while (true) {
            auto cName = splitter.Consume();
            auto it = columns.find(TString(cName.data(), cName.size()));
            if (it == columns.end()) {
                columnIndexes.clear();
                for (ui32 i = 0; i < columns.size(); ++i) {
                    columnIndexes.emplace_back(i);
                }
                break;
            }
            columnIndexes.emplace_back(it->second);

            if (!splitter.Step()) {
                break;
            }
        }
        if (columnIndexes.size() != columns.size()) {
            Cerr << "there are unexpected columns in result" << Endl;
            return false;
        }
    }

    for (ui32 i = 0; i < Result.size(); ++i) {
        TString copy = expectedLines[i + 1];
        NCsvFormat::CsvSplitter splitter(copy);
        bool isCorrectCurrent = true;
        for (ui32 cIdx = 0; cIdx < columnIndexes.size(); ++cIdx) {
            const NYdb::TValue& resultValue = Result[i][columnIndexes[cIdx]];
            if (!isCorrectCurrent) {
                Cerr << "has diff: no element in expectation" << Endl;
                return false;
            }
            TStringBuf cItem = splitter.Consume();
            if (!CompareValue(resultValue, cItem)) {
                Cerr << "has diff: " << resultValue.GetProto().DebugString() << ";EXPECTED:" << cItem << Endl;
                return false;
            }
            isCorrectCurrent = splitter.Step();
        }
        if (isCorrectCurrent) {
            Cerr << "expected more items than have in result" << Endl;
            return false;
        }
    }
    return true;
}

} // NYdb::NConsoleClient::BenchmarkUtils
