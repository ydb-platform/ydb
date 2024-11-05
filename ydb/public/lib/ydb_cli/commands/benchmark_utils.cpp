#include "benchmark_utils.h"

#include <util/string/split.h>
#include <util/string/builder.h>
#include <util/stream/file.h>
#include <util/folder/pathsplit.h>
#include <util/folder/path.h>

#include <library/cpp/colorizer/colors.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/string_utils/csv/csv.h>
#include <library/cpp/digest/md5/md5.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <ydb/public/api/protos/ydb_query.pb.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>

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


TMaybe<TQueryBenchmarkResult> ResultByStatus(const TStatus& status, const TString& deadlineName) {
    if (status.IsSuccess()) {
        return Nothing();
    }
    TStringBuilder errorInfo;
    switch (status.GetStatus()) {
        case NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED:
            errorInfo << deadlineName << " deadline expiried: " << status.GetIssues();
            break;
        default:
            errorInfo << "Operation failed with status " << status.GetStatus() << ": " << status.GetIssues().ToString();
            break;
    }
    return TQueryBenchmarkResult::Error(errorInfo, "", "");
}

bool HasCharsInString(const TString& str) {
    for(TStringBuf q(str), line; q.ReadLine(line);) {
        line = line.NextTok("--");
        for (auto c: line) {
            if (std::isalpha(c)) {
                return true;
            }
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
    YDB_ACCESSOR_DEF(TString, DeadlineName);
    TQueryBenchmarkResult::TRawResults RawResults;
public:
    virtual ~IQueryResultScanner() = default;
    TQueryBenchmarkResult::TRawResults&& ExtractRawResults() {
        return std::move(RawResults);
    }
    virtual void OnStart(const TVector<NYdb::TColumn>& columns) = 0;
    virtual void OnBeforeRow() = 0;
    virtual void OnAfterRow() = 0;
    virtual void OnRowItem(const NYdb::TColumn& c, const NYdb::TValue& value) = 0;
    virtual void OnFinish() = 0;
    void OnError(const NYdb::EStatus status, const TString& info) {
        switch (status) {
            case NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED:
                ErrorInfo = TStringBuilder() << DeadlineName << " deadline expiried: " << info;
                break;
            default:
                ErrorInfo = info;
                break;
        }
    }

    template <typename TIterator>
    bool Scan(TIterator& it) {
        for (;;) {
            auto streamPart = it.ReadNext().GetValueSync();

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

            if (!streamPart.IsSuccess()) {
                if (!streamPart.EOS()) {
                    OnError(streamPart.GetStatus(), streamPart.GetIssues().ToString());
                    return false;
                }
                break;
            }

            if (streamPart.HasResultSet()) {
                RawResults.emplace_back(streamPart.ExtractResultSet());
                auto columns = RawResults.back().GetColumnsMeta();

                OnStart(columns);
                NYdb::TResultSetParser parser(RawResults.back());
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

template<class TSettings>
TMaybe<TQueryBenchmarkResult> SetTimeoutSettings(TSettings& settings, const TQueryBenchmarkDeadline& deadline) {
    if (deadline.Deadline != TInstant::Max()) {
        auto now = Now();
        if (now >= deadline.Deadline) {
            return TQueryBenchmarkResult::Error(deadline.Name + " deadline expiried", "", "");
        }
        settings.ClientTimeout(deadline.Deadline - now);
    }
    return Nothing();
}

TQueryBenchmarkResult ExecuteImpl(const TString& query, NTable::TTableClient& client, const TQueryBenchmarkDeadline& deadline, bool explainOnly) {
    TStreamExecScanQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Full);
    settings.Explain(explainOnly);
    if (const auto error = SetTimeoutSettings(settings, deadline)) {
        return *error;
    }
    auto it = client.StreamExecuteScanQuery(query, settings).GetValueSync();
    if (const auto error = ResultByStatus(it, deadline.Name)) {
        return *error;
    }

    std::shared_ptr<TCSVResultScanner> scannerCSV = std::make_shared<TCSVResultScanner>();
    TQueryResultScannerComposite composite;
    composite.SetDeadlineName(deadline.Name);
    composite.AddScanner(scannerCSV);
    if (!composite.Scan(it)) {
        return TQueryBenchmarkResult::Error(
            composite.GetErrorInfo(), composite.GetQueryPlan(), composite.GetPlanAst());
    } else {
        return TQueryBenchmarkResult::Result(
            composite.ExtractRawResults(),
            *scannerCSV,
            composite.GetServerTiming(),
            composite.GetQueryPlan(),
            composite.GetPlanAst()
            );
    }
}

TQueryBenchmarkResult Execute(const TString& query, NTable::TTableClient& client, const TQueryBenchmarkDeadline& deadline) {
    return ExecuteImpl(query, client, deadline, false);
}

TQueryBenchmarkResult Explain(const TString& query, NTable::TTableClient& client, const TQueryBenchmarkDeadline& deadline) {
    return ExecuteImpl(query, client, deadline, true);
}

TQueryBenchmarkResult ExecuteImpl(const TString& query, NQuery::TQueryClient& client, const TQueryBenchmarkDeadline& deadline, bool explainOnly) {
    NQuery::TExecuteQuerySettings settings;
    settings.StatsMode(NQuery::EStatsMode::Full);
    settings.ExecMode(explainOnly ? NQuery::EExecMode::Explain : NQuery::EExecMode::Execute);
    if (auto error = SetTimeoutSettings(settings, deadline)) {
        return *error;
    }
    auto it = client.StreamExecuteQuery(
        query,
        NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
        settings).GetValueSync();
    if (auto error = ResultByStatus(it, deadline.Name)) {
        return *error;
    }

    std::shared_ptr<TCSVResultScanner> scannerCSV = std::make_shared<TCSVResultScanner>();
    TQueryResultScannerComposite composite;
    composite.SetDeadlineName(deadline.Name);
    composite.AddScanner(scannerCSV);
    if (!composite.Scan(it)) {
        return TQueryBenchmarkResult::Error(
            composite.GetErrorInfo(), composite.GetQueryPlan(), composite.GetPlanAst());
    } else {
        return TQueryBenchmarkResult::Result(
            composite.ExtractRawResults(),
            *scannerCSV,
            composite.GetServerTiming(),
            composite.GetQueryPlan(),
            composite.GetPlanAst()
            );
    }
}

TQueryBenchmarkResult Execute(const TString& query, NQuery::TQueryClient& client, const TQueryBenchmarkDeadline& deadline) {
    return ExecuteImpl(query, client, deadline, false);
}

TQueryBenchmarkResult Explain(const TString& query, NQuery::TQueryClient& client, const TQueryBenchmarkDeadline& deadline) {
    return ExecuteImpl(query, client, deadline, true);
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

template <class T>
bool CompareValueImplFloat(const T& valResult, TStringBuf vExpected) {
    T relativeFloatPrecision = 0.0001;
    TStringBuf precesionStr;
    vExpected.Split("+-", vExpected, precesionStr);
    T valExpected;
    if (!TryFromString<T>(vExpected, valExpected)) {
        Cerr << "cannot parse expected as " << typeid(valResult).name() << "(" << vExpected << ")" << Endl;
        return false;
    }
    if (precesionStr.ChopSuffix("%")) {
        if (!TryFromString<T>(precesionStr, relativeFloatPrecision)) {
            Cerr << "cannot parse precision expected as " << typeid(valResult).name() << "(" << precesionStr << "%)" << Endl;
            return false;
        }
        relativeFloatPrecision /= 100;
    } else if (precesionStr) {
        T absolutePrecesion;
        if (!TryFromString<T>(precesionStr, absolutePrecesion)) {
            Cerr << "cannot parse precision expected as " << typeid(valResult).name() << "(" << precesionStr << ")" << Endl;
            return false;
        }
        return valResult >= valExpected - absolutePrecesion && valResult <= valExpected + absolutePrecesion;
    }

    return valResult > (1 - relativeFloatPrecision) * valExpected && valResult < (1 + relativeFloatPrecision) * valExpected;
}

template <>
bool CompareValueImpl<float>(const float& valResult, TStringBuf vExpected) {
    return CompareValueImplFloat(valResult, vExpected);
}

template <>
bool CompareValueImpl<double>(const double& valResult, TStringBuf vExpected) {
    return CompareValueImplFloat(valResult, vExpected);
}

bool CompareValueImplDecimal(const NYdb::TDecimalValue& valResult, TStringBuf vExpected) {
    auto resInt = NYql::NDecimal::FromHalfs(valResult.Low_, valResult.Hi_);
    TStringBuf precesionStr;
    vExpected.Split("+-", vExpected, precesionStr);
    auto expectedInt = NYql::NDecimal::FromString(vExpected, valResult.DecimalType_.Precision, valResult.DecimalType_.Scale);

    if (precesionStr) {
        auto precInt = NYql::NDecimal::FromString(precesionStr, valResult.DecimalType_.Precision, valResult.DecimalType_.Scale);
        return resInt >= expectedInt - precInt && resInt <= expectedInt + precInt;
    }
    const auto from = NYql::NDecimal::FromString("0.9999", valResult.DecimalType_.Precision, valResult.DecimalType_.Scale);
    const auto to = NYql::NDecimal::FromString("1.0001", valResult.DecimalType_.Precision, valResult.DecimalType_.Scale);
    const auto devider = NYql::NDecimal::GetDivider(valResult.DecimalType_.Scale);
    return resInt > NYql::NDecimal::MulAndDivNormalDivider(from, expectedInt, devider) && resInt < NYql::NDecimal::MulAndDivNormalDivider(to, expectedInt, devider);
}

bool CompareValueImplDatetime(const TInstant& valResult, TStringBuf vExpected, TDuration unit) {
    TInstant expected;
    if (!TInstant::TryParseIso8601(vExpected, expected)) {
        i64 i;
        if (!TryFromString(vExpected, i)) {
            Cerr << "cannot parse expected as " << typeid(valResult).name() << "(" << vExpected << ")" << Endl;
            return false;
        }
        expected = TInstant::Zero() + i * unit;
    }
    return valResult == expected;
}

template<class T>
bool CompareValueImplDatetime64(const T& valResult, TStringBuf vExpected, TDuration unit) {
    T valExpected;
    if (!TryFromString<T>(vExpected, valExpected)) {
        TInstant expected;
        if (!TInstant::TryParseIso8601(vExpected, expected)) {
            Cerr << "cannot parse expected as " << typeid(valResult).name() << "(" << vExpected << ")" << Endl;
            return false;
        }
        valExpected = expected.GetValue() / unit.GetValue();
    }
    return valResult == valExpected;
}

template<class T>
bool CompareValuePgImpl(const NYdb::TPgValue& v, TStringBuf vExpected) {
    if (v.IsText()) {
        T value;
        if (!TryFromString(v.Content_, value)) {
            Cerr << "cannot parse value as " << typeid(value).name() << "(" << v.Content_ << ")" << Endl;
            return false;
        }
        return CompareValueImpl(value, vExpected);
    }
    const T* value = reinterpret_cast<const T*>(v.Content_.data());
    return CompareValueImpl(*value, vExpected);
}

bool CompareValuePg(const NYdb::TPgValue& v, TStringBuf vExpected) {
    if (v.IsNull()) {
        return vExpected == "";
    }
    if (v.PgType_.TypeName == "pgint2") {
        return CompareValuePgImpl<i16>(v, vExpected);
    }
    if (v.PgType_.TypeName == "pgint4") {
        return CompareValuePgImpl<i32>(v, vExpected);
    }
    if (v.PgType_.TypeName == "pgint8") {
        return CompareValuePgImpl<i64>(v, vExpected);
    }
    if (v.PgType_.TypeName == "pgfloat4") {
        return CompareValuePgImpl<float>(v, vExpected);
    }
    if (v.PgType_.TypeName == "pgfloat8") {
        return CompareValuePgImpl<double>(v, vExpected);
    }
    if (IsIn({"pgbytea", "pgtext"}, v.PgType_.TypeName)) {
        return vExpected == v.Content_;
    }
    Cerr << "Unsupported pg type: typename=" << v.PgType_.TypeName
        << "; type_mod=" << v.PgType_.TypeModifier
        << Endl;
    return false;
}

bool CompareValuePrimitive(const NYdb::TValue& v, const TValueParser& vp, const TTypeParser& tp, TStringBuf vExpected) {
    switch (tp.GetPrimitive()) {
    case EPrimitiveType::Bool:
        return CompareValueImpl(vp.GetBool(), vExpected);
    case EPrimitiveType::Int8:
        return CompareValueImpl(vp.GetInt8(), vExpected);
    case EPrimitiveType::Uint8:
        return CompareValueImpl(vp.GetUint8(), vExpected);
    case EPrimitiveType::Int16:
        return CompareValueImpl(vp.GetInt16(), vExpected);
    case EPrimitiveType::Uint16:
        return CompareValueImpl(vp.GetUint16(), vExpected);
    case EPrimitiveType::Int32:
        return CompareValueImpl(vp.GetInt32(), vExpected);
    case EPrimitiveType::Uint32:
        return CompareValueImpl(vp.GetUint32(), vExpected);
    case EPrimitiveType::Int64:
        return CompareValueImpl(vp.GetInt64(), vExpected);
    case EPrimitiveType::Uint64:
        return CompareValueImpl(vp.GetUint64(), vExpected);
    case EPrimitiveType::Float:
        return CompareValueImpl(vp.GetFloat(), vExpected);
    case EPrimitiveType::Double:
        return CompareValueImpl(vp.GetDouble(), vExpected);
    case EPrimitiveType::Date:
        return CompareValueImplDatetime(vp.GetDate(), vExpected, TDuration::Days(1));
    case EPrimitiveType::Datetime:
        return CompareValueImplDatetime(vp.GetDatetime(), vExpected, TDuration::Seconds(1));
    case EPrimitiveType::Timestamp:
        return CompareValueImplDatetime(vp.GetTimestamp(), vExpected, TDuration::MicroSeconds(1));
    case EPrimitiveType::Interval:
        return CompareValueImpl(vp.GetInterval(), vExpected);
    case EPrimitiveType::Date32:
        return CompareValueImplDatetime64(vp.GetDate32(), vExpected, TDuration::Days(1));
    case EPrimitiveType::Datetime64:
        return CompareValueImplDatetime64(vp.GetDatetime64(), vExpected, TDuration::Seconds(1));
    case EPrimitiveType::Timestamp64:
        return CompareValueImplDatetime64(vp.GetTimestamp64(), vExpected, TDuration::MicroSeconds(1));
    case EPrimitiveType::Interval64:
        return CompareValueImpl(vp.GetInterval64(), vExpected);
    case EPrimitiveType::String:
        return CompareValueImpl(vp.GetString(), vExpected);
    case EPrimitiveType::Utf8:
        return CompareValueImpl(vp.GetUtf8(), vExpected);
    default:
        Cerr << "unexpected type for comparision: " << v.GetProto().DebugString() << Endl;
        return false;
    }
}

bool CompareValue(const NYdb::TValue& v, TStringBuf vExpected) {
    TValueParser vp(v);
    TTypeParser tp(v.GetType());
    while (tp.GetKind() == TTypeParser::ETypeKind::Optional) {
        if (vp.IsNull()) {
            return vExpected == "";
        }
        vp.OpenOptional();
        tp.OpenOptional();
    }
    switch (tp.GetKind()) {
    case TTypeParser::ETypeKind::Decimal:
        return  CompareValueImplDecimal(vp.GetDecimal(), vExpected);
    case TTypeParser::ETypeKind::Primitive:
        return CompareValuePrimitive(v, vp, tp, vExpected);
    case TTypeParser::ETypeKind::Pg:
        return CompareValuePg(vp.GetPg(), vExpected);
    default:
        Cerr  << "Unsupported value type kind: " << tp.GetKind() << Endl;
        return false;
    }
}

TQueryResultInfo::TColumnsRemap TQueryResultInfo::GetColumnsRemap() const {
    TColumnsRemap result;
    ui32 idx = 0;
    for (auto&& i : Columns) {
        result.emplace(i.Name, idx++);
    }
    return result;
}

TString TQueryResultInfo::CalcHash() const {
    MD5 hasher;
    for (const auto& row: Result) {
        for (const auto& v: row) {
            hasher.Update(FormatValueYson(v, NYson::EYsonFormat::Binary));
        }
    }
    char buf[25];
    return hasher.End_b64(buf);
}

bool TQueryResultInfo::IsExpected(std::string_view expected) const {
    if (expected.empty()) {
        return true;
    }
    auto expectedLines = StringSplitter(expected).Split('\n').SkipEmpty().ToList<TString>();
    if (!expectedLines.empty() && expectedLines.back() == "...") {
        expectedLines.pop_back();
        if (Result.size() + 1 < expectedLines.size()) {
            Cerr << "has diff: too samll lines count (" << Result.size() << " in result, but " << expectedLines.size() << "+ expected with header)" << Endl;
            return false;
        }
    } else if (Result.size() + 1 != expectedLines.size()) {
        Cerr << "has diff: incorrect lines count (" << Result.size() << " in result, but " << expectedLines.size() << " expected with header)" << Endl;
        return false;
    }

    std::vector<ui32> columnIndexes;
    {
        const auto columns = GetColumnsRemap();
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
    TVector<TVector<TString>> diffs;
    for (ui32 i = 0; i < expectedLines.size() - 1; ++i) {
        TString copy = expectedLines[i + 1];
        NCsvFormat::CsvSplitter splitter(copy);
        bool isCorrectCurrent = true;
        TVector<TString> lineDiff(columnIndexes.size() + 1, ToString(i));
        bool hasDiff = false;
        for (ui32 cIdx = 0; cIdx < columnIndexes.size(); ++cIdx) {
            const NYdb::TValue& resultValue = Result[i][columnIndexes[cIdx]];
            if (!isCorrectCurrent) {
                Cerr << "has diff: no element in expectation" << Endl;
                return false;
            }
            TStringBuf expectedValue = splitter.Consume();
            const TString resultStr = FormatValueYson(resultValue);
            if (CompareValue(resultValue, expectedValue)) {
                lineDiff[cIdx + 1] = resultStr;
            } else {
                auto& colors = NColorizer::StdErr();
                lineDiff[cIdx + 1] = TStringBuilder()
                    << colors.Red()  << resultStr << colors.Reset()
                    << " (" << colors.Green()  << expectedValue << colors.Reset() << ")";
                hasDiff = true;
            }
            isCorrectCurrent = splitter.Step();
        }
        if (isCorrectCurrent) {
            Cerr << "expected more columns than have in result" << Endl;
            return false;
        }
        if (hasDiff) {
            diffs.emplace_back(std::move(lineDiff));
        }
    }
    if (!diffs.empty()) {
        TVector<TString> tableColums {"Line"};
        tableColums.reserve(columnIndexes.size() + 1);
        for (const auto& col: Columns) {
            tableColums.emplace_back(col.Name);
        }
        TPrettyTable table(tableColums);
        for (const auto& diffLine: diffs) {
            auto& row = table.AddRow();
            for (ui32 i = 0; i < diffLine.size(); ++i) {
                row.Column(i, diffLine[i]);
            }
        }
        Cerr << "There is diff in results: " << Endl;
        table.Print(Cerr);
        return false;
    }
    return true;
}

} // NYdb::NConsoleClient::BenchmarkUtils
