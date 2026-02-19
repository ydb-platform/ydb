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

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <ydb/public/lib/ydb_cli/common/formats.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/plan2svg.h>
#include <ydb/public/lib/ydb_cli/common/progress_indication.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <ydb/public/api/protos/ydb_query.pb.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <util/string/join.h>

#include <vector>
#include <algorithm>

namespace NYdb::NConsoleClient::BenchmarkUtils {

using namespace NYdb;

NQuery::TTxControl GetTxControl(ETxMode txMode) {
    if (txMode == ETxMode::NoTx) {
        return NQuery::TTxControl::NoTx();
    }

    NQuery::TTxSettings txSettings;
    switch (txMode) {
    case ETxMode::SerializableRW:
        txSettings = NQuery::TTxSettings::SerializableRW();
        break;
    case ETxMode::OnlineRO:
        txSettings = NQuery::TTxSettings::OnlineRO();
        break;
    case ETxMode::StaleRO:
        txSettings = NQuery::TTxSettings::StaleRO();
        break;
    case ETxMode::SnapshotRO:
        txSettings = NQuery::TTxSettings::SnapshotRO();
        break;
    case ETxMode::SnapshotRW:
        txSettings = NQuery::TTxSettings::SnapshotRW();
        break;
    default:
        Y_UNREACHABLE();
    }
    return NQuery::TTxControl::BeginTx(txSettings).CommitTx();
}

TTestInfo::TTestInfo(std::vector<TTiming>&& timings)
    : Timings(std::move(timings))
{

    if (Timings.empty()) {
        return;
    }

    ColdTime = Timings.front().Server;
    std::vector<TDuration> serverTimingsCopy;
    serverTimingsCopy.reserve(Timings.size());
    double totalServer = 0;
    double totalRtt = 0;
    double totalCompilation = 0;
    Min = RttMin = CompilationMin = TDuration::Max();
    for (const auto& timing : Timings) {
        Max = std::max(Max, timing.Server);
        Min = std::min(Min, timing.Server);
        totalServer += timing.Server.MillisecondsFloat();

        TDuration rtt = timing.Total - timing.Server; 
        RttMax = std::max(RttMax, rtt);
        RttMin = std::min(RttMin, rtt);
        totalRtt += rtt.MillisecondsFloat();

        CompilationMax = std::max(CompilationMax, timing.Compilation);
        CompilationMin = std::min(CompilationMin, timing.Compilation);
        totalCompilation += timing.Compilation.MillisecondsFloat();

        serverTimingsCopy.emplace_back(timing.Server);
    }
    Mean = totalServer / Timings.size();
    RttMean = totalRtt / Timings.size();
    CompilationMean = totalCompilation / Timings.size();

    double variance = 0;
    for (const auto& timing : Timings) {
        double diff = (Mean - timing.Server.MilliSeconds());
        variance += diff * diff;
    }
    variance = variance / Timings.size();
    Std = sqrt(variance);

    Sort(serverTimingsCopy);
    auto centerElement = serverTimingsCopy.begin() + Timings.size() / 2;
    std::nth_element(serverTimingsCopy.begin(), centerElement, serverTimingsCopy.end());

    if (Timings.size() % 2 == 0) {
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
    CompilationMin /= count;
    CompilationMax /= count;
    CompilationMean /= count;
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
    CompilationMin += other.CompilationMin;
    CompilationMax += other.CompilationMax;
    CompilationMean += other.CompilationMean;
}

TString FullTablePath(const TString& database, const TString& table) {
    TPathSplitUnix prefixPathSplit(database);
    prefixPathSplit.AppendComponent(table);
    return prefixPathSplit.Reconstruct();
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

class TQueryResultScanner {
private:
    YDB_READONLY_DEF(TString, ErrorInfo);
    YDB_READONLY_DEF(TTiming, Timing);
    YDB_READONLY_DEF(TString, QueryPlan);
    YDB_READONLY_DEF(TString, PlanAst);
    YDB_ACCESSOR_DEF(TString, DeadlineName);
    YDB_ACCESSOR_DEF(TString, ExecStats);
    TQueryBenchmarkResult::TRawResults RawResults;
    // Limit rows stored per result index
    size_t MaxRowsPerResultIndex = DefaultMaxRowsPerResultIndex;
public:
    void SetMaxRowsPerResultIndex(size_t maxRows) {
        MaxRowsPerResultIndex = maxRows;
    }

    TQueryBenchmarkResult::TRawResults&& ExtractRawResults() {
        return std::move(RawResults);
    }
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

    TStatus Scan(NQuery::TExecuteQueryIterator& it, std::optional<TString> planFileName = std::nullopt) {

        TProgressIndication progressIndication;
        TMaybe<NQuery::TExecStats> execStats;

        TString currentPlanFileNameStats;
        TString currentPlanWithStatsFileName;
        TString currentPlanWithStatsFileNameJson;
        if (planFileName) {
            currentPlanFileNameStats = TStringBuilder() << *planFileName << ".stats";
            currentPlanWithStatsFileName = TStringBuilder() << *planFileName << ".svg";
            currentPlanWithStatsFileNameJson = TStringBuilder() << *planFileName << ".json";
        }
        for (;;) {
            auto streamPart = it.ReadNext().ExtractValueSync();
            ui64 rsIndex = 0;

            if (streamPart.HasStats()) {
                execStats = streamPart.ExtractStats();

                if (planFileName) {
                    TFileOutput out(currentPlanFileNameStats);
                    out << execStats->ToString();
                    {
                        auto plan = execStats->GetPlan();
                        if (plan) {
                            {
                                TPlanVisualizer pv;
                                TFileOutput out(currentPlanWithStatsFileName);
                                try {
                                    pv.LoadPlans(TString(*execStats->GetPlan()));
                                    out << pv.PrintSvg();
                                } catch (std::exception& e) {
                                    out << "<svg width='1024' height='256' xmlns='http://www.w3.org/2000/svg'><text>" << e.what() << "<text></svg>";
                                }
                            }
                            {
                                TFileOutput out(currentPlanWithStatsFileNameJson);
                                TQueryPlanPrinter queryPlanPrinter(EDataFormat::JsonBase64, true, out, 120);
                                queryPlanPrinter.Print(*execStats->GetPlan());
                            }
                        }
                    }
                }

                const auto& protoStats = TProtoAccessor::GetProto(execStats.GetRef());
                for (const auto& queryPhase : protoStats.query_phases()) {
                    for (const auto& tableAccessStats : queryPhase.table_access()) {
                        progressIndication.UpdateProgress({tableAccessStats.reads().rows(), tableAccessStats.reads().bytes()});
                    }
                }
                progressIndication.SetDurationUs(protoStats.total_duration_us());

                progressIndication.Render();
            }

            rsIndex = streamPart.GetResultSetIndex();

            if (!streamPart.IsSuccess()) {
                if (!streamPart.EOS()) {
                    OnError(streamPart.GetStatus(), streamPart.GetIssues().ToString());
                    return streamPart;
                }
                break;
            }

            if (streamPart.HasResultSet()) {
                auto resultSet = streamPart.ExtractResultSet();
                size_t rowsInThisPart = resultSet.RowsCount();

                // Track total rows read and store only first MaxRowsPerResultIndex rows
                auto& resultData = RawResults[rsIndex];

                // Store result set if we haven't reached the limit yet
                if (resultData.TotalRowsRead < MaxRowsPerResultIndex) {
                    resultData.ResultSets.emplace_back(std::move(resultSet));
                }

                resultData.TotalRowsRead += rowsInThisPart;
            }
        }
        if (execStats) {
            Timing.Server += execStats->GetTotalDuration();
            QueryPlan = execStats->GetPlan().value_or("");
            PlanAst = execStats->GetAst().value_or("");
            ExecStats = execStats->ToString();
            const auto& protoStats = TProtoAccessor::GetProto(execStats.GetRef());
            Timing.Compilation += TDuration::MicroSeconds(protoStats.Getcompilation().Getduration_us());
        }
        return TStatus(EStatus::SUCCESS, NIssue::TIssues());
    }
};

TQueryBenchmarkResult  ConstructResultByStatus(const TStatus& status, const THolder<TQueryResultScanner>& scaner, TStringBuf expected, const TQueryBenchmarkSettings& becnhmarkSettings) {
    if (status.IsSuccess()) {
        Y_ENSURE(scaner);
        return TQueryBenchmarkResult::Result(
            scaner->ExtractRawResults(),
            scaner->GetTiming(),
            scaner->GetQueryPlan(),
            scaner->GetPlanAst(),
            scaner->GetExecStats(),
            expected
        );
    }
    TStringBuilder errorInfo;
    TString plan, ast, execStats;
    switch (status.GetStatus()) {
        case NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED:
            errorInfo << becnhmarkSettings.Deadline.Name << " deadline expiried: " << status.GetIssues();
            break;
        default:
            if (scaner) {
                errorInfo << scaner->GetErrorInfo();
                plan = scaner->GetQueryPlan();
                ast = scaner->GetPlanAst();
                execStats = scaner->GetExecStats();
            } else {
                errorInfo << "Operation failed with status " << status.GetStatus() << ": " << status.GetIssues().ToString();
            }
            break;
    }
    return TQueryBenchmarkResult::Error(errorInfo, plan, ast, execStats);
}

TMaybe<TQueryBenchmarkResult> SetTimeoutSettings(NQuery::TExecuteQuerySettings& settings, const TQueryBenchmarkDeadline& deadline) {
    if (deadline.Deadline != TInstant::Max()) {
        auto now = Now();
        if (now >= deadline.Deadline) {
            return TQueryBenchmarkResult::Error(deadline.Name + " deadline expiried", "", "", "");
        }
        settings.ClientTimeout(deadline.Deadline - now);
    }
    return Nothing();
}

TQueryBenchmarkResult ExecuteImpl(const TString& query, TStringBuf expected, NQuery::TQueryClient& client, const TQueryBenchmarkSettings& benchmarkSettings, bool explainOnly) {
    NQuery::TExecuteQuerySettings settings;
    settings.StatsMode(NQuery::EStatsMode::Full);
    settings.ExecMode(explainOnly ? NQuery::EExecMode::Explain : NQuery::EExecMode::Execute);
    if (benchmarkSettings.WithProgress) {
        settings.StatsCollectPeriod(std::chrono::milliseconds(3000));
    }
    if (auto error = SetTimeoutSettings(settings, benchmarkSettings.Deadline)) {
        return *error;
    }
    THolder<TQueryResultScanner> composite;
    const auto txMode = benchmarkSettings.TxMode;
    const auto resStatus = client.RetryQuerySync([&composite, &benchmarkSettings, &query, &settings, txMode](NQuery::TQueryClient& qc) -> TStatus {
        auto txControl = GetTxControl(txMode);
        auto it = qc.StreamExecuteQuery(
            query,
            txControl,
            settings).GetValueSync();
        if (!it.IsSuccess()) {
            return it;
        }
        composite = MakeHolder<TQueryResultScanner>();
        composite->SetDeadlineName(benchmarkSettings.Deadline.Name);
        composite->SetMaxRowsPerResultIndex(benchmarkSettings.MaxRowsPerResultIndex);
        return composite->Scan(it, benchmarkSettings.PlanFileName);
    }, benchmarkSettings.RetrySettings);
    return ConstructResultByStatus(resStatus, composite, expected, benchmarkSettings);
}

TQueryBenchmarkResult Execute(const TString& query, TStringBuf expected, NQuery::TQueryClient& client, const TQueryBenchmarkSettings& settings) {
    return ExecuteImpl(query, expected, client, settings, false);
}

TQueryBenchmarkResult Explain(const TString& query, NQuery::TQueryClient& client, const TQueryBenchmarkSettings& settings) {
    return ExecuteImpl(query, TStringBuf(), client, settings, true);
}

NJson::TJsonValue GetQueryLabels(TStringBuf queryId) {
    NJson::TJsonValue labels(NJson::JSON_MAP);
    labels.InsertValue("query", queryId);
    return labels;
}

NJson::TJsonValue GetSensorValue(TStringBuf sensor, TDuration& value, TStringBuf queryId) {
    NJson::TJsonValue sensorValue(NJson::JSON_MAP);
    sensorValue.InsertValue("sensor", sensor);
    sensorValue.InsertValue("value", value.MilliSeconds());
    sensorValue.InsertValue("labels", GetQueryLabels(queryId));
    return sensorValue;
}

NJson::TJsonValue GetSensorValue(TStringBuf sensor, double value, TStringBuf queryId) {
    NJson::TJsonValue sensorValue(NJson::JSON_MAP);
    sensorValue.InsertValue("sensor", sensor);
    sensorValue.InsertValue("value", value);
    sensorValue.InsertValue("labels", GetQueryLabels(queryId));
    return sensorValue;
}

template <class T>
bool CompareValueImpl(IOutputStream& errStream, const T& valResult, TStringBuf vExpected) {
    T valExpected;
    if (!TryFromString<T>(vExpected, valExpected)) {
        errStream << "cannot parse expected as " << typeid(valResult).name() << "(" << vExpected << ")" << Endl;
        return false;
    }
    return valResult == valExpected;
}

template <class T>
bool CompareValueImplFloat(IOutputStream& errStream, const T& valResult, TStringBuf vExpected) {
    T relativeFloatPrecision = 0.0001;
    TStringBuf precesionStr;
    vExpected.Split("+-", vExpected, precesionStr);
    T valExpected;
    if (!TryFromString<T>(vExpected, valExpected)) {
        errStream << "cannot parse expected as " << typeid(valResult).name() << "(" << vExpected << ")" << Endl;
        return false;
    }
    if (precesionStr.ChopSuffix("%")) {
        if (!TryFromString<T>(precesionStr, relativeFloatPrecision)) {
            errStream << "cannot parse precision expected as " << typeid(valResult).name() << "(" << precesionStr << "%)" << Endl;
            return false;
        }
        relativeFloatPrecision /= 100;
    } else if (precesionStr) {
        T absolutePrecesion;
        if (!TryFromString<T>(precesionStr, absolutePrecesion)) {
            errStream << "cannot parse precision expected as " << typeid(valResult).name() << "(" << precesionStr << ")" << Endl;
            return false;
        }
        return valResult >= valExpected - absolutePrecesion && valResult <= valExpected + absolutePrecesion;
    }
    const auto left = std::min((1 - relativeFloatPrecision) * valExpected, (1 + relativeFloatPrecision) * valExpected);
    const auto right = std::max((1 - relativeFloatPrecision) * valExpected, (1 + relativeFloatPrecision) * valExpected);

    return valResult >= left && valResult <= right;
}

template <>
bool CompareValueImpl<float>(IOutputStream& errStream, const float& valResult, TStringBuf vExpected) {
    return CompareValueImplFloat(errStream, valResult, vExpected);
}

template <>
bool CompareValueImpl<double>(IOutputStream& errStream, const double& valResult, TStringBuf vExpected) {
    return CompareValueImplFloat(errStream, valResult, vExpected);
}

bool CompareValueImplDecimal(const NYdb::TDecimalValue& valResult, TStringBuf vExpected) {
    auto resInt = NYql::NDecimal::FromHalfs(valResult.Low_, valResult.Hi_);
    TStringBuf precesionStr;
    vExpected.Split("+-", vExpected, precesionStr);
    auto expectedInt = NYql::NDecimal::FromString(vExpected, valResult.DecimalType_.Precision, valResult.DecimalType_.Scale);
    auto relativePrecision = 0.0001;
    if (precesionStr.ChopSuffix("%")) {
        relativePrecision = FromString<double>(precesionStr) / 100;
    } else if (precesionStr) {
        auto precInt = NYql::NDecimal::FromString(precesionStr, valResult.DecimalType_.Precision, valResult.DecimalType_.Scale);
        return resInt >= expectedInt - precInt && resInt <= expectedInt + precInt;
    }
    NYql::NDecimal::TInt128 precInt = i64(double(expectedInt) * relativePrecision);
    if (precInt < 0) {
        precInt = -precInt;
    }
    precInt = std::max<NYql::NDecimal::TInt128>(precInt, 1);
    return resInt >= expectedInt - precInt && resInt <= expectedInt + precInt;
}

bool CompareValueImplDatetime(IOutputStream& errStream, const TInstant& valResult, TStringBuf vExpected, TDuration unit) {
    TInstant expected;
    if (!TInstant::TryParseIso8601(vExpected, expected)) {
        i64 i;
        if (!TryFromString(vExpected, i)) {
            errStream << "cannot parse expected as " << typeid(valResult).name() << "(" << vExpected << ")" << Endl;
            return false;
        }
        expected = TInstant::Zero() + i * unit;
    }
    return valResult == expected;
}

template<class T>
bool CompareValueImplDatetime64(IOutputStream& errStream, const T& valResult, TStringBuf vExpected, TDuration unit) {
    T valExpected;
    if (!TryFromString<T>(vExpected, valExpected)) {
        TInstant expected;
        if (!TInstant::TryParseIso8601(vExpected, expected)) {
            errStream << "cannot parse expected as " << typeid(valResult).name() << "(" << vExpected << ")" << Endl;
            return false;
        }
        valExpected = expected.GetValue() / unit.GetValue();
    }
    return valResult == valExpected;
}

template<class T>
bool CompareValuePgImpl(IOutputStream& errStream, const NYdb::TPgValue& v, TStringBuf vExpected) {
    if (v.IsText()) {
        T value;
        if (!TryFromString(v.Content_, value)) {
            errStream << "cannot parse value as " << typeid(value).name() << "(" << v.Content_ << ")" << Endl;
            return false;
        }
        return CompareValueImpl(errStream, value, vExpected);
    }
    const T* value = reinterpret_cast<const T*>(v.Content_.data());
    return CompareValueImpl(errStream, *value, vExpected);
}

bool CompareValuePg(IOutputStream& errStream, const NYdb::TPgValue& v, TStringBuf vExpected) {
    if (v.IsNull()) {
        return vExpected == "";
    }
    if (v.PgType_.TypeName == "pgint2") {
        return CompareValuePgImpl<i16>(errStream, v, vExpected);
    }
    if (v.PgType_.TypeName == "pgint4") {
        return CompareValuePgImpl<i32>(errStream, v, vExpected);
    }
    if (v.PgType_.TypeName == "pgint8") {
        return CompareValuePgImpl<i64>(errStream, v, vExpected);
    }
    if (v.PgType_.TypeName == "pgfloat4") {
        return CompareValuePgImpl<float>(errStream, v, vExpected);
    }
    if (v.PgType_.TypeName == "pgfloat8") {
        return CompareValuePgImpl<double>(errStream, v, vExpected);
    }
    if (IsIn({"pgbytea", "pgtext"}, v.PgType_.TypeName)) {
        return vExpected == v.Content_;
    }
    errStream << "Unsupported pg type: typename=" << v.PgType_.TypeName
        << "; type_mod=" << v.PgType_.TypeModifier
        << Endl;
    return false;
}

bool CompareValuePrimitive(IOutputStream& errStream, const TValueParser& vp, TStringBuf vExpected) {
    switch (vp.GetPrimitiveType()) {
    case EPrimitiveType::Bool:
        return CompareValueImpl(errStream, vp.GetBool(), vExpected);
    case EPrimitiveType::Int8:
        return CompareValueImpl(errStream, vp.GetInt8(), vExpected);
    case EPrimitiveType::Uint8:
        return CompareValueImpl(errStream, vp.GetUint8(), vExpected);
    case EPrimitiveType::Int16:
        return CompareValueImpl(errStream, vp.GetInt16(), vExpected);
    case EPrimitiveType::Uint16:
        return CompareValueImpl(errStream, vp.GetUint16(), vExpected);
    case EPrimitiveType::Int32:
        return CompareValueImpl(errStream, vp.GetInt32(), vExpected);
    case EPrimitiveType::Uint32:
        return CompareValueImpl(errStream, vp.GetUint32(), vExpected);
    case EPrimitiveType::Int64:
        return CompareValueImpl(errStream, vp.GetInt64(), vExpected);
    case EPrimitiveType::Uint64:
        return CompareValueImpl(errStream, vp.GetUint64(), vExpected);
    case EPrimitiveType::Float:
        return CompareValueImpl(errStream, vp.GetFloat(), vExpected);
    case EPrimitiveType::Double:
        return CompareValueImpl(errStream, vp.GetDouble(), vExpected);
    case EPrimitiveType::Date:
        return CompareValueImplDatetime(errStream, vp.GetDate(), vExpected, TDuration::Days(1));
    case EPrimitiveType::Datetime:
        return CompareValueImplDatetime(errStream, vp.GetDatetime(), vExpected, TDuration::Seconds(1));
    case EPrimitiveType::Timestamp:
        return CompareValueImplDatetime(errStream, vp.GetTimestamp(), vExpected, TDuration::MicroSeconds(1));
    case EPrimitiveType::Interval:
        return CompareValueImpl(errStream, vp.GetInterval(), vExpected);
    case EPrimitiveType::Date32:
        return CompareValueImplDatetime64(errStream, vp.GetDate32().time_since_epoch().count(), vExpected, TDuration::Days(1));
    case EPrimitiveType::Datetime64:
        return CompareValueImplDatetime64(errStream, vp.GetDatetime64().time_since_epoch().count(), vExpected, TDuration::Seconds(1));
    case EPrimitiveType::Timestamp64:
        return CompareValueImplDatetime64(errStream, vp.GetTimestamp64().time_since_epoch().count(), vExpected, TDuration::MicroSeconds(1));
    case EPrimitiveType::Interval64:
        return CompareValueImpl(errStream, vp.GetInterval64().count(), vExpected);
    case EPrimitiveType::String:
        return CompareValueImpl(errStream, vp.GetString(), vExpected);
    case EPrimitiveType::Utf8:
        return CompareValueImpl(errStream, vp.GetUtf8(), vExpected);
    default:
        errStream << "unexpected type for comparision: " << vp.GetPrimitiveType() << Endl;
        return false;
    }
}

bool CompareValue(IOutputStream& errStream, const NYdb::TValue& v, TStringBuf vExpected) {
    TValueParser vp(v);
    while (vp.GetKind() == TTypeParser::ETypeKind::Optional) {
        vp.OpenOptional();
        if (vp.IsNull()) {
            return vExpected == "";
        } else if (vExpected == "") {
            return false;
        }
    }
    switch (vp.GetKind()) {
    case TTypeParser::ETypeKind::Decimal:
        return  CompareValueImplDecimal(vp.GetDecimal(), vExpected);
    case TTypeParser::ETypeKind::Primitive:
        return CompareValuePrimitive(errStream, vp, vExpected);
    case TTypeParser::ETypeKind::Pg:
        return CompareValuePg(errStream, vp.GetPg(), vExpected);
    default:
        errStream  << "Unsupported value type kind: " << vp.GetKind() << Endl;
        return false;
    }
}

TString TQueryBenchmarkResult::CalcHash() const {
    MD5 hasher;
    for (const auto& [i, resultData]: RawResults) {
        for (const auto& result: resultData.ResultSets) {
            hasher.Update(FormatResultSetYson(result, NYson::EYsonFormat::Binary));
        }
    }
    char buf[25];
    return hasher.End_b64(buf);
}

void TQueryBenchmarkResult::CompareWithExpected(TStringBuf expected) {
    if (expected.empty()) {
        return;
    }
    const auto expectedSets = StringSplitter(expected).SplitByString("\n\n").SkipEmpty().ToList<TStringBuf>();
    if (expectedSets.size() != RawResults.size()) {
        TStringOutput so(DiffWarrnings);
        so << "Warning: expected " << expectedSets.size() << " results, but actualy " << RawResults.size() << Endl;
    }
    for (size_t i = 0; i < std::min(expectedSets.size(), RawResults.size()); ++i) {
        CompareWithExpected(expectedSets[i], i);
    }
}

size_t GetBenchmarkTableWidth() {
    auto terminalWidth = GetTerminalWidth();
    return terminalWidth ? *terminalWidth : 120;
}

void TQueryBenchmarkResult::CompareWithExpected(TStringBuf expected, size_t resultSetIndex) {
    const auto& resultData = RawResults.at(resultSetIndex);
    const auto& resultSets = resultData.ResultSets;
    auto expectedLines = StringSplitter(expected).Split('\n').SkipEmpty().ToList<TString>();
    if (expectedLines.empty()) {
        return;
    }
    if (resultSets.empty()) {
        TStringOutput errStream(DiffErrors);
        errStream << "Result " << resultSetIndex << ": expected " << (expectedLines.size() - 1) << " rows, but got empty result" << Endl;
        return;
    }

    TStringOutput errStream(DiffErrors);
    TStringOutput warnStream(DiffWarrnings);
    auto columns = static_cast<TVector<TString>>(NCsvFormat::CsvSplitter(expectedLines.front()));
    bool schemeOk = true;
    if (resultSets.front().ColumnsCount() != columns.size()) {
        errStream << "Result " << resultSetIndex << ": incorrect scheme, " << resultSets.front().ColumnsCount() << " columns in result, but " << columns.size() << " expected." << Endl;
        schemeOk = false;
    }
    auto parser = MakeHolder<TResultSetParser>(resultSets.front());
    for (size_t c = 0; c < columns.size(); ++c) {
        if (parser->ColumnIndex(columns[c]) < 0) {
            if (c < parser->ColumnsCount()) {
                warnStream << "Result " << resultSetIndex << ": scheme warning, column " << columns[c] << " not found in result. By position will be used " << resultSets.front().GetColumnsMeta()[c].Name << Endl;
                columns[c] = resultSets.front().GetColumnsMeta()[c].Name;
            } else {
                errStream << "Result " << resultSetIndex << ": incorrect scheme, column " << columns[c] << " not found in result." << Endl;
                schemeOk = false;
            }
        }
    }
    if (!schemeOk) {
        TVector<TString> rCols;
        for (const auto& c: resultSets.front().GetColumnsMeta()) {
            rCols.emplace_back(c.Name);
        }
        errStream << "Result " << resultSetIndex << " columns: " << JoinSeq(", ", rCols) << Endl;
        return;
    }

    size_t resultRowsCount = 0;
    for (const auto& rs : resultSets) {
        resultRowsCount += rs.RowsCount();
    }
    if (expectedLines.back() == "...") {
        expectedLines.pop_back();
        if (resultRowsCount + 1 < expectedLines.size()) {
            errStream << "Result " << resultSetIndex << ": too small lines count (" << resultRowsCount << " in result, but " << expectedLines.size() - 1 << "+ expected)" << Endl;
            return;
        }
    } else if (resultRowsCount + 1 != expectedLines.size()) {
        errStream << "Result " << resultSetIndex << ": incorrect lines count (" << resultRowsCount << " in result, but " << expectedLines.size() - 1 << " expected)." << Endl;
        return;
    }

    TVector<TVector<TString>> diffs;
    size_t resNum = 1;
    for (auto expectedLine = expectedLines.begin() + 1; expectedLine != expectedLines.end(); ++expectedLine) {
        while (!parser->TryNextRow() && resNum < resultSets.size()) {
            parser = MakeHolder<TResultSetParser>(resultSets[resNum++]);
        }
        NCsvFormat::CsvSplitter splitter(*expectedLine);
        TVector<TString> lineDiff(columns.size() + 1, "NoExp!");
        lineDiff.front() = ToString(expectedLine - expectedLines.begin() - 1);
        bool hasDiff = false;
        for (const auto& column: columns) {
            auto cIdx = &column - columns.cbegin();
            const NYdb::TValue resultValue = parser->GetValue(column);
            TStringBuf expectedValue = splitter.Consume();
            const TString resultStr = FormatValueYson(resultValue);
            if (CompareValue(errStream, resultValue, expectedValue)) {
                lineDiff[cIdx + 1] = resultStr;
            } else {
                auto& colors = NColorizer::StdErr();
                lineDiff[cIdx + 1] = TStringBuilder()
                    << colors.Red()  << resultStr << colors.Reset()
                    << " (" << colors.Green()  << expectedValue << colors.Reset() << ")";
                hasDiff = true;
            }
            if (!splitter.Step() && &column + 1 != columns.end()) {
                hasDiff = true;
                break;
            }
        }
        if (hasDiff) {
            diffs.emplace_back(std::move(lineDiff));
        }
    }
    if (!diffs.empty()) {
        TVector<TString> tableColums {"Line"};
        tableColums.insert(tableColums.end(), columns.cbegin(), columns.cend());
        TPrettyTable table(tableColums, TPrettyTableConfig().MaxWidth(GetBenchmarkTableWidth()));
        for (const auto& diffLine: diffs) {
            auto& row = table.AddRow();
            for (ui32 i = 0; i < diffLine.size(); ++i) {
                row.Column(i, diffLine[i]);
            }
        }
        errStream << "Result " << resultSetIndex << " has diff in results: " << Endl;
        table.Print(errStream);
    }
}

} // NYdb::NConsoleClient::BenchmarkUtils
