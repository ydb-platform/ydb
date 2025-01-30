#include "ydb_benchmark.h"
#include "benchmark_utils.h"
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/plan2svg.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <library/cpp/json/json_writer.h>
#include <util/string/printf.h>
#include <util/folder/path.h>
#include <optional>

namespace NYdb::NConsoleClient {
    TWorkloadCommandBenchmark::TWorkloadCommandBenchmark(NYdbWorkload::TWorkloadParams& params, const NYdbWorkload::IWorkloadQueryGenerator::TWorkloadType& workload)
        : TWorkloadCommandBase(workload.CommandName, params, NYdbWorkload::TWorkloadParams::ECommandType::Run, workload.Description, workload.Type)
    {

    }


void TWorkloadCommandBenchmark::Config(TConfig& config) {
    TWorkloadCommandBase::Config(config);
    config.Opts->AddLongOption("output", "Path to file to save query output to.")
        .Optional()
        .RequiredArgument("FILE")
        .DefaultValue("results.out")
        .StoreResult(&OutFilePath);
    config.Opts->AddLongOption("iterations", "Iterations count")
        .DefaultValue(1)
        .StoreResult(&IterationsCount);
    config.Opts->AddLongOption("json", "Path to file to save json report to.\nJson report includes some metrics of queries, min and max time, stddev, etc. Has Solomon sensor format.")
        .DefaultValue("")
        .StoreResult(&JsonReportFileName);
    config.Opts->AddLongOption("csv", "Path to file to save csv version of summary table.")
        .DefaultValue("")
        .StoreResult(&CsvReportFileName);
    config.Opts->AddLongOption("ministat", "Ministat report file name")
        .DefaultValue("")
        .StoreResult(&MiniStatFileName);
    config.Opts->AddLongOption("plan", "Query plans report file name")
        .DefaultValue("")
        .StoreResult(&PlanFileName);
    config.Opts->AddLongOption("query-settings")
        .AppendTo(&QuerySettings).Hidden();
    config.Opts->AddLongOption("query-prefix", "Query prefix.\nEvery prefix is a line that will be added to the beginning of each query. For multiple prefixes lines use this option several times.")
        .AppendTo(&QuerySettings);
    config.Opts->MutuallyExclusive("query-prefix", "query-settings");
    auto fillTestCases = [](TStringBuf line, std::function<void(ui32)>&& op) {
        for (const auto& token : StringSplitter(line).Split(',').SkipEmpty()) {
            TStringBuf part = token.Token();
            TStringBuf from, to;
            if (part.TrySplit('-', from, to)) {
                ui32 begin = FromString(from);
                ui32 end = FromString(to);
                while (begin <= end) {
                    op(begin);
                    ++begin;
                }
            } else {
                op(FromString<ui32>(part));
            }
        }
    };

    auto includeOpt = config.Opts->AddLongOption("include",
        "Run only specified queries (ex.: 0,1,2,3,5-10,20)")
        .Optional()
        .Handler1T<TStringBuf>([this, fillTestCases](TStringBuf line) {
            QueriesToRun.clear();
            fillTestCases(line, [this](ui32 q) {
                QueriesToRun.insert(q);
            });
        });
    auto excludeOpt = config.Opts->AddLongOption("exclude",
        "Run all queries except given ones (ex.: 0,1,2,3,5-10,20)")
        .Optional()
        .Handler1T<TStringBuf>([this, fillTestCases](TStringBuf line) {
            fillTestCases(line, [this](ui32 q) {
                QueriesToSkip.emplace(q);
            });
        });

    config.Opts->MutuallyExclusiveOpt(includeOpt, excludeOpt);

    config.Opts->AddLongOption("executer", "Query executer type."
            " Options: scan, generic\n"
            "scan - use scan queries;\n"
            "generic - use generic queries.")
        .DefaultValue(QueryExecuterType)
        .Handler1T<TStringBuf>([this](TStringBuf arg) {
                const auto l = to_lower(TString(arg));
                if (!TryFromString(arg, QueryExecuterType)) {
                    throw yexception() << "Ivalid query executer type: " << arg;
                }
            });
    config.Opts->AddLongOption('v', "verbose", "Verbose output").NoArgument().StoreValue(&VerboseLevel, 1);

    config.Opts->AddLongOption("global-timeout", "Global timeout for all requests")
        .StoreResult(&GlobalTimeout);

    config.Opts->AddLongOption("request-timeout", "Timeout for each iteration of each request")
        .StoreResult(&RequestTimeout);

}

TString TWorkloadCommandBenchmark::PatchQuery(const TStringBuf& original) const {
    TString result(original);

    if (!QuerySettings.empty()) {
        result = JoinSeq("\n", QuerySettings) + "\n" + result;
    }

    std::vector<TStringBuf> lines;
    for (auto& line : StringSplitter(result).Split('\n').SkipEmpty()) {
        if (line.StartsWith("--") && !line.StartsWith("--!")) {
            continue;
        }

        lines.push_back(line);
    }

    return JoinSeq('\n', lines);
}

bool TWorkloadCommandBenchmark::NeedRun(ui32 queryIdx) const {
    if (QueriesToRun && !QueriesToRun.contains(queryIdx)) {
        return false;
    }
    if (QueriesToSkip.contains(queryIdx)) {
        return false;
    }
    return true;
}

namespace {

TVector<TString> ColumnNames {
    "Query #",
    "ColdTime",
    "Min",
    "Max",
    "Mean",
    "Median",
    "UnixBench",
    "Std",
    "RttMin",
    "RttMax",
    "RttAvg",
    "SuccessCount",
    "FailsCount",
    "DiffsCount"
};

struct TTestInfoProduct {
    double ColdTime = 1;
    double Min = 1;
    double Max = 1;
    double RttMin = 1;
    double RttMax = 1;
    double RttMean = 1;
    double Mean = 1;
    double Median = 1;
    double UnixBench = 1;
    double Std = 0;
    void operator *=(const BenchmarkUtils::TTestInfo& other) {
        ColdTime *= other.ColdTime.MillisecondsFloat();
        Min *= other.Min.MillisecondsFloat();
        Max *= other.Max.MillisecondsFloat();
        RttMin *= other.RttMin.MillisecondsFloat();
        RttMax *= other.RttMax.MillisecondsFloat();
        Mean *= other.Mean;
        Median *= other.Median;
        UnixBench *= other.UnixBench.MillisecondsFloat();
    }
    void operator ^= (ui32 count) {
        ColdTime = pow(ColdTime, 1./count);
        Min = pow(Min, 1./count);
        Max = pow(Max, 1./count);
        RttMin = pow(RttMin, 1./count);
        RttMax = pow(RttMax, 1./count);
        RttMean = pow(RttMean, 1./count);
        Mean = pow(Mean, 1./count);
        Median = pow(Median, 1./count);
        UnixBench = pow(UnixBench, 1./count);
    }
};

template<class T>
double DurationToDouble(const T& value) {
    return value;
}

template<>
double DurationToDouble<TDuration>(const TDuration& value) {
    return value.MillisecondsFloat();
}

template<class T, bool isDuration>
struct TValueToTable {
    static void Do(TPrettyTable::TRow& tableRow, ui32 index, const T& value) {
        if (value) {
            tableRow.Column(index, value);
        }
    }
};

template<class T>
struct TValueToTable<T, true>{
    static void Do(TPrettyTable::TRow& tableRow, ui32 index, const T& value) {
        tableRow.Column(index, Sprintf("%7.3f", 0.001 * DurationToDouble(value)));
    }
};

template<class T, bool isDuration>
struct TValueToCsv {
    static void Do(IOutputStream& csv, const T& value) {
        if (value) {
            csv << value;
        }
    }
};

template<class T>
struct TValueToCsv<T, true> {
    static void Do(IOutputStream& csv, const T& value) {
        if (value) {
            csv << 0.001 * DurationToDouble(value);
        }
    }
};

template<class T, bool isDuration, bool is_arr = std::is_arithmetic<T>::value>
struct TValueToJson {
    static void Do(NJson::TJsonValue& json, ui32 index, ui32 queryN, const T& value) {
        Y_UNUSED(json);
        Y_UNUSED(index);
        Y_UNUSED(queryN);
        Y_UNUSED(value);
    }
};

template<class T, bool is_arr>
struct TValueToJson<T, true, is_arr> {
    static void Do(NJson::TJsonValue& json, ui32 index, ui32 queryN, const T& value) {
        json.AppendValue(BenchmarkUtils::GetSensorValue(ColumnNames[index], DurationToDouble(value), queryN));
    }
};

template<class T>
struct TValueToJson<T, false, true> {
    static void Do(NJson::TJsonValue& json, ui32 index, ui32 queryN, const T& value) {
        json.AppendValue(BenchmarkUtils::GetSensorValue(ColumnNames[index], value, queryN));
    }
};


template <bool isDuration = false, class T>
void CollectField(TPrettyTable::TRow& tableRow, ui32 index, IOutputStream* csv, NJson::TJsonValue* json, TStringBuf rowName, const T& value) {
    TValueToTable<T, isDuration>::Do(tableRow, index, value);
    if (csv) {
        if (index) {
            *csv << ",";
        }
        TValueToCsv<T, isDuration>::Do(*csv, value);
    }
    auto queryN = rowName;
    if(json && queryN.SkipPrefix("Query")) {
        TValueToJson<T, isDuration>::Do(*json, index, FromString<ui32>(queryN), value);
    }
}

template<class T>
void CollectStats(TPrettyTable& table, IOutputStream* csv, NJson::TJsonValue* json, const TString& name, ui32 sCount, ui32 fCount, ui32 dCount, const T& testInfo) {
    auto& row = table.AddRow();
    ui32 index = 0;
    CollectField(row, index++, csv, json, name, name);
    CollectField<true>(row, index++, csv, json, name, testInfo.ColdTime);
    CollectField<true>(row, index++, csv, json, name, testInfo.Min);
    CollectField<true>(row, index++, csv, json, name, testInfo.Max);
    CollectField<true>(row, index++, csv, json, name, testInfo.Mean);
    CollectField<true>(row, index++, csv, json, name, testInfo.Median);
    CollectField<true>(row, index++, csv, json, name, testInfo.UnixBench);
    CollectField<true>(row, index++, csv, json, name, testInfo.Std);
    CollectField<true>(row, index++, csv, json, name, testInfo.RttMin);
    CollectField<true>(row, index++, csv, json, name, testInfo.RttMax);
    CollectField<true>(row, index++, csv, json, name, testInfo.RttMean);
    CollectField(row, index++, csv, json, name, sCount);
    CollectField(row, index++, csv, json, name, fCount);
    CollectField(row, index++, csv, json, name, dCount);
    if (csv) {
        *csv << Endl;
    }
}

}

template <typename TClient>
int TWorkloadCommandBenchmark::RunBench(TClient* client, NYdbWorkload::IWorkloadQueryGenerator& workloadGen) {
    using namespace BenchmarkUtils;
    TOFStream outFStream{OutFilePath};
    TPrettyTable statTable(ColumnNames);
    TStringStream report;
    report << "Results for " << IterationsCount << " iterations" << Endl;

    THolder<NJson::TJsonValue> jsonReport;
    if (JsonReportFileName) {
        jsonReport = MakeHolder<NJson::TJsonValue>(NJson::JSON_ARRAY);
    }
    const auto qtokens = workloadGen.GetWorkload(Type);
    ui32 queriesWithAllSuccess = 0;
    ui32 queriesWithSomeFails = 0;
    ui32 queriesWithDiff = 0;
    THolder<TOFStream> plansReport;
    THolder<TOFStream> csvReport;
    if (CsvReportFileName) {
        csvReport = MakeHolder<TOFStream>(CsvReportFileName);
        *csvReport << JoinSeq(",", ColumnNames) << Endl;
    }

    TTestInfo sumInfo({}, {});
    TTestInfoProduct productInfo;

    std::map<ui32, TTestInfo> queryRuns;
    auto qIter = qtokens.cbegin();
    GlobalDeadline = (GlobalTimeout != TDuration::Zero()) ? Now() + GlobalTimeout : TInstant::Max();
    for (ui32 queryN = 0; queryN < qtokens.size() && Now() < GlobalDeadline; ++queryN, ++qIter) {
        const auto& qInfo = *qIter;
        if (!NeedRun(queryN)) {
            continue;
        }

        if (!HasCharsInString(qInfo.Query.c_str())) {
            continue;
        }

        const TString query = PatchQuery(qInfo.Query.c_str());

        std::vector<TDuration> clientTimings;
        std::vector<TDuration> serverTimings;
        clientTimings.reserve(IterationsCount);
        serverTimings.reserve(IterationsCount);

        Cout << Sprintf("Query%02u", queryN) << ":" << Endl;
        if (VerboseLevel > 0) {
            Cout << "Query text:" << Endl;
            Cout << query << Endl << Endl;
        }

        ui32 successIteration = 0;
        ui32 failsCount = 0;
        ui32 diffsCount = 0;
        std::optional<TString> prevResult;
        if (PlanFileName) {
            TQueryBenchmarkResult res = TQueryBenchmarkResult::Error("undefined", "undefined", "undefined");
            try {
                if (client) {
                    res = Explain(query, *client, GetDeadline());
                } else {
                    res = TQueryBenchmarkResult::Result(TQueryBenchmarkResult::TRawResults(), TDuration::Zero(), "", "");
                }
            } catch (...) {
                res = TQueryBenchmarkResult::Error(CurrentExceptionMessage(), "", "");
            }
            SavePlans(res, queryN, "explain");
        }

        for (ui32 i = 0; i < IterationsCount; ++i) {
            auto t1 = TInstant::Now();
            if (t1 >= GlobalDeadline) {
                Cerr << "Global timeout (" << GlobalTimeout << ") expiried, global deadline was " << GlobalDeadline << Endl;
                break;
            }
            TQueryBenchmarkResult res = TQueryBenchmarkResult::Error("undefined", "undefined", "undefined");

            TQueryBenchmarkSettings settings;
            settings.Deadline = GetDeadline();
            settings.WithProgress = true;

            if (PlanFileName) {
                settings.PlanFileName = TStringBuilder() << PlanFileName << "." << queryN << "." << ToString(i) << ".in_progress";
            }

            try {
                if (client) {
                    res = Execute(query, *client, settings);
                } else {
                    res = TQueryBenchmarkResult::Result(TQueryBenchmarkResult::TRawResults(), TDuration::Zero(), "", "");
                }
            } catch (...) {
                const auto msg = CurrentExceptionMessage();
                Cerr << "Exception while execute query: " << msg << Endl;
                res = TQueryBenchmarkResult::Error(CurrentExceptionMessage(), "", "");
            }
            auto duration = TInstant::Now() - t1;

            Cout << "\titeration " << i << ":\t";
            if (res) {
                Cout << "ok\t" << duration << " seconds" << Endl;
                clientTimings.emplace_back(duration);
                serverTimings.emplace_back(res.GetServerTiming());
                ++successIteration;
                if (successIteration == 1) {
                    outFStream << queryN << ": " << Endl;
                    PrintResult(res, outFStream, qInfo.ExpectedResult);
                }
                const auto resHash = res.CalcHash();
                if ((!prevResult || *prevResult != resHash) && !res.IsExpected(qInfo.ExpectedResult)) {
                    outFStream << queryN << ":" << Endl <<
                        "Query text:" << Endl <<
                        query << Endl << Endl <<
                        "UNEXPECTED DIFF: " << Endl
                          << "RESULT: " << Endl;
                    PrintResult(res, outFStream, qInfo.ExpectedResult);
                    outFStream << Endl
                            << "EXPECTATION: " << Endl << qInfo.ExpectedResult << Endl;
                    prevResult = resHash;
                    ++diffsCount;
                }
            } else {
                ++failsCount;
                Cout << "failed\t" << duration << " seconds" << Endl;
                Cerr << queryN << ":" << Endl
                    << "iteration " << i << Endl
                    << res.GetErrorInfo() << Endl;
                Cerr << "Query text:" << Endl;
                Cerr << query << Endl << Endl;
                Sleep(TDuration::Seconds(1));
            }
            SavePlans(res, queryN, ToString(i));
        }

        auto [inserted, success] = queryRuns.emplace(queryN, TTestInfo(std::move(clientTimings), std::move(serverTimings)));
        Y_ABORT_UNLESS(success);
        auto& testInfo = inserted->second;
        CollectStats(statTable, csvReport.Get(), jsonReport.Get(), Sprintf("Query%02u", queryN), successIteration, failsCount, diffsCount, testInfo);
        if (successIteration != IterationsCount) {
            ++queriesWithSomeFails;
        } else {
            ++queriesWithAllSuccess;
            sumInfo += testInfo;
            productInfo *= testInfo;
        }
        if (diffsCount) {
            ++queriesWithDiff;
        }
    }

    if (queriesWithAllSuccess) {
        CollectStats(statTable, csvReport.Get(), jsonReport.Get(), "Sum", queriesWithAllSuccess, queriesWithSomeFails, queriesWithDiff, sumInfo);
        sumInfo /= queriesWithAllSuccess;
        CollectStats(statTable, csvReport.Get(), jsonReport.Get(), "Avg", queriesWithAllSuccess, queriesWithSomeFails, queriesWithDiff, sumInfo);
        productInfo ^= queriesWithAllSuccess;
        CollectStats(statTable, csvReport.Get(), jsonReport.Get(), "GAvg", queriesWithAllSuccess, queriesWithSomeFails, queriesWithDiff, productInfo);
    }

    statTable.Print(report);

    Cout << Endl << report.Str() << Endl;
    Cout << "Results saved to " << OutFilePath << Endl;

    if (MiniStatFileName) {
        TOFStream jStream{MiniStatFileName};

        for (ui32 rowId = 0; rowId < IterationsCount; ++rowId) {
            ui32 colId = 0;
            for(auto [_, testInfo] : queryRuns) {
                if (colId) {
                    jStream << ",";
                }
                ++colId;
                if (rowId < testInfo.ServerTimings.size()) {
                    jStream << testInfo.ServerTimings.at(rowId).MilliSeconds();
                }
            }

            jStream << Endl;
        }
        jStream.Finish();
    }

    if (jsonReport) {
        TOFStream jStream{JsonReportFileName};
        NJson::WriteJson(&jStream, jsonReport.Get(), /*formatOutput*/ true);
        jStream.Finish();
        Cout << "Json report saved to " << JsonReportFileName << Endl;
    }

    if (csvReport) {
        csvReport.Reset();
        Cout << "Summary table saved in CSV format to " << CsvReportFileName << Endl;
    }

    return queriesWithSomeFails ? EXIT_FAILURE : EXIT_SUCCESS;
}

void TWorkloadCommandBenchmark::PrintResult(const BenchmarkUtils::TQueryBenchmarkResult& res, IOutputStream& out, const std::string& expected) const {
    TResultSetPrinter printer(TResultSetPrinter::TSettings()
        .SetOutput(&out)
        .SetMaxRowsCount(std::max(StringSplitter(expected.c_str()).Split('\n').Count(), (size_t)100))
        .SetFormat(EDataFormat::Pretty).SetMaxWidth(120)
    );
    for (const auto& [i, rr]: res.GetRawResults()) {
        for(const auto& r: rr) {
            printer.Print(r);
            printer.Reset();
        }
    }
    out << Endl << Endl;
}

void TWorkloadCommandBenchmark::SavePlans(const BenchmarkUtils::TQueryBenchmarkResult& res, ui32 queryNum, const TStringBuf name) const {
    if (!PlanFileName) {
        return;
    }
    TFsPath(PlanFileName).Parent().MkDirs();
    const TString planFName =  TStringBuilder() << PlanFileName << "." << queryNum << "." << name << ".";
    if (res.GetQueryPlan()) {
        {
            TFileOutput out(planFName + "table");
            TQueryPlanPrinter queryPlanPrinter(EDataFormat::PrettyTable, true, out, 120);
            queryPlanPrinter.Print(res.GetQueryPlan());
        }
        {
            TFileOutput out(planFName + "json");
            TQueryPlanPrinter queryPlanPrinter(EDataFormat::JsonBase64, true, out, 120);
            queryPlanPrinter.Print(res.GetQueryPlan());
        }
        {
            TPlanVisualizer pv;
            TFileOutput out(planFName + "svg");
            try {
                pv.LoadPlans(res.GetQueryPlan());
                out << pv.PrintSvg();
            } catch (std::exception& e) {
                out << "<svg width='1024' height='256' xmlns='http://www.w3.org/2000/svg'><text>" << e.what() << "<text></svg>";
            }
        }
    }
    if (res.GetPlanAst()) {
        TFileOutput out(planFName + "ast");
        out << res.GetPlanAst();
    }
}

BenchmarkUtils::TQueryBenchmarkDeadline TWorkloadCommandBenchmark::GetDeadline() const {
    BenchmarkUtils::TQueryBenchmarkDeadline result;
    if (GlobalDeadline != TInstant::Max()) {
        result.Deadline = GlobalDeadline;
        result.Name = "Global ";
    }
    TInstant requestDeadline = (RequestTimeout == TDuration::Zero()) ? TInstant::Max() : (Now() + RequestTimeout);
    if (requestDeadline < result.Deadline) {
        result.Deadline = requestDeadline;
        result.Name = "Request";
    }
    return result;
}

int TWorkloadCommandBenchmark::DoRun(NYdbWorkload::IWorkloadQueryGenerator& workloadGen, TConfig& /*config*/) {
    switch (QueryExecuterType) {
    case EQueryExecutor::Scan:
        return RunBench(TableClient.Get(), workloadGen);
    case EQueryExecutor::Generic:
        return RunBench(QueryClient.Get(), workloadGen);
    }
}

}
