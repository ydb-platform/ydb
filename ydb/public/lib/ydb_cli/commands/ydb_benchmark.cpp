#include "ydb_benchmark.h"
#include "benchmark_utils.h"
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/plan2svg.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <library/cpp/json/json_writer.h>
#include <util/stream/null.h>
#include <util/string/printf.h>
#include <util/folder/path.h>
#include <util/random/shuffle.h>

namespace NYdb::NConsoleClient {
    TWorkloadCommandBenchmark::TWorkloadCommandBenchmark(NYdbWorkload::TWorkloadParams& params, const NYdbWorkload::IWorkloadQueryGenerator::TWorkloadType& workload)
        : TWorkloadCommandBase(workload.CommandName, params, NYdbWorkload::TWorkloadParams::ECommandType::Run, workload.Description, workload.Type)
    {
        RetrySettings.MaxRetries(0);
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
    config.Opts->AddLongOption("retries", "Max retry count for every request.").StoreResult(&RetrySettings.MaxRetries_).DefaultValue(RetrySettings.MaxRetries_);
    auto fillTestCases = [](TStringBuf line, std::function<void(TStringBuf)>&& op) {
        for (const auto& token : StringSplitter(line).Split(',').SkipEmpty()) {
            TStringBuf part = token.Token();
            TStringBuf from, to;
            ui32 index;
            if (part.TrySplit('-', from, to)) {
                ui32 begin, end;
                if (TryFromString(from, begin) && TryFromString(to, end)) {
                    for (;begin <= end; ++begin) {
                        op(Sprintf("Query%02u", begin));
                    }
                    continue;
                }
            } else if (TryFromString(part, index)) {
                op(Sprintf("Query%02u", index));
                continue;
            }
            op(part);
        }
    };

    auto& includeOpt = config.Opts->AddLongOption("include",
        "Run only specified queries (ex.: 0,1,2,3,5-10,20). If queries has names then names shoud be used and indexes in other case.");
    includeOpt
        .Optional()
        .GetOpt().Handler1T<TStringBuf>([this, fillTestCases](TStringBuf line) {
            QueriesToRun.clear();
            fillTestCases(line, [this](TStringBuf q) {
                QueriesToRun.emplace(q);
            });
        });
    auto& excludeOpt = config.Opts->AddLongOption("exclude",
        "Run all queries except given ones (ex.: 0,1,2,3,5-10,20). If queries has names then names shoud be used and indexes in other case.");
    excludeOpt
        .Optional()
        .GetOpt().Handler1T<TStringBuf>([this, fillTestCases](TStringBuf line) {
            fillTestCases(line, [this](TStringBuf q) {
                QueriesToSkip.emplace(q);
            });
        });

    config.Opts->MutuallyExclusiveOpt(includeOpt, excludeOpt);

    config.Opts->AddLongOption('v', "verbose", "Verbose output").NoArgument().StoreValue(&VerboseLevel, 1);

    config.Opts->AddLongOption("global-timeout", "Global timeout for all requests")
        .StoreResult(&GlobalTimeout);

    config.Opts->AddLongOption("request-timeout", "Timeout for each iteration of each request")
        .StoreResult(&RequestTimeout);

    config.Opts->AddLongOption('t', "threads", "Number of parallel threads in workload")
        .StoreResult(&Threads).DefaultValue(Threads).RequiredArgument("COUNT");
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

bool TWorkloadCommandBenchmark::NeedRun(const TString& queryName) const {
    if (QueriesToRun && !QueriesToRun.contains(queryName)) {
        return false;
    }
    if (QueriesToSkip.contains(queryName)) {
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
    "GrossTime",
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
    std::vector<TDuration> ClientTimings;

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
    static void Do(NJson::TJsonValue& json, ui32 index, TStringBuf queryName, const T& value) {
        Y_UNUSED(json);
        Y_UNUSED(index);
        Y_UNUSED(queryName);
        Y_UNUSED(value);
    }
};

template<class T, bool is_arr>
struct TValueToJson<T, true, is_arr> {
    static void Do(NJson::TJsonValue& json, ui32 index, TStringBuf queryName, const T& value) {
        json.AppendValue(BenchmarkUtils::GetSensorValue(ColumnNames[index], DurationToDouble(value), queryName));
    }
};

template<class T>
struct TValueToJson<T, false, true> {
    static void Do(NJson::TJsonValue& json, ui32 index, TStringBuf queryName, const T& value) {
        json.AppendValue(BenchmarkUtils::GetSensorValue(ColumnNames[index], value, queryName));
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
    if(json) {
        TValueToJson<T, isDuration>::Do(*json, index, rowName, value);
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
    auto grossTime = TDuration::Zero();
    for (const auto& clientTime: testInfo.ClientTimings) {
        grossTime += clientTime;
    }
    CollectField<true>(row, index++, csv, json, name, grossTime);
    CollectField(row, index++, csv, json, name, sCount);
    CollectField(row, index++, csv, json, name, fCount);
    CollectField(row, index++, csv, json, name, dCount);
    if (csv) {
        *csv << Endl;
    }
}

}

using namespace BenchmarkUtils;

class TWorkloadCommandBenchmark::TIterationExecution: public IObjectInQueue, public TAtomicRefCount<TIterationExecution> {
public:
    using TPtr = TIntrusivePtr<TIterationExecution>;
    TIterationExecution(const TWorkloadCommandBenchmark& owner, const TString& query, const TString& queryName, const TString& expected)
        : QueryName(queryName)
        , Query(query)
        , Expected(expected)
        , Owner(owner)
    {}

    void Process(void*) override {
        bool execute = Iteration >= 0; // explain in other case
        if (Owner.Threads == 0) {
            PrintQueryHeader();
        }
        auto t1 = TInstant::Now();
        if (t1 >= Owner.GlobalDeadline) {
            Cerr << "Global timeout (" << Owner.GlobalTimeout << ") expiried, global deadline was " << Owner.GlobalDeadline << Endl;
            return;
        }
        try {
            if (Owner.QueryClient) {
                auto settings = Owner.GetBenchmarkSettings(execute);
                if (execute) {
                    if (Owner.PlanFileName) {
                        settings.PlanFileName = TStringBuilder() << Owner.PlanFileName << "." << QueryName << "." << ToString(Iteration) << ".in_progress";
                    }
                    Result = Execute(Query, Expected, *Owner.QueryClient, settings);
                } else {
                    Result = Explain(Query, *Owner.QueryClient, settings);
                }
            } else {
                Result = TQueryBenchmarkResult::Result(TQueryBenchmarkResult::TRawResults(), TDuration::Zero(), "", "", "");
            }
        } catch (...) {
            const auto msg = CurrentExceptionMessage();
            Result = TQueryBenchmarkResult::Error(CurrentExceptionMessage(), "", "");
        }
        ClientDuration = TInstant::Now() - t1;
        Owner.SavePlans(Result, QueryName, execute ? ToString(Iteration) : "explain");
        if (Owner.Threads == 0) {
            PrintResult();
        }
    }

    void PrintQueryHeader() const {
        if (Iteration == 0 && !Owner.PlanFileName || Iteration < 0) {
            Cout << QueryName << ":" << Endl;
            if (Owner.VerboseLevel > 0) {
                Cout << "Query text:" << Endl;
                Cout << Query << Endl << Endl;
            }
        }
    }

    void PrintResult() const {
        if (Iteration < 0) {
            return;
        }

        Cout << "\titeration " << Iteration << ":\t";
        if (Result) {
            Cout << "ok\t" << ClientDuration << " seconds" << Endl;
            if (Result.GetDiffErrors()) {
                Cerr << Result.GetDiffWarrnings() << Endl;
                Cerr << Result.GetDiffErrors() << Endl;
            }
        } else {
            Cout << "failed\t" << ClientDuration << " seconds" << Endl;
            Cerr << QueryName << ":" << Endl
                << "iteration " << Iteration << Endl
                << Result.GetErrorInfo() << Endl;
            Cerr << "Query text:" << Endl;
            Cerr << Query << Endl << Endl;
        }
    }

    YDB_READONLY(TQueryBenchmarkResult, Result, TQueryBenchmarkResult::Error("undefined", "undefined", "undefined"));
    YDB_READONLY_DEF(TString, QueryName);
    YDB_READONLY_DEF(TString, Query);
    YDB_READONLY_DEF(TString, Expected);
    YDB_READONLY_DEF(TDuration, ClientDuration);
    YDB_ACCESSOR(i32, Iteration, 0);

private:
    const TWorkloadCommandBenchmark& Owner;
};


int TWorkloadCommandBenchmark::RunBench(NYdbWorkload::IWorkloadQueryGenerator& workloadGen) {
    const auto qtokens = workloadGen.GetWorkload(Type);
    using TIterations = TVector<TIterationExecution::TPtr>;
    TIterations iterations;
    auto qIter = qtokens.cbegin();
    for (ui32 queryN = 0; queryN < qtokens.size() && Now() < GlobalDeadline; ++queryN, ++qIter) {
        const auto& qInfo = *qIter;
        const TString queryName = qInfo.QueryName.empty() ? Sprintf("Query%02u", queryN) : TString(qInfo.QueryName);
        const TString query = PatchQuery(qInfo.Query.c_str());
        if (!NeedRun(queryName) || !HasCharsInString(query)) {
            continue;
        }
        for (ui32 i = 0; i < IterationsCount + (PlanFileName ? 1 : 0); ++i) {
            iterations.emplace_back(MakeIntrusive<TIterationExecution>(*this, query, queryName, qInfo.ExpectedResult.c_str()));
        }
    }
    if (Threads > 0) {
        Shuffle(iterations.begin(), iterations.end());
    }
    TMap<TString, TIterations> queryExecByName;
    for (auto iter: iterations) {
        auto& queryExec = queryExecByName[iter->GetQueryName()];
        iter->SetIteration(queryExec.ysize() - (PlanFileName ? 1 : 0));
        queryExec.emplace_back(iter);
    }

    GlobalDeadline = (GlobalTimeout != TDuration::Zero()) ? Now() + GlobalTimeout : TInstant::Max();
    TThreadPool pool;
    pool.Start(Threads);
    const auto startTime = Now();
    for (auto iter: iterations) {
        pool.SafeAdd(iter.Get());
    }
    pool.Stop();
    const auto grossTime = Now() - startTime;

    ui32 queriesWithAllSuccess = 0;
    ui32 queriesWithSomeFails = 0;
    ui32 queriesWithDiff = 0;
    THolder<TOFStream> miniStatReport;
    if (MiniStatFileName) {
        miniStatReport = MakeHolder<TOFStream>(MiniStatFileName);
    }
    TTestInfo sumInfo({}, {});
    TTestInfoProduct productInfo;
    TPrettyTable statTable(ColumnNames);
    TStringStream report;
    report << "Results for " << IterationsCount << " iterations" << Endl;
    THolder<NJson::TJsonValue> jsonReport;
    if (JsonReportFileName) {
        jsonReport = MakeHolder<NJson::TJsonValue>(NJson::JSON_ARRAY);
    }
    THolder<TOFStream> csvReport;
    if (CsvReportFileName) {
        csvReport = MakeHolder<TOFStream>(CsvReportFileName);
        *csvReport << JoinSeq(",", ColumnNames) << Endl;
    }

    for (const auto& [queryName, queryExec]: queryExecByName) {
        std::vector<TDuration> clientTimings;
        std::vector<TDuration> serverTimings;
        clientTimings.reserve(IterationsCount);
        serverTimings.reserve(IterationsCount);

        ui32 successIteration = 0;
        ui32 failsCount = 0;
        ui32 diffsCount = 0;
        std::optional<TString> prevResult;
        THolder<IOutputStream> outFStreamHolder;
        IOutputStream& outFStream = [&]() -> IOutputStream& {
            if (TSet<TString>{"cout", "stdout", "console"}.contains(OutFilePath)) {
                return Cout;
            }
            if (TSet<TString>{"cerr", "stderr"}.contains(OutFilePath)) {
                return Cerr;
            }
            if (TSet<TString>{"", "/dev/null", "null"}.contains(OutFilePath)) {
                outFStreamHolder = MakeHolder<TNullOutput>();
            } else {
                outFStreamHolder = MakeHolder<TOFStream>(TStringBuilder() << OutFilePath << "." << queryName << ".out");
            }
            return *outFStreamHolder;
        }();
        for (const auto& iterExec: queryExec) {
            if (Threads > 0) {
                iterExec->PrintQueryHeader();
                iterExec->PrintResult();
            }
            if (iterExec->GetIteration() < 0) {
                continue;
            }
            if (iterExec->GetResult()) {
                clientTimings.emplace_back(iterExec->GetClientDuration());
                serverTimings.emplace_back(iterExec->GetResult().GetServerTiming());
                ++successIteration;
                if (successIteration == 1) {
                    outFStream << iterExec->GetQueryName() << ": " << Endl;
                    PrintResult(iterExec->GetResult(), outFStream, iterExec->GetExpected());
                }
                const auto resHash = iterExec->GetResult().CalcHash();
                if ((!prevResult || *prevResult != resHash) && iterExec->GetResult().GetDiffErrors()) {
                    outFStream << iterExec->GetQueryName() << ":" << Endl <<
                        "Query text:" << Endl
                            << iterExec->GetQuery() << Endl << Endl <<
                            "UNEXPECTED DIFF: " << Endl
                            << "RESULT: " << Endl;
                    PrintResult(iterExec->GetResult(), outFStream, iterExec->GetExpected());
                    outFStream << Endl
                            << "EXPECTATION: " << Endl << iterExec->GetExpected() << Endl;
                    prevResult = resHash;
                    ++diffsCount;
                }
            } else {
                ++failsCount;
            }
            if (miniStatReport) {
                if (iterExec->GetIteration()) {
                    *miniStatReport << ",";
                }
                if (iterExec->GetResult()) {
                    *miniStatReport << iterExec->GetResult().GetServerTiming().MilliSeconds();
                }
            }
        }
        if (miniStatReport) {
            *miniStatReport << Endl;
        }
        TTestInfo testInfo(std::move(clientTimings), std::move(serverTimings));
        CollectStats(statTable, csvReport.Get(), jsonReport.Get(), queryName, successIteration, failsCount, diffsCount, testInfo);
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
        sumInfo.ClientTimings.push_back(grossTime);
        CollectStats(statTable, csvReport.Get(), jsonReport.Get(), "Sum", queriesWithAllSuccess, queriesWithSomeFails, queriesWithDiff, sumInfo);
        sumInfo /= queriesWithAllSuccess;
        sumInfo.ClientTimings.back() = grossTime / queriesWithAllSuccess;
        CollectStats(statTable, csvReport.Get(), jsonReport.Get(), "Avg", queriesWithAllSuccess, queriesWithSomeFails, queriesWithDiff, sumInfo);
        productInfo ^= queriesWithAllSuccess;
        CollectStats(statTable, csvReport.Get(), jsonReport.Get(), "GAvg", queriesWithAllSuccess, queriesWithSomeFails, queriesWithDiff, productInfo);
    }

    statTable.Print(report);

    Cout << Endl << report.Str() << Endl;
    Cout << "Results saved to " << OutFilePath << Endl;

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

    return (queriesWithSomeFails || queriesWithDiff) ? EXIT_FAILURE : EXIT_SUCCESS;
}

void TWorkloadCommandBenchmark::PrintResult(const BenchmarkUtils::TQueryBenchmarkResult& res, IOutputStream& out, const std::string& expected) const {
    TResultSetPrinter printer(TResultSetPrinter::TSettings()
        .SetOutput(&out)
        .SetMaxRowsCount(std::max(StringSplitter(expected.c_str()).Split('\n').Count(), (size_t)100))
        .SetFormat(EDataFormat::Pretty).SetMaxWidth(GetBenchmarkTableWidth())
    );
    for (const auto& [i, rr]: res.GetRawResults()) {
        for(const auto& r: rr) {
            printer.Print(r);
            printer.Reset();
        }
    }
    out << Endl << Endl;
}

void TWorkloadCommandBenchmark::SavePlans(const BenchmarkUtils::TQueryBenchmarkResult& res, TStringBuf queryName, const TStringBuf name) const {
    if (!PlanFileName) {
        return;
    }
    TFsPath(PlanFileName).Parent().MkDirs();
    const TString planFName =  TStringBuilder() << PlanFileName << "." << queryName << "." << name << ".";
    if (res.GetQueryPlan()) {
        {
            TFileOutput out(planFName + "table");
            TQueryPlanPrinter queryPlanPrinter(EDataFormat::PrettyTable, true, out, GetBenchmarkTableWidth());
            queryPlanPrinter.Print(res.GetQueryPlan());
        }
        {
            TFileOutput out(planFName + "json");
            TQueryPlanPrinter queryPlanPrinter(EDataFormat::JsonBase64, true, out);
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

BenchmarkUtils::TQueryBenchmarkSettings TWorkloadCommandBenchmark::GetBenchmarkSettings(bool withProgress) const {
    BenchmarkUtils::TQueryBenchmarkSettings result;
    result.WithProgress = withProgress;
    result.RetrySettings = RetrySettings;
    if (GlobalDeadline != TInstant::Max()) {
        result.Deadline.Deadline = GlobalDeadline;
        result.Deadline.Name = "Global ";
    }
    TInstant requestDeadline = (RequestTimeout == TDuration::Zero()) ? TInstant::Max() : (Now() + RequestTimeout);
    if (requestDeadline < result.Deadline.Deadline) {
        result.Deadline.Deadline = requestDeadline;
        result.Deadline.Name = "Request";
    }
    return result;
}

int TWorkloadCommandBenchmark::DoRun(NYdbWorkload::IWorkloadQueryGenerator& workloadGen, TConfig& /*config*/) {
    return RunBench(workloadGen);
}

}
