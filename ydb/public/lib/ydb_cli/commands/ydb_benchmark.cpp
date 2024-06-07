#include "ydb_benchmark.h"
#include "benchmark_utils.h"
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <library/cpp/json/json_writer.h>
#include <util/string/printf.h>

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
    config.Opts->AddLongOption("ministat", "Ministat report file name")
        .DefaultValue("")
        .StoreResult(&MiniStatFileName);
    config.Opts->AddLongOption("query-settings", "Query settings.\nEvery setting is a line that will be added to the beginning of each query. For multiple settings lines use this option several times.")
        .DefaultValue("")
        .AppendTo(&QuerySettings);
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
        .DefaultValue("scan").StoreResult(&QueryExecuterType);
}

TString TWorkloadCommandBenchmark::PatchQuery(const TStringBuf& original) const {
    TString result(original);

    if (!QuerySettings.empty()) {
        result = JoinSeq("\n", QuerySettings) + "\n" + result;
    }

    std::vector<TStringBuf> lines;
    for (auto& line : StringSplitter(result).Split('\n').SkipEmpty()) {
        if (line.StartsWith("--")) {
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

template <typename TClient>
bool TWorkloadCommandBenchmark::RunBench(TClient& client, NYdbWorkload::IWorkloadQueryGenerator& workloadGen) {
    using namespace BenchmarkUtils;
    TOFStream outFStream{OutFilePath};
    TPrettyTable statTable({
        "Query #",
        "ColdTime",
        "Min",
        "Max",
        "Mean",
        "Median",
        "Std",
        "RttMin",
        "RttMax",
        "RttAvg"
    });
    TStringStream report;
    report << "Results for " << IterationsCount << " iterations" << Endl;

    NJson::TJsonValue jsonReport(NJson::JSON_ARRAY);
    const bool collectJsonSensors = !JsonReportFileName.empty();
    const auto qtokens = workloadGen.GetWorkload(Type);
    bool allOkay = true;

    std::map<ui32, TTestInfo> queryRuns;
    auto qIter = qtokens.cbegin();
    for (ui32 queryN = 0; queryN < qtokens.size(); ++queryN, ++qIter) {
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
        Cerr << "Query text:\n" << Endl;
        Cerr << query << Endl << Endl;

        ui32 successIteration = 0;
        ui32 failsCount = 0;
        ui32 diffsCount = 0;
        std::optional<TString> prevResult;
        for (ui32 i = 0; i < IterationsCount * 10 && successIteration < IterationsCount; ++i) {
            auto t1 = TInstant::Now();
            TQueryBenchmarkResult res = TQueryBenchmarkResult::Error("undefined");
            try {
                res = Execute(query, client);
            } catch (...) {
                res = TQueryBenchmarkResult::Error(CurrentExceptionMessage());
            }
            auto duration = TInstant::Now() - t1;

            Cout << "\titeration " << i << ":\t";
            if (!!res) {
                Cout << "ok\t" << duration << " seconds" << Endl;
                clientTimings.emplace_back(duration);
                serverTimings.emplace_back(res.GetServerTiming());
                ++successIteration;
                if (successIteration == 1) {
                    outFStream << queryN << ": " << Endl
                        << res.GetYSONResult() << Endl << Endl;
                }
                if ((!prevResult || *prevResult != res.GetYSONResult()) && !res.GetQueryResult().IsExpected(qInfo.ExpectedResult)) {
                    outFStream << queryN << ":" << Endl <<
                        "Query text:" << Endl <<
                        query << Endl << Endl <<
                        "UNEXPECTED DIFF: " << Endl
                            << "RESULT: " << Endl << res.GetYSONResult() << Endl
                            << "EXPECTATION: " << Endl << qInfo.ExpectedResult << Endl;
                    prevResult = res.GetYSONResult();
                    ++diffsCount;
                }
            } else {
                ++failsCount;
                Cout << "failed\t" << duration << " seconds" << Endl;
                Cerr << queryN << ": " << query << Endl
                    << res.GetErrorInfo() << Endl;
                Sleep(TDuration::Seconds(1));
            }
        }

        if (successIteration != IterationsCount) {
            allOkay = false;
        }

        auto [inserted, success] = queryRuns.emplace(queryN, TTestInfo(std::move(clientTimings), std::move(serverTimings)));
        Y_ABORT_UNLESS(success);
        auto& testInfo = inserted->second;
        statTable.AddRow()
            .Column(0, Sprintf("Query%02u", queryN))
            .Column(1, Sprintf("%8.3f", 0.001 * testInfo.ColdTime.MilliSeconds()))
            .Column(2, Sprintf("%7.3f", 0.001 * testInfo.Min.MilliSeconds()))
            .Column(3, Sprintf("%7.3f", 0.001 * testInfo.Max.MilliSeconds()))
            .Column(4, Sprintf("%8.3f", 0.001 * testInfo.Mean))
            .Column(5, Sprintf("%8.3f", 0.001 * testInfo.Median))
            .Column(6, Sprintf("%7.3f", 0.001 * testInfo.Std))
            .Column(7, Sprintf("%7.3f", 0.001 * testInfo.RttMin.MilliSeconds()))
            .Column(8, Sprintf("%7.3f", 0.001 * testInfo.RttMax.MilliSeconds()))
            .Column(9, Sprintf("%7.3f", 0.001 * testInfo.RttMean));

        if (collectJsonSensors) {
            jsonReport.AppendValue(GetSensorValue("ColdTime", testInfo.ColdTime, queryN));
            jsonReport.AppendValue(GetSensorValue("Min", testInfo.Min, queryN));
            jsonReport.AppendValue(GetSensorValue("Max", testInfo.Max, queryN));
            jsonReport.AppendValue(GetSensorValue("Mean", testInfo.Mean, queryN));
            jsonReport.AppendValue(GetSensorValue("Median", testInfo.Median, queryN));
            jsonReport.AppendValue(GetSensorValue("Std", testInfo.Std, queryN));
            jsonReport.AppendValue(GetSensorValue("RttMin", testInfo.RttMin, queryN));
            jsonReport.AppendValue(GetSensorValue("RttMax", testInfo.RttMax, queryN));
            jsonReport.AppendValue(GetSensorValue("RttMean", testInfo.RttMean, queryN));
            jsonReport.AppendValue(GetSensorValue("DiffsCount", diffsCount, queryN));
            jsonReport.AppendValue(GetSensorValue("FailsCount", failsCount, queryN));
            jsonReport.AppendValue(GetSensorValue("SuccessCount", successIteration, queryN));
        }
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

    if (collectJsonSensors) {
        TOFStream jStream{JsonReportFileName};
        NJson::WriteJson(&jStream, &jsonReport, /*formatOutput*/ true);
        jStream.Finish();
        Cout << "Report saved to " << JsonReportFileName << Endl;
    }

    return allOkay;
}

int TWorkloadCommandBenchmark::DoRun(NYdbWorkload::IWorkloadQueryGenerator& workloadGen, TConfig& /*config*/) {
    if (QueryExecuterType == "scan") {
        return !RunBench(*TableClient, workloadGen);
    }
    if (QueryExecuterType == "generic") {
        return !RunBench(*QueryClient, workloadGen);
    }
    ythrow yexception() << "Incorrect executer type. Available options: \"scan\", \"generic\"." << Endl;
}

}
