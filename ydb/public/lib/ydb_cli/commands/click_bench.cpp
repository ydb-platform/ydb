#include <util/string/split.h>
#include <util/stream/file.h>
#include <util/string/strip.h>
#include <util/string/printf.h>
#include <util/folder/pathsplit.h>
#include <library/cpp/json/json_writer.h>

#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/string_utils/base64/base64.h>

#include "click_bench.h"

using namespace NYdb;
using namespace NYdb::NTable;


static void ThrowOnError(const TStatus& status) {
    if (!status.IsSuccess()) {
        ythrow yexception() << "Operation failed with status " << status.GetStatus() << ": "
                            << status.GetIssues().ToString();
    }
}

static NJson::TJsonValue GetQueryLabels(ui32 queryId) {
    NJson::TJsonValue labels(NJson::JSON_MAP);
    labels.InsertValue("query", Sprintf("Query%02u", queryId));
    return labels;
}

static NJson::TJsonValue GetSensorValue(TStringBuf sensor, TDuration& value, ui32 queryId) {
    NJson::TJsonValue sensorValue(NJson::JSON_MAP);
    sensorValue.InsertValue("sensor", sensor);
    sensorValue.InsertValue("value", value.MilliSeconds());
    sensorValue.InsertValue("labels", GetQueryLabels(queryId));
    return sensorValue;
}

static NJson::TJsonValue GetSensorValue(TStringBuf sensor, double value, ui32 queryId) {
    NJson::TJsonValue sensorValue(NJson::JSON_MAP);
    sensorValue.InsertValue("sensor", sensor);
    sensorValue.InsertValue("value", value);
    sensorValue.InsertValue("labels", GetQueryLabels(queryId));
    return sensorValue;
}

void TClickHouseBench::Init() {
    TString createSql = NResource::Find("click_bench_schema.sql");
    TTableClient client(Driver);

    SubstGlobal(createSql, "{table}", FullTablePath());
    ThrowOnError(client.RetryOperationSync([createSql](TSession session) {
        return session.ExecuteSchemeQuery(createSql).GetValueSync();
    }));
}


TClickHouseBench::TTestInfo TClickHouseBench::AnalyzeTestRuns(const TVector<TDuration>& timings) {
    TTestInfo info;

    if (timings.empty()) {
        return info;
    }

    info.ColdTime = timings[0];

    if (timings.size() > 1) {
        ui32 sum = 0;
        for (size_t j = 1; j < timings.size(); ++j) {
            if (info.Max < timings[j]) {
                info.Max = timings[j];
            }
            if (!info.Min || info.Min > timings[j]) {
                info.Min = timings[j];
            }
            sum += timings[j].MilliSeconds();
        }
        info.Mean = (double) sum / (double) (timings.size() - 1);
        if (timings.size() > 2) {
            double variance = 0;
            for (size_t j = 1; j < timings.size(); ++j) {
                variance += (info.Mean - timings[j].MilliSeconds()) * (info.Mean - timings[j].MilliSeconds());
            }
            variance = variance / (double) (timings.size() - 2);
            info.Std = sqrt(variance);
        }
    }

    return info;
}

TClickBenchCommandInit::TClickBenchCommandInit()
    : TYdbCommand("init", {"i"}, "Initialize table")
{}

void TClickBenchCommandInit::Config(TConfig& config) {
    NYdb::NConsoleClient::TClientCommand::Config(config);
    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption("table", "Table name to work with")
        .Optional()
        .RequiredArgument("NAME")
        .DefaultValue("hits")
        .StoreResult(&Table);
    config.Opts->AddLongOption('p', "path", "Relative path to table")
        .Optional()
        .RequiredArgument("PATH")
        .Handler1T<TStringBuf>([this](TStringBuf arg) {
            if (arg.StartsWith('/')) {
                ythrow NLastGetopt::TUsageException() << "Path must be relative";
            }
            Path = arg;
        });
};

int TClickBenchCommandInit::Run(TConfig& config) {
    auto driver = CreateDriver(config);

    TClickHouseBench chb(driver, config.Database, Path, Table);
    chb.Init();
    Cout << "Table created" << Endl;
    driver.Stop(true);
    return 0;
};


TClickBenchCommandRun::TClickBenchCommandRun()
    : TYdbCommand("run", {"b"}, "Perform benchmark")
{}

void TClickBenchCommandRun::Config(TConfig& config) {
    TClientCommand::Config(config);
    config.SetFreeArgsNum(0);
    config.Opts->AddLongOption('o', "output", "Save queries output to file")
        .Optional()
        .RequiredArgument("FILE")
        .DefaultValue("results.out")
        .StoreResult(&OutFilePath);
    config.Opts->AddLongOption('n', "iterations", "Iterations count (without cold-start run)")
        .DefaultValue(0)
        .StoreResult(&IterationsCount);
    config.Opts->AddLongOption('j', "json", "Json report file name")
        .DefaultValue("")
        .StoreResult(&JsonReportFileName);
    config.Opts->AddLongOption("disable-llvm", "disable llvm")
        .NoArgument()
        .SetFlag(&DisableLlvm);
    config.Opts->AddLongOption("enable-pushdown", "enabled pushdown")
        .NoArgument()
        .SetFlag(&EnablePushdown);
    config.Opts->AddLongOption('f', "ext-queries-file", "File with external queries. Separated by ';'")
        .DefaultValue("")
        .StoreResult(&ExternalQueriesFile);
    config.Opts->AddLongOption("table", "Table name to work with")
        .Optional()
        .RequiredArgument("NAME")
        .DefaultValue("hits")
        .StoreResult(&Table);
    config.Opts->AddLongOption("path", "Relative path to table")
        .Optional()
        .RequiredArgument("PATH")
        .Handler1T<TStringBuf>([this](TStringBuf arg) {
            if (arg.StartsWith('/')) {
                ythrow NLastGetopt::TUsageException() << "Path must be relative";
            }
            Path = arg;
        });
    config.Opts->AddLongOption('q', "ext-query", "String with external queries. Separated by ';'")
        .DefaultValue("")
        .StoreResult(&ExternalQueries);

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
        "Run only specified queries (ex.: 1,2,3,5-10,20)")
        .Optional()
        .Handler1T<TStringBuf>([this, fillTestCases](TStringBuf line) {
            QueriesToRun.clear();
            fillTestCases(line, [this](ui32 q) {
                QueriesToRun.insert(q);
            });
        });
    auto excludeOpt = config.Opts->AddLongOption("exclude",
        "Run all queries except given ones (ex.: 1,2,3,5-10,20)")
        .Optional()
        .Handler1T<TStringBuf>([this, fillTestCases](TStringBuf line) {
            fillTestCases(line, [this](ui32 q) {
                QueriesToSkip.emplace(q);
            });
        });

    config.Opts->MutuallyExclusiveOpt(includeOpt, excludeOpt);
};


int TClickBenchCommandRun::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    TClickHouseBench chb(driver, config.Database, Path, Table);
    std::unique_ptr<IQueryRunner> runner;
    runner = std::make_unique<TStreamQueryRunner>(driver);
    const bool okay = chb.RunBench(*runner, *this);
    driver.Stop(true);
    return !okay;
};


bool TClickHouseBench::RunBench(IQueryRunner& queryRunner, const TBenchContext& context)
{
    TOFStream outFStream{context.OutFilePath};

    TStringStream report;
    report << "Results for " << (context.IterationsCount + 1) << " iterations" << Endl;
    report << "+---------+----------+---------+---------+----------+---------+" << Endl;
    report << "| Query # | ColdTime |   Min   |   Max   |   Mean   |   Std   |" << Endl;
    report << "+---------+----------+---------+---------+----------+---------+" << Endl;

    NJson::TJsonValue jsonReport(NJson::JSON_ARRAY);
    const bool collectJsonSensors = !context.JsonReportFileName.empty();
    const TString queries = queryRunner.GetQueries(FullTablePath(), context.GetExternalQueries());
    i32 queryN = 0;
    bool allOkay = true;
    for (auto& qtoken : StringSplitter(queries).Split(';')) {
        if (!context.NeedRun(++queryN)) {
            continue;
        }
        const TString query = context.PatchQuery(qtoken.Token());

        TVector<TDuration> timings;
        timings.reserve(1 + context.IterationsCount);

        Cout << Sprintf("Query%02u", queryN) << ":" << Endl;
        Cerr << "Query text:\n" << Endl;
        Cerr << query << Endl << Endl;

        for (ui32 i = 0; i <= context.IterationsCount; ++i) {
            auto t1 = TInstant::Now();
            auto res = queryRunner.Execute(query);

            auto duration = TInstant::Now() - t1;

            Cout << "\titeration " << i << ":\t";
            if (res.second == "") {
                Cout << "ok\t" << duration << " seconds" << Endl;
                timings.emplace_back(duration);
            } else {
                allOkay = false;
                Cout << "failed\t" << duration << " seconds" << Endl;
                Cerr << queryN << ": " << query << Endl
                     << res.first << res.second << Endl;
            }

            if (i == 0) {
                outFStream << queryN << ": " << Endl
                           << res.first << res.second << Endl << Endl;
            }
        }

        auto testInfo = AnalyzeTestRuns(timings);
        report << Sprintf("|   %02u    | %8zu | %7zu | %7.zu | %8.2f | %7.2f |", queryN,
            testInfo.ColdTime.MilliSeconds(), testInfo.Min.MilliSeconds(), testInfo.Max.MilliSeconds(),
            testInfo.Mean, testInfo.Std) << Endl;
        if (collectJsonSensors) {
            jsonReport.AppendValue(GetSensorValue("ColdTime", testInfo.ColdTime, queryN));
            jsonReport.AppendValue(GetSensorValue("Min", testInfo.Min, queryN));
            jsonReport.AppendValue(GetSensorValue("Max", testInfo.Max, queryN));
            jsonReport.AppendValue(GetSensorValue("Mean", testInfo.Mean, queryN));
            jsonReport.AppendValue(GetSensorValue("Std", testInfo.Std, queryN));
        }
    }

    report << "+---------+----------+---------+---------+----------+---------+" << Endl;

    Cout << Endl << report.Str() << Endl;
    Cout << "Results saved to " << context.OutFilePath << Endl;

    if (collectJsonSensors) {
        TOFStream jStream{context.JsonReportFileName};
        NJson::WriteJson(&jStream, &jsonReport, /*formatOutput*/ true);
        jStream.Finish();
        Cout << "Report saved to " << context.JsonReportFileName << Endl;
    }

    return allOkay;
}

TString TClickHouseBench::FullTablePath() const {
    TPathSplitUnix prefixPathSplit(Database);
    prefixPathSplit.AppendComponent(Path);
    prefixPathSplit.AppendComponent(Table);
    return prefixPathSplit.Reconstruct();
}


TString TStreamQueryRunner::GetQueries(const TString& fullTablePath, const TString& externalQueries) {
    TString queries = externalQueries ? externalQueries : NResource::Find("click_bench_queries.sql");
    SubstGlobal(queries, "{table}", "`" + fullTablePath + "`");
    return queries;
}

std::pair<TString, TString> TStreamQueryRunner::Execute(const TString& query) {
    TStreamExecScanQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Full);
    auto it = SqClient.StreamExecuteScanQuery(query, settings).GetValueSync();
    ThrowOnError(it);
    return ResultToYson(it);
}

std::pair<TString, TString> TStreamQueryRunner::ResultToYson(NTable::TScanQueryPartIterator& it) {
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
