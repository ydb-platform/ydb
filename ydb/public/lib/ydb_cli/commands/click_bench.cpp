#include <util/string/split.h>
#include <util/stream/file.h>
#include <util/string/strip.h>
#include <util/string/join.h>
#include <util/string/printf.h>
#include <util/folder/pathsplit.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include "click_bench.h"


namespace NYdb::NConsoleClient {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

static const char DefaultTablePath[] = "clickbench/hits";

struct TTestInfo {
    TDuration ColdTime;
    TDuration Min;
    TDuration Max;
    double Mean = 0;
    double Std = 0;
    std::vector<TDuration> Timings;

    explicit TTestInfo(std::vector<TDuration>&& timings)
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
};

TString FullTablePath(const TString& database, const TString& table) {
    TPathSplitUnix prefixPathSplit(database);
    prefixPathSplit.AppendComponent(table);
    return prefixPathSplit.Reconstruct();
}


static void ThrowOnError(const TStatus& status) {
    if (!status.IsSuccess()) {
        ythrow yexception() << "Operation failed with status " << status.GetStatus() << ": "
                            << status.GetIssues().ToString();
    }
}

static std::pair<TString, TString> ResultToYson(NTable::TScanQueryPartIterator& it) {
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

static std::pair<TString, TString> Execute(const TString& query, NTable::TTableClient& client) {
    TStreamExecScanQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Full);
    auto it = client.StreamExecuteScanQuery(query, settings).GetValueSync();
    ThrowOnError(it);
    return ResultToYson(it);
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

}

TString TClickBenchCommandRun::GetQueries(const TString& fullTablePath) const {

    TString queries;
    if (ExternalQueries) {
        queries = ExternalQueries;
    } else if (ExternalQueriesFile) {
        TFileInput fInput(ExternalQueriesFile);
        queries = fInput.ReadAll();
    } else {
        queries = NResource::Find("click_bench_queries.sql");
    }

    SubstGlobal(queries, "{table}", "`" + fullTablePath + "`");
    return queries;
}

bool TClickBenchCommandRun::RunBench(TConfig& config)
{
    TOFStream outFStream{OutFilePath};

    auto driver = CreateDriver(config);
    auto client = NYdb::NTable::TTableClient(driver);

    TStringStream report;
    report << "Results for " << IterationsCount << " iterations" << Endl;
    report << "+---------+----------+---------+---------+----------+---------+" << Endl;
    report << "| Query # | ColdTime |   Min   |   Max   |   Mean   |   Std   |" << Endl;
    report << "+---------+----------+---------+---------+----------+---------+" << Endl;

    NJson::TJsonValue jsonReport(NJson::JSON_ARRAY);
    const bool collectJsonSensors = !JsonReportFileName.empty();
    const TString queries = GetQueries(FullTablePath(config.Database, Table));
    i32 queryN = 0;
    bool allOkay = true;

    std::map<ui32, TTestInfo> QueryRuns;
    for (auto& qtoken : StringSplitter(queries).Split(';')) {
        if (!NeedRun(++queryN)) {
            continue;
        }

        const TString query = PatchQuery(qtoken.Token());

        std::vector<TDuration> timings;
        timings.reserve(IterationsCount);

        Cout << Sprintf("Query%02u", queryN) << ":" << Endl;
        Cerr << "Query text:\n" << Endl;
        Cerr << query << Endl << Endl;

        for (ui32 i = 0; i < IterationsCount; ++i) {
            auto t1 = TInstant::Now();
            auto res = Execute(query, client);
            auto duration = TInstant::Now() - t1;
            timings.emplace_back(duration);

            Cout << "\titeration " << i << ":\t";
            if (res.second == "") {
                Cout << "ok\t" << duration << " seconds" << Endl;
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

        auto [inserted, success] = QueryRuns.emplace(queryN, TTestInfo(std::move(timings)));
        Y_VERIFY(success);
        auto& testInfo = inserted->second;

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

    driver.Stop(true);

    report << "+---------+----------+---------+---------+----------+---------+" << Endl;

    Cout << Endl << report.Str() << Endl;
    Cout << "Results saved to " << OutFilePath << Endl;

    if (MiniStatFileName) {
        TOFStream jStream{MiniStatFileName};

        for(ui32 rowId = 0; rowId < IterationsCount; ++rowId) {
            ui32 colId = 0;
            for(auto [_, testInfo] : QueryRuns) {
                if (colId) {
                    jStream << ",";
                }
                ++colId;
                jStream << testInfo.Timings.at(rowId).MilliSeconds();
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


TString TClickBenchCommandRun::PatchQuery(const TStringBuf& original) const {
    TString result(original.data(), original.size());

    if (!QuerySettings.empty()) {
        result = JoinSeq("\n", QuerySettings) + "\n" + result;
    }

    std::vector<TStringBuf> lines;
    for(auto& line : StringSplitter(result).Split('\n').SkipEmpty()) {
        if (line.StartsWith("--")) {
            continue;
        }

        lines.push_back(line);
    }

    return JoinSeq('\n', lines);
}


bool TClickBenchCommandRun::NeedRun(const ui32 queryIdx) const {
    if (QueriesToRun.size() && !QueriesToRun.contains(queryIdx)) {
        return false;
    }
    if (QueriesToSkip.contains(queryIdx)) {
        return false;
    }
    return true;
}


TClickBenchCommandInit::TClickBenchCommandInit()
    : TYdbCommand("init", {"i"}, "Initialize table")
{}

void TClickBenchCommandInit::Config(TConfig& config) {
    NYdb::NConsoleClient::TClientCommand::Config(config);
    config.SetFreeArgsNum(0);
    config.Opts->AddLongOption('p', "path", "Table name to work with")
        .Optional()
        .RequiredArgument("NAME")
        .DefaultValue(DefaultTablePath)
        .Handler1T<TStringBuf>([this](TStringBuf arg) {
            if (arg.StartsWith('/')) {
                ythrow NLastGetopt::TUsageException() << "Path must be relative";
            }
            Table = arg;
        });
    config.Opts->AddLongOption("store", "Storage type."
            " Options: row, column\n"
            "row - use row-based storage engine;\n"
            "column - use column-based storage engine.")
        .DefaultValue("column").StoreResult(&StoreType);
};

int TClickBenchCommandInit::Run(TConfig& config) {
    StoreType = to_lower(StoreType);
    TString partitionBy = "";
    TString storageType = "";
    if (StoreType == "column") {
        partitionBy = "PARTITION BY HASH(CounterID)";
        storageType = "STORE = COLUMN,";
    } else if (StoreType != "row") {
        throw yexception() << "Incorrect storage type. Available options: \"row\", \"column\"." << Endl;
    }

    auto driver = CreateDriver(config);

    TString createSql = NResource::Find("click_bench_schema.sql");
    TTableClient client(driver);

    SubstGlobal(createSql, "{table}", FullTablePath(config.Database, Table));
    SubstGlobal(createSql, "{partition}", partitionBy);
    SubstGlobal(createSql, "{store}", storageType);

    ThrowOnError(client.RetryOperationSync([createSql](TSession session) {
        return session.ExecuteSchemeQuery(createSql).GetValueSync();
    }));

    Cout << "Table created." << Endl;
    driver.Stop(true);
    return 0;
};

TClickBenchCommandClean::TClickBenchCommandClean()
    : TYdbCommand("clean", {}, "Drop table")
{}

void TClickBenchCommandClean::Config(TConfig& config) {
    NYdb::NConsoleClient::TClientCommand::Config(config);
    config.SetFreeArgsNum(0);
    config.Opts->AddLongOption('p', "path", "Table name to work with")
        .Optional()
        .RequiredArgument("NAME")
        .DefaultValue(DefaultTablePath)
        .Handler1T<TStringBuf>([this](TStringBuf arg) {
            if (arg.StartsWith('/')) {
                ythrow NLastGetopt::TUsageException() << "Path must be relative";
            }
            Table = arg;
        });
};

int TClickBenchCommandClean::Run(TConfig& config) {
    auto driver = CreateDriver(config);

    static const char DropDdlTmpl[] = "DROP TABLE `%s`;";
    char dropDdl[sizeof(DropDdlTmpl) + 8192*3]; // 32*256 for DbPath
    TString fullPath = FullTablePath(config.Database, Table);
    int res = std::sprintf(dropDdl, DropDdlTmpl, fullPath.c_str());
    if (res < 0) {
        Cerr << "Failed to generate DROP DDL query for `" << fullPath << "` table." << Endl;
        return -1;
    }
    TTableClient client(driver);

    ThrowOnError(client.RetryOperationSync([dropDdl](TSession session) {
        return session.ExecuteSchemeQuery(dropDdl).GetValueSync();
    }));

    Cout << "Clean succeeded." << Endl;
    driver.Stop(true);
    return 0;
};


TClickBenchCommandRun::TClickBenchCommandRun()
    : TYdbCommand("run", {"b"}, "Perform benchmark")
{}

void TClickBenchCommandRun::Config(TConfig& config) {
    TClientCommand::Config(config);
    config.SetFreeArgsNum(0);
    config.Opts->AddLongOption("output", "Save queries output to file")
        .Optional()
        .RequiredArgument("FILE")
        .DefaultValue("results.out")
        .StoreResult(&OutFilePath);
    config.Opts->AddLongOption("iterations", "Iterations count")
        .DefaultValue(1)
        .StoreResult(&IterationsCount);
    config.Opts->AddLongOption("json", "Json report file name")
        .DefaultValue("")
        .StoreResult(&JsonReportFileName);
    config.Opts->AddLongOption("ministat", "Ministat report file name")
        .DefaultValue("")
        .StoreResult(&MiniStatFileName);
    config.Opts->AddLongOption("query-settings", "Query settings.")
        .DefaultValue("")
        .AppendTo(&QuerySettings);
    config.Opts->AddLongOption("ext-queries-file", "File with external queries. Separated by ';'")
        .DefaultValue("")
        .StoreResult(&ExternalQueriesFile);
    config.Opts->AddLongOption("table", "Table to work with")
        .Optional()
        .RequiredArgument("NAME")
        .DefaultValue(DefaultTablePath)
        .Handler1T<TStringBuf>([this](TStringBuf arg) {
            if (arg.StartsWith('/')) {
                ythrow NLastGetopt::TUsageException() << "Path must be relative";
            }
            Table = arg;
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
    const bool okay = RunBench(config);
    return !okay;
};

TCommandClickBench::TCommandClickBench()
    : TClientCommandTree("clickbench", {}, "ClickBench workload (ClickHouse OLAP test)")
{
    AddCommand(std::make_unique<TClickBenchCommandRun>());
    AddCommand(std::make_unique<TClickBenchCommandInit>());
    AddCommand(std::make_unique<TClickBenchCommandClean>());
}

} // namespace NYdb::NConsoleClient
