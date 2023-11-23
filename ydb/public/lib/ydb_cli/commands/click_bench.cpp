#include <util/string/split.h>
#include <util/stream/file.h>
#include <util/string/strip.h>
#include <util/string/join.h>
#include <util/string/printf.h>
#include <util/folder/pathsplit.h>
#include <util/folder/path.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include "click_bench.h"
#include "benchmark_utils.h"


namespace NYdb::NConsoleClient {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NConsoleClient::BenchmarkUtils;

namespace {

static const char DefaultTablePath[] = "clickbench/hits";

class TExternalVariable {
private:
    TString Id;
    TString Value;
public:
    TExternalVariable() = default;

    TExternalVariable(const TString& id, const TString& value)
        : Id(id)
        , Value(value) {

    }

    const TString& GetId() const {
        return Id;
    }

    const TString& GetValue() const {
        return Value;
    }

    bool DeserializeFromString(const TString& vStr) {
        TStringBuf sb(vStr.data(), vStr.size());
        TStringBuf l, r;
        if (!sb.TrySplit('=', l, r)) {
            Cerr << "Incorrect variables format: have to be a=b, but really have: " << sb << Endl;
            return false;
        }
        Id = l;
        Value = r;
        return true;
    }
};

}

bool TClickBenchCommandRun::TQueryFullInfo::CompareValue(const NYdb::TValue& v, const TStringBuf vExpected) const {
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

bool TClickBenchCommandRun::TQueryFullInfo::IsCorrectResult(const BenchmarkUtils::TQueryResultInfo& resultFull) const {
    if (!ExpectedResult) {
        return true;
    }
    const auto expectedLines = StringSplitter(ExpectedResult).Split('\n').SkipEmpty().ToList<TString>();
    auto& result = resultFull.GetResult();
    if (result.size() + 1 != expectedLines.size()) {
        Cerr << "has diff: incorrect lines count (" << result.size() << " in result, but " << expectedLines.size() << " expected with header)" << Endl;
        return false;
    }

    std::vector<ui32> columnIndexes;
    {
        const std::map<TString, ui32> columns = resultFull.GetColumnsRemap();
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
            } else {
                columnIndexes.emplace_back(it->second);
            }

            if (!splitter.Step()) {
                break;
            }
        }
        if (columnIndexes.size() != columns.size()) {
            Cerr << "there are unexpected columns in result" << Endl;
            return false;
        }
    }

    for (ui32 i = 0; i < result.size(); ++i) {
        TString copy = expectedLines[i + 1];
        NCsvFormat::CsvSplitter splitter(copy);
        bool isCorrectCurrent = true;
        for (ui32 cIdx = 0; cIdx < columnIndexes.size(); ++cIdx) {
            const NYdb::TValue& resultValue = result[i][columnIndexes[cIdx]];
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

TVector<TClickBenchCommandRun::TQueryFullInfo> TClickBenchCommandRun::GetQueries(const TString& fullTablePath) const {
    TVector<TString> queries;
    const TMap<ui32, TString> qResults = LoadExternalResults();
    if (ExternalQueries) {
        queries = StringSplitter(ExternalQueries).Split(';').ToList<TString>();
    } else if (ExternalQueriesFile) {
        TFileInput fInput(ExternalQueriesFile);
        queries = StringSplitter(fInput.ReadAll()).Split(';').ToList<TString>();
    } else if (ExternalQueriesDir) {
        TFsPath queriesDir(ExternalQueriesDir);
        TVector<TString> queriesList;
        queriesDir.ListNames(queriesList);
        std::sort(queriesList.begin(), queriesList.end());
        for (auto&& i : queriesList) {
            const TString expectedFileName = "q" + ::ToString(queries.size()) + ".sql";
            Y_ABORT_UNLESS(i == expectedFileName, "incorrect files naming. have to be q<number>.sql where number in [0, N - 1], where N is requests count");
            TFileInput fInput(ExternalQueriesDir + "/" + expectedFileName);
            queries.emplace_back(fInput.ReadAll());
        }
    } else {
        queries = StringSplitter(NResource::Find("click_bench_queries.sql")).Split(';').ToList<TString>();
    }
    auto strVariables = StringSplitter(ExternalVariablesString).Split(';').SkipEmpty().ToList<TString>();
    TVector<TExternalVariable> vars;
    for (auto&& i : strVariables) {
        TExternalVariable v;
        Y_ABORT_UNLESS(v.DeserializeFromString(i));
        vars.emplace_back(v);
    }
    vars.emplace_back("table", "`" + fullTablePath + "`");
    for (auto&& i : queries) {
        for (auto&& v : vars) {
            SubstGlobal(i, "{" + v.GetId() + "}", v.GetValue());
        }
    }
    TVector<TQueryFullInfo> result;
    ui32 resultsUsage = 0;
    for (ui32 i = 0; i < queries.size(); ++i) {
        auto it = qResults.find(i);
        if (it != qResults.end()) {
            ++resultsUsage;
            result.emplace_back(queries[i], it->second);
        } else {
            result.emplace_back(queries[i], "");
        }
    }
    Y_ABORT_UNLESS(resultsUsage == qResults.size(), "there are unused files with results in directory");
    return result;
}

template <typename TClient>
bool TClickBenchCommandRun::RunBench(TConfig& config)
{
    TOFStream outFStream{OutFilePath};

    auto driver = CreateDriver(config);
    auto client = TClient(driver);

    TStringStream report;
    report << "Results for " << IterationsCount << " iterations" << Endl;
    report << "+---------+----------+---------+---------+----------+----------+---------+---------+---------+---------+" << Endl;
    report << "| Query # | ColdTime |   Min   |   Max   |   Mean   |  Median  |   Std   |  RttMin |  RttMax |  RttAvg |" << Endl;
    report << "+---------+----------+---------+---------+----------+----------+---------+---------+---------+---------+" << Endl;

    NJson::TJsonValue jsonReport(NJson::JSON_ARRAY);
    const bool collectJsonSensors = !JsonReportFileName.empty();
    const TVector<TQueryFullInfo> qtokens = GetQueries(FullTablePath(config.Database, Table));
    bool allOkay = true;

    std::map<ui32, TTestInfo> QueryRuns;
    for (ui32 queryN = 0; queryN < qtokens.size(); ++queryN) {
        const TQueryFullInfo& qInfo = qtokens[queryN];
        if (!NeedRun(queryN)) {
            continue;
        }

        if (!HasCharsInString(qInfo.GetQuery())) {
            continue;
        }

        const TString query = PatchQuery(qInfo.GetQuery());

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
                if ((!prevResult || *prevResult != res.GetYSONResult()) && !qInfo.IsCorrectResult(res.GetQueryResult())) {
                    outFStream << queryN << ":" << Endl <<
                        "Query text:" << Endl <<
                        query << Endl << Endl <<
                        "UNEXPECTED DIFF: " << Endl
                            << "RESULT: " << Endl << res.GetYSONResult() << Endl
                            << "EXPECTATION: " << Endl << qInfo.GetExpectedResult() << Endl;
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

        auto [inserted, success] = QueryRuns.emplace(queryN, TTestInfo(std::move(clientTimings), std::move(serverTimings)));
        Y_ABORT_UNLESS(success);
        auto& testInfo = inserted->second;

        report << Sprintf("|   %02u    | %8.3f | %7.3f | %7.3f | %8.3f | %8.3f | %7.3f | %7.3f | %7.3f | %7.3f |", queryN,
            testInfo.ColdTime.MilliSeconds() * 0.001, testInfo.Min.MilliSeconds() * 0.001, testInfo.Max.MilliSeconds() * 0.001,
            testInfo.Mean * 0.001, testInfo.Median * 0.001, testInfo.Std * 0.001, testInfo.RttMin.MilliSeconds() * 0.001, testInfo.RttMax.MilliSeconds() * 0.001,
            testInfo.RttMean * 0.001) << Endl;
        if (collectJsonSensors) {
            jsonReport.AppendValue(GetSensorValue("ColdTime", testInfo.ColdTime, queryN));
            jsonReport.AppendValue(GetSensorValue("Min", testInfo.Min, queryN));
            jsonReport.AppendValue(GetSensorValue("Max", testInfo.Max, queryN));
            jsonReport.AppendValue(GetSensorValue("Mean", testInfo.Mean, queryN));
            jsonReport.AppendValue(GetSensorValue("Median", testInfo.Median, queryN));
            jsonReport.AppendValue(GetSensorValue("Std", testInfo.Std, queryN));
            jsonReport.AppendValue(GetSensorValue("DiffsCount", diffsCount, queryN));
            jsonReport.AppendValue(GetSensorValue("FailsCount", failsCount, queryN));
            jsonReport.AppendValue(GetSensorValue("SuccessCount", successIteration, queryN));
        }
    }

    driver.Stop(true);

    report << "+---------+----------+---------+---------+----------+----------+---------+---------+---------+---------+" << Endl;

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
        .DefaultValue("row").StoreResult(&StoreType);
};

int TClickBenchCommandInit::Run(TConfig& config) {
    StoreType = to_lower(StoreType);
    TString partitionBy = "";
    TString storageType = "";
    TString notNull = "";
    if (StoreType == "column") {
        //partitionBy = "PARTITION BY HASH(CounterID)"; Not enough cardinality in CounterID column @sa KIKIMR-16478
        partitionBy = "PARTITION BY HASH(EventTime)";
        storageType = "STORE = COLUMN,";
        notNull = "NOT NULL";
    } else if (StoreType != "row") {
        throw yexception() << "Incorrect storage type. Available options: \"row\", \"column\"." << Endl;
    }

    auto driver = CreateDriver(config);

    TString createSql = NResource::Find("click_bench_schema.sql");
    TTableClient client(driver);

    SubstGlobal(createSql, "{table}", FullTablePath(config.Database, Table));
    SubstGlobal(createSql, "{notnull}", notNull);
    SubstGlobal(createSql, "{partition}", partitionBy);
    SubstGlobal(createSql, "{store}", storageType);

    ThrowOnError(client.RetryOperationSync([createSql](TSession session) {
        return session.ExecuteSchemeQuery(createSql).GetValueSync();
    }));

    Cout << "Table created. Please, follow instructions https://ydb.tech/en/docs/reference/ydb-cli/workload-click-bench#load to load benchmark data." << Endl;
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
    config.Opts->AddLongOption("ext-queries-dir", "Directory with external queries. Naming have to be q[0-N].sql")
        .DefaultValue("")
        .StoreResult(&ExternalQueriesDir);
    config.Opts->AddLongOption("ext-results-dir", "Directory with external results. Naming have to be q[0-N].sql")
        .DefaultValue("")
        .StoreResult(&ExternalResultsDir);
    TString externalVariables;
    config.Opts->AddLongOption("ext-query-variables", "v1_id=v1_value;v2_id=v2_value;...; applied for queries {v1_id} -> v1_value")
        .DefaultValue("")
        .StoreResult(&ExternalVariablesString);
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

    config.Opts->AddLongOption("executor", "Query executor type."
            " Options: scan, generic\n"
            "scan - use scan queries;\n"
            "generic - use generic queries.")
        .DefaultValue("scan").StoreResult(&QueryExecutorType);
};


int TClickBenchCommandRun::Run(TConfig& config) {
    if (QueryExecutorType == "scan") {
        const bool okay = RunBench<NYdb::NTable::TTableClient>(config);
        return !okay;
    } else if (QueryExecutorType == "generic") {
        const bool okay = RunBench<NYdb::NQuery::TQueryClient>(config);
        return !okay;
    } else {
        ythrow yexception() << "Incorrect executor type. Available options: \"scan\", \"generic\"." << Endl;
    }
};

TMap<ui32, TString> TClickBenchCommandRun::LoadExternalResults() const {
    TMap<ui32, TString> result;
    if (ExternalResultsDir) {
        TFsPath dir(ExternalResultsDir);
        TVector<TString> filesList;
        dir.ListNames(filesList);
        std::sort(filesList.begin(), filesList.end());
        for (auto&& i : filesList) {
            Y_ABORT_UNLESS(i.StartsWith("q") && i.EndsWith(".result"));
            TStringBuf sb(i.data(), i.size());
            sb.Skip(1);
            sb.Chop(7);
            ui32 qId;
            Y_ABORT_UNLESS(TryFromString<ui32>(sb, qId));
            TFileInput fInput(ExternalResultsDir + "/" + i);
            result.emplace(qId, fInput.ReadAll());
        }
    }
    return result;
}

TCommandClickBench::TCommandClickBench()
    : TClientCommandTree("clickbench", {}, "ClickBench workload (ClickHouse OLAP test)")
{
    AddCommand(std::make_unique<TClickBenchCommandRun>());
    AddCommand(std::make_unique<TClickBenchCommandInit>());
    AddCommand(std::make_unique<TClickBenchCommandClean>());
}

} // namespace NYdb::NConsoleClient
