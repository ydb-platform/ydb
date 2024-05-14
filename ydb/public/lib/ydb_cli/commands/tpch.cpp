#include "tpch.h"

#include <contrib/libs/fmt/include/fmt/format.h>
#include <util/string/split.h>
#include <util/stream/file.h>
#include <util/string/strip.h>
#include <util/string/join.h>
#include <util/string/printf.h>
#include <util/folder/pathsplit.h>
#include <util/folder/path.h>

#include <library/cpp/json/json_writer.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include "benchmark_utils.h"


namespace NYdb::NConsoleClient {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NConsoleClient::BenchmarkUtils;

namespace {
    int getQueryNumber(int queryN) {
        return queryN + 1;
    }
}


TVector<TString> TTpchCommandRun::GetQueries() const {
    TVector<TString> queries;
    if (!ExternalQueriesDir.Empty()) {
        TFsPath queriesDir(ExternalQueriesDir);
        TVector<TString> queriesList;
        queriesDir.ListNames(queriesList);
        std::sort(queriesList.begin(), queriesList.end(), [](const TString& l, const TString& r) {
            auto leftNum = l.substr(1);
            auto rightNum = r.substr(1);
            return std::stoi(leftNum) < std::stoi(rightNum);
            });
        for (auto&& queryFileName : queriesList) {
            const TString expectedFileName = "q" + ::ToString(getQueryNumber(queries.size())) + ".sql";
            Y_ABORT_UNLESS(queryFileName == expectedFileName, "incorrect files naming. have to be q<number>.sql where number in [1, N], where N is requests count");
            TFileInput fInput(ExternalQueriesDir + "/" + expectedFileName);
            queries.emplace_back(fInput.ReadAll());
        }
    } else {
        queries = StringSplitter(NResource::Find("tpch_queries.sql")).SplitByString("-- end query").ToList<TString>();
    }

    for (auto& query : queries) {
        SubstGlobal(query, "{path}", TablesPath);
    }
    return queries;
}

template <typename TClient>
bool TTpchCommandRun::RunBench(TConfig& config)
{
    TOFStream outFStream{OutFilePath};

    auto driver = CreateDriver(config);
    auto client = TClient(driver);

    TStringStream report;
    report << "Results for " << IterationsCount << " iterations" << Endl;
    report << "+---------+----------+---------+---------+----------+---------+" << Endl;
    report << "| Query # | ColdTime |   Min   |   Max   |   Mean   |   Std   |" << Endl;
    report << "+---------+----------+---------+---------+----------+---------+" << Endl;

    NJson::TJsonValue jsonReport(NJson::JSON_ARRAY);
    const bool collectJsonSensors = !JsonReportFileName.empty();
    const TVector<TString> qtokens = GetQueries();
    bool allOkay = true;

    std::map<ui32, TTestInfo> QueryRuns;
    for (ui32 queryN = 0; queryN < qtokens.size(); ++queryN) {
        if (!NeedRun(queryN)) {
            continue;
        }

        if (!HasCharsInString(qtokens[queryN])) {
            continue;
        }
        const TString query = PatchQuery(qtokens[queryN]);

        std::vector<TDuration> clientTimings;
        std::vector<TDuration> serverTimings;
        clientTimings.reserve(IterationsCount);
        serverTimings.reserve(IterationsCount);

        Cout << Sprintf("Query%02u", getQueryNumber(queryN)) << ":" << Endl;
        Cerr << "Query text:\n" << Endl;
        Cerr << query << Endl << Endl;

        ui32 successIteration = 0;
        for (ui32 i = 0; i < IterationsCount * 10 && successIteration < IterationsCount; ++i) {
            auto t1 = TInstant::Now();
            auto res = Execute(query, client);
            auto duration = TInstant::Now() - t1;

            Cout << "\titeration " << i << ":\t";
            if (!!res) {
                Cout << "ok\t" << duration << " seconds" << Endl;
                clientTimings.emplace_back(duration);
                serverTimings.emplace_back(res.GetServerTiming());
                ++successIteration;
                if (successIteration == 1) {
                    outFStream << getQueryNumber(queryN) << ": " << Endl
                        << res.GetYSONResult() << Endl << Endl;
                }
            } else {
                Cout << "failed\t" << duration << " seconds" << Endl;
                Cerr << getQueryNumber(queryN) << ": " << query << Endl
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

        report << Sprintf("|   %02u    | %8.3f | %7.3f | %7.3f | %8.3f | %7.3f |", getQueryNumber(queryN),
            testInfo.ColdTime.MilliSeconds() * 0.001, testInfo.Min.MilliSeconds() * 0.001, testInfo.Max.MilliSeconds() * 0.001,
            testInfo.Mean * 0.001, testInfo.Std * 0.001) << Endl;
        if (collectJsonSensors) {
            jsonReport.AppendValue(GetSensorValue("ColdTime", testInfo.ColdTime, getQueryNumber(queryN)));
            jsonReport.AppendValue(GetSensorValue("Min", testInfo.Min, getQueryNumber(queryN)));
            jsonReport.AppendValue(GetSensorValue("Max", testInfo.Max, getQueryNumber(queryN)));
            jsonReport.AppendValue(GetSensorValue("Mean", testInfo.Mean, getQueryNumber(queryN)));
            jsonReport.AppendValue(GetSensorValue("Std", testInfo.Std, getQueryNumber(queryN)));
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
                jStream << testInfo.ServerTimings.at(rowId).MilliSeconds();
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


TString TTpchCommandRun::PatchQuery(const TStringBuf& original) const {
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


bool TTpchCommandRun::NeedRun(const ui32 queryIdx) const {
    if (QueriesToRun.size() && !QueriesToRun.contains(queryIdx)) {
        return false;
    }
    if (QueriesToSkip.contains(queryIdx)) {
        return false;
    }
    return true;
}


TTpchCommandInit::TTpchCommandInit()
    : TYdbCommand("init", {"i"}, "Initialize tables")
{}

void TTpchCommandInit::Config(TConfig& config) {
    NYdb::NConsoleClient::TClientCommand::Config(config);
    config.SetFreeArgsNum(0);
    config.Opts->AddLongOption('p', "path", "Folder name to create tables in")
        .Optional()
        .DefaultValue("")
        .Handler1T<TStringBuf>([this](TStringBuf arg) {
            if (arg.StartsWith('/')) {
                ythrow NLastGetopt::TUsageException() << "Path must be relative";
            }
            TablesPath = arg;
        });
    config.Opts->AddLongOption("store", "Storage type."
            " Options: row, column, s3\n"
            "row - use row-based storage engine;\n"
            "column - use column-based storage engine.\n"
            "s3 - use cloud tpc bucket")
        .DefaultValue("row").StoreResult(&StoreType);
    config.Opts->AddLongOption("s3-prefix", "Root path to TPC-H dataset in s3 storage")
        .Optional()
        .StoreResult(&S3Prefix);
    config.Opts->AddLongOption('e', "s3-endpoint", "Endpoint of S3 bucket with TPC-H dataset")
        .Optional()
        .StoreResult(&S3Endpoint);
};

void TTpchCommandInit::SetPartitionByCols(TString& createSql) {
    if (StoreType == "column") {
        SubstGlobal(createSql, "{partition_by}", "PARTITION BY HASH");
    } else {
        SubstGlobal(createSql, "{partition_by}", "--");
    }
}

int TTpchCommandInit::Run(TConfig& config) {
    StoreType = to_lower(StoreType);
    TString storageType = "-- ";
    TString notNull = "";
    TString createExternalDataSource;
    TString external;
    TString partitioning = "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT";
    TString primaryKey = ", PRIMARY KEY";
    if (StoreType == "column") {
        storageType = "STORE = COLUMN, --";
        notNull = "NOT NULL";
    } else if (StoreType == "s3") {
        storageType = fmt::format(R"(DATA_SOURCE = "{}_tpc_s3_external_source", FORMAT = "parquet", LOCATION = )", TablesPath);
        notNull = "NOT NULL";
        createExternalDataSource = fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `{}_tpc_s3_external_source` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{}",
                AUTH_METHOD="NONE"
            );
        )", TablesPath, S3Endpoint);
        external = "EXTERNAL";
        partitioning = "--";
        primaryKey = "--";
    } else if (StoreType != "row") {
        throw yexception() << "Incorrect storage type. Available options: \"row\", \"column\"." << Endl;
    }

    auto driver = CreateDriver(config);

    TString createSql = NResource::Find("tpch_schema.sql");
    TTableClient client(driver);

    SubstGlobal(createSql, "{createExternal}", createExternalDataSource);
    SubstGlobal(createSql, "{external}", external);
    SubstGlobal(createSql, "{notnull}", notNull);
    SubstGlobal(createSql, "{partitioning}", partitioning);
    SubstGlobal(createSql, "{path}", TablesPath);
    SubstGlobal(createSql, "{primary_key}", primaryKey);
    SubstGlobal(createSql, "{s3_prefix}", S3Prefix);
    SubstGlobal(createSql, "{store}", storageType);
    SetPartitionByCols(createSql);

    Cout << createSql << Endl;

    ThrowOnError(client.RetryOperationSync([createSql](TSession session) {
        return session.ExecuteSchemeQuery(createSql).GetValueSync();
    }));

    Cout << "Tables are created." << Endl;
    driver.Stop(true);
    return 0;
};

TTpchCommandClean::TTpchCommandClean()
    : TYdbCommand("clean", {}, "Drop tables")
{}

void TTpchCommandClean::Config(TConfig& config) {
    NYdb::NConsoleClient::TClientCommand::Config(config);
    config.SetFreeArgsNum(0);
    config.Opts->AddLongOption('e', "external", "Drop tables as external. Use if initialized with external storage")
        .Optional()
        .StoreTrue(&IsExternal);
    config.Opts->AddLongOption('p', "path", "Folder name where benchmark tables are located")
        .Optional()
        .StoreResult(&TablesPath);
};

int TTpchCommandClean::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    TTableClient client(driver);

    TString dropDdl;
    for (const auto& table : Tables) {
        TString fullPath = FullTablePath(config.Database, fmt::format("{}{}", TablesPath, table));
        fmt::format_to(std::back_inserter(dropDdl), "DROP {} TABLE `{}`", IsExternal ? "EXTERNAL" : "", fullPath);

        ThrowOnError(client.RetryOperationSync([&dropDdl](TSession session) {
            return session.ExecuteSchemeQuery(dropDdl).GetValueSync();
        }));
        dropDdl.clear();
    }

    if (IsExternal) {
        TString fullPath = FullTablePath(config.Database, fmt::format("{}_tpc_s3_external_source", TablesPath));
        ThrowOnError(client.RetryOperationSync([&](TSession session) {
            return session.ExecuteSchemeQuery(fmt::format("DROP EXTERNAL DATA SOURCE `{}`;", fullPath)).GetValueSync();
        }));
    }

    Cout << "Clean succeeded." << Endl;
    driver.Stop(true);
    return 0;
};


TTpchCommandRun::TTpchCommandRun()
    : TYdbCommand("run", {"b"}, "Perform benchmark")
{}

void TTpchCommandRun::Config(TConfig& config) {
    TClientCommand::Config(config);
    config.SetFreeArgsNum(0);
    config.Opts->AddLongOption('p', "path", "Folder name where benchmark tables are located")
        .Optional()
        .DefaultValue("")
        .Handler1T<TStringBuf>([this](TStringBuf arg) {
            if (arg.StartsWith('/')) {
                ythrow NLastGetopt::TUsageException() << "Path must be relative";
            }
            TablesPath = arg;
        });
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
    config.Opts->AddLongOption("ext-queries-dir", "Directory with external queries. Naming have to be q[0-N].sql")
        .StoreResult(&ExternalQueriesDir);

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
                QueriesToRun.insert(q-1);
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
};


int TTpchCommandRun::Run(TConfig& config) {
    if (QueryExecuterType == "scan") {
        const bool okay = RunBench<NYdb::NTable::TTableClient>(config);
        return !okay;
    } else if (QueryExecuterType == "generic") {
        const bool okay = RunBench<NYdb::NQuery::TQueryClient>(config);
        return !okay;
    } else {
        ythrow yexception() << "Incorrect executer type. Available options: \"scan\", \"generic\"." << Endl;
    }
};

TCommandTpch::TCommandTpch()
    : TClientCommandTree("tpch", {}, "TPC-H workload")
{
    AddCommand(std::make_unique<TTpchCommandRun>());
    AddCommand(std::make_unique<TTpchCommandInit>());
    AddCommand(std::make_unique<TTpchCommandClean>());
}

} // namespace NYdb::NConsoleClient
