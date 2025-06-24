#include "query.h"
#include "data_generator.h"
#include <ydb/library/workload/benchmark_base/workload.h_serialized.h>
#include <util/stream/file.h>

namespace NYdbWorkload {

namespace NQuery {

void TQueryWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
    if (commandType != TWorkloadParams::ECommandType::Init) {
        TWorkloadBaseParams::ConfigureOpts(opts, commandType, workloadType);
    }
    switch (commandType) {
        default:
            break;
        case TWorkloadParams::ECommandType::Run:
        case TWorkloadParams::ECommandType::Init:
            opts.AddLongOption('q', "query", "Query to execute. Can be used multiple times.").AppendTo(&CustomQueries);
            opts.AddLongOption("suite-path", "Path to suite directory. See \"ydb workload query\" command description for more information.")
                .RequiredArgument("PATH").StoreResult(&SuitePath);
            break;
    }
}

THolder<IWorkloadQueryGenerator> TQueryWorkloadParams::CreateGenerator() const {
    return MakeHolder<TQueryGenerator>(this);
}

TWorkloadDataInitializer::TList TQueryWorkloadParams::CreateDataInitializers() const {
    return {std::make_shared<TQueryWorkloadDataInitializer>(*this)};
}

TString TQueryWorkloadParams::GetWorkloadName() const {
    return "Query";
}

TString TQueryWorkloadParams::GetDescription(ECommandType commandType, int /*workloadType*/) const {
    constexpr auto INIT_HELP = R"(Initialization of tables and their configurations.
Typically involving DDL queries from files with "sql" and "yql" extensions. These queries can also be directly specified from the command line using the "--query" parameter.

Next aliases can be used in queries:
  * {db} - absolute path in database to workload root. It is combination of --database and --path option values.

)";

    constexpr auto IMPORT_HELP = R"(Populating tables with data.
The "import" directory should contain subfolders named after each table, with files in supported data formats such as csv, tsv, csv.gz, tsv.gz or parquet.

)";

    constexpr auto RUN_HELP = R"(Run load testing.
Executing load testing using queries from files in the "run" directory or directly from the command line via the "--query" parameter.

Files with the "sql" and "yql" extensions will be used to generate queries. For each one, a canonical result can be set using a file with the same name and the additional ".result" extension. These are CSV files with headers and some additional syntax:
    * If a query has more than one result set, the result file should contain the same number of data sets, separated by empty lines.
    * The last line may be set to "...", which means the query result can have more rows, but only the first ones will be checked.
    * By default, floating-point numbers are compared with a relative accuracy of 1e-3 percent, but you can specify any absolute or relative accuracy like: "1.5+-0.01", "2.4e+10+-1%".

The canonical result will not be used unless the "--check-canonical" flag is set.

)";

    constexpr auto CLEAN_HELP = R"(Cleaning up by removing tables used for load testing.
This step only requires the database path.)";

switch (commandType) {
    default:
        return "";
    case ECommandType::Init:
        return TStringBuilder() << INIT_HELP << "There is example of init directory: https://github.com/ydb-platform/ydb/tree/main/ydb/tests/functional/tpc/data/e1/init.";
    case ECommandType::Import:
        return TStringBuilder() << IMPORT_HELP << "There is example of import directory: https://github.com/ydb-platform/ydb/tree/main/ydb/tests/functional/tpc/data/e1/import.";
    case ECommandType::Run:
        return TStringBuilder() << RUN_HELP << "There is example of run directory: https://github.com/ydb-platform/ydb/tree/main/ydb/tests/functional/tpc/data/e1/run.";
    case ECommandType::Clean:
        return TStringBuilder() << CLEAN_HELP;
    case ECommandType::Root:
        return TStringBuilder() << R"(Executes a user-defined workload consisting of multiple stages.
The user provides a directory path, referred to as a suite, which contains subdirectories for each stage. This path is specified using the "--suite-path" parameter in each command.

There is example of suite directory: https://github.com/ydb-platform/ydb/tree/main/ydb/tests/functional/tpc/data/e1.

The suite can contain up to four stages:
1. init
)" << INIT_HELP
<< R"(2. import
)" << IMPORT_HELP
<< R"(3. run
)" << RUN_HELP
<< R"(4. clean
)" << CLEAN_HELP
<< R"(

Details can be found in the description of the commands, using the "--help" option.)";
    }
}

TQueryInfo TQueryGenerator::MakeQuery(const TString& queryText, const TString& queryName) const {
    TQueryInfo result;
    TStringBuilder query;
    query << "-- Query " << queryName << Endl;
    query << "PRAGMA TablePathPrefix = \"" << Params.GetFullTableName(nullptr) << "\";" << Endl;
    query << queryText;
    result.Query = query;
    result.QueryName = queryName;
    return result;
}

TQueryInfoList TQueryGenerator::GetWorkloadFromDir(const TFsPath& dir, const TString namePrefix) const {
    TQueryInfoList result;
    TVector<TFsPath> children;
    dir.List(children);
    Sort(children, [](const TFsPath& a, const TFsPath& b) {return a.GetPath() < b.GetPath();});
    for (const auto& i : children) {
        const auto name = namePrefix ? Join(".", namePrefix, i.GetName()) : i.GetName();
        if (i.IsDirectory()) {
            result.splice(result.end(), GetWorkloadFromDir(i, name));
        }
        if (!i.IsFile() || (i.GetExtension() != "sql" && i.GetExtension() != "yql")) {
            continue;
        }
        TFileInput fInput(i.GetPath());
        result.emplace_back(MakeQuery(fInput.ReadAll(), name));
        const TFsPath expectedPath(i.GetPath() + ".result");
        if (Params.GetCheckCanonical() && expectedPath.Exists()) {
            result.back().ExpectedResult = TFileInput(expectedPath).ReadAll();
        } 
    }
    return result;
}

std::string TQueryGenerator::GetDDLQueriesFromDir(const TFsPath& dir) const {
    std::stringstream result;
    TVector<TFsPath> children;
    dir.List(children);
    for (const auto& i : children) {
        if (i.IsDirectory()) {
            result << GetDDLQueriesFromDir(i);
        }
        if (!i.IsFile() || (i.GetExtension() != "sql" && i.GetExtension() != "yql")) {
            continue;
        }
        result << "PRAGMA TablePathPrefix = \"" << Params.GetFullTableName(nullptr) << "\";" << std::endl;
        TFileInput fInput(i.GetPath());
        auto query = fInput.ReadAll();
        SubstGlobal(query, "{db}", Params.GetFullTableName(nullptr));
        result << query << std::endl;
    }
    return result.str();
}

TQueryInfoList TQueryGenerator::GetWorkload(int /*type*/) {
    TQueryInfoList result;
    const auto runPath = Params.GetSuitePath() / "run";
    if (Params.GetSuitePath().IsDefined() && runPath.IsDirectory()) {
        result.splice(result.end(), GetWorkloadFromDir(runPath, ""));
    }

    for (size_t i = 0; i < Params.GetCustomQueries().size(); ++i) {
        result.push_back(MakeQuery(Params.GetCustomQueries()[i], TStringBuilder() << "Custom" << i));
    }

    return result;
}

TVector<IWorkloadQueryGenerator::TWorkloadType> TQueryGenerator::GetSupportedWorkloadTypes() const {
    return {
        IWorkloadQueryGenerator::TWorkloadType(0, "olap", "Perform load testing.", IWorkloadQueryGenerator::TWorkloadType::EKind::Benchmark),
    };
}

std::string TQueryGenerator::GetDDLQueries() const {
    std::stringstream result;
    for (const auto& cq: Params.GetCustomQueries()) {
        result << cq.c_str() << ";" << std::endl;
    }
    result << GetDDLQueriesFromDir(Params.GetSuitePath() / "init");
    return result.str();
}

TQueryInfoList TQueryGenerator::GetInitialData() {
    return {};
}

TVector<std::string> TQueryGenerator::GetCleanPaths() const {
    if (Params.GetPath()) {
        return { Params.GetPath().c_str() };
    }
    return {};
}

} // namespace NQuery

} // namespace NYdbWorkload
