#include "query.h"
#include "data_generator.h"
#include <ydb/library/workload/benchmark_base/workload.h_serialized.h>
#include <util/stream/file.h>

namespace NYdbWorkload {

namespace NQuery {

void TQueryWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
    switch (commandType) {
        default:
            break;
        case TWorkloadParams::ECommandType::Root:
            TWorkloadBaseParams::ConfigureOpts(opts, commandType, workloadType);
            break;
        case TWorkloadParams::ECommandType::Run:
        case TWorkloadParams::ECommandType::Init:
            opts.AddLongOption('q', "query", "Query to execute. Can be used multiple times.").AppendTo(&CustomQueries);
            TWorkloadBaseParams::ConfigureOpts(opts, commandType, workloadType);
            opts.AddLongOption("suite-path", "Path to suite directory.")
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
    switch (commandType) {
    default:
        return "";

    case ECommandType::Init:
        return R"(Initialization of tables and their configurations.
Typically involving DDL queries from files with "sql" and "yql" extensions. These queries can also be directly specified from the command line using the "--query" parameter.

Next aliases can be used in queries:
  * {db} - absolute path in database to workload root. It is combination of --database and --path option values.)";

    case ECommandType::Import:
        return R"(Populating tables with data.
The "import" directory should contain subfolders named after each table, with files in supported data formats such as csv, tsv, csv.gz, or tsv.gz)";

    case ECommandType::Run:
        return R"(Run load testing.
Executing load testing using queries from files in the "run" directory or directly from the command line via the "--query" parameter.)";

    case ECommandType::Root:
        return R"(Executes a user-defined workload consisting of multiple stages.
The user provides a directory path, referred to as a suite, which contains subdirectories for each stage. This path is specified using the "--suite-path" parameter in each command.

The suite can contain up to four stages:
1. init
Initialization of tables and their configurations, typically involving DDL queries from files with "sql" and "yql" extensions. These queries can also be directly specified from the command line using the "--query" parameter of the "init" command.

2. import
Populating tables with data. The "import" directory should contain subfolders named after each table, with files in supported data formats such as csv, tsv, csv.gz, or tsv.gz.

3. run
Executing load testing using queries from files in the "run" directory or directly from the command line via the "--query" parameter.

4. clean
Cleaning up by removing tables used for load testing.
This step only requires the database path.

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
    return { Params.GetPath().c_str() };
}

} // namespace NQuery

} // namespace NYdbWorkload
