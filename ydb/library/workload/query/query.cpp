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
            opts.AddLongOption('d', "data-path", "Path to workload data.")
                .RequiredArgument("PATH").StoreResult(&DataPath);
            break;
        case TWorkloadParams::ECommandType::Run:
            opts.AddLongOption('q', "query", "Query to execute. Can be used multiple times.").AppendTo(&CustomQueries);
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
        result << fInput.ReadAll() << std::endl;
    }
    return result.str();
}

TQueryInfoList TQueryGenerator::GetWorkload(int /*type*/) {
    TQueryInfoList result;
    const auto runPath = Params.GetDataPath() / "run";
    if (Params.GetDataPath().IsDefined() && runPath.IsDirectory()) {
        result.splice(result.end(), GetWorkloadFromDir(runPath, ""));
    }

    for (size_t i = 0; i < Params.GetCustomQueries().size(); ++i) {
        result.push_back(MakeQuery(Params.GetCustomQueries()[i], TStringBuilder() << "Custom" << i));
    }

    return result;
}

TVector<IWorkloadQueryGenerator::TWorkloadType> TQueryGenerator::GetSupportedWorkloadTypes() const {
    return {
        IWorkloadQueryGenerator::TWorkloadType(0, "olap", "Hard analitics queries from external source. One thread, more stats for every query.", IWorkloadQueryGenerator::TWorkloadType::EKind::Benchmark),
        IWorkloadQueryGenerator::TWorkloadType(0, "oltp", "Many light queries from external source, witch be lanch by some threads many times.", IWorkloadQueryGenerator::TWorkloadType::EKind::Workload)
    };
}

std::string TQueryGenerator::GetDDLQueries() const {
    return  GetDDLQueriesFromDir(Params.GetDataPath() / "create");
}

TQueryInfoList TQueryGenerator::GetInitialData() {
    return {};
}

TVector<std::string> TQueryGenerator::GetCleanPaths() const {
    return { Params.GetPath().c_str() };
}

} // namespace NQuery

} // namespace NYdbWorkload
