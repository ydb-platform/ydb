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
            opts.AddLongOption('d', "data-path", "path to workload data")
                .Required().RequiredArgument("PATH").StoreResult(&DataPath);
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


TQueryInfoList TQueryGenerator::GetWorkloadFromDir(const TFsPath& dir) const {
    TQueryInfoList result;
    TVector<TFsPath> children;
    dir.List(children);
    Sort(children, [](const TFsPath& a, const TFsPath& b) {return a.GetPath() < b.GetPath();});
    for (const auto& i : children) {
        if (i.IsDirectory()) {
            result.splice(result.end(), GetWorkloadFromDir(i));
        }
        if (!i.IsFile() || (i.GetExtension() != "sql" && i.GetExtension() != "yql")) {
            continue;
        }
        TStringBuilder query;
        query << "-- Query from " << i << Endl;
        query << "PRAGMA TablePathPrefix = \"" << Params.GetFullTableName(nullptr) << "\";" << Endl;
        TFileInput fInput(i.GetPath());
        query << fInput.ReadAll();
        result.emplace_back();
        result.back().Query = query;
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

TQueryInfoList TQueryGenerator::GetWorkload(int type) {
    const auto runPath = Params.GetDataPath() / "run";
    if (type || !runPath.IsDirectory()) {
        return {};
    }
    return GetWorkloadFromDir(runPath);
}

TVector<IWorkloadQueryGenerator::TWorkloadType> TQueryGenerator::GetSupportedWorkloadTypes() const {
    return {
        IWorkloadQueryGenerator::TWorkloadType(0, "root", "Requests from external source", IWorkloadQueryGenerator::TWorkloadType::EKind::Benchmark)
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
