#include "external.h"
#include "data_generator.h"
#include <ydb/library/workload/benchmark_base/workload.h_serialized.h>
#include <util/stream/file.h>

namespace NYdbWorkload {

namespace NExternal {

void TExternalWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
    TWorkloadBaseParams::ConfigureOpts(opts, commandType, workloadType);
    switch (commandType) {
        default:
            break;
        case TWorkloadParams::ECommandType::Root:
            opts.AddLongOption('d', "data-path", "path to workload data")
                .Required().RequiredArgument("PATH").StoreResult(&DataPath);
            break;
        case TWorkloadParams::ECommandType::Run:
            opts.AddLongOption( "syntax", "Query syntax [" + GetEnumAllNames<EQuerySyntax>() + "].")
            .StoreResult(&Syntax).DefaultValue(Syntax);
            break;
    }
}

THolder<IWorkloadQueryGenerator> TExternalWorkloadParams::CreateGenerator() const {
    return MakeHolder<TExternalGenerator>(*this);
}

TWorkloadDataInitializer::TList TExternalWorkloadParams::CreateDataInitializers() const {
    return {std::make_shared<TExternalWorkloadDataInitializer>(*this)};
}

TString TExternalWorkloadParams::GetWorkloadName() const {
    return "External";
}


TExternalGenerator::TExternalGenerator(const TExternalWorkloadParams& params)
    : TWorkloadGeneratorBase(params)
    , Params(params)
{}

TQueryInfoList TExternalGenerator::GetWorkloadFromDir(const TFsPath& dir) const {
    TQueryInfoList result;
    TVector<TFsPath> children;
    dir.List(children);
    Sort(children, [](const TFsPath& a, const TFsPath& b) {return a.GetPath() < b.GetPath();});
    TVector<TFsPath> dirChildren; 
    for (const auto& i : children) {
        if (i.IsDirectory()) {
            result.splice(result.end(), GetWorkloadFromDir(i));
        }
        if (!i.IsFile()) {
            continue;
        }
        TStringBuilder query;
        query << "-- Query from " << i << Endl;
        switch (Params.GetSyntax()) {
            case TWorkloadBaseParams::EQuerySyntax::PG:
                query << "--!syntax_pg" << Endl;
                break;
            case TWorkloadBaseParams::EQuerySyntax::YQL:
                query << "PRAGMA TablePathPrefix = \"" << Params.GetFullTableName(nullptr) << "\";" << Endl;
                break;
        }
        TFileInput fInput(i.GetPath());
        query << fInput.ReadAll();
        const auto tableJson = GetTablesJson();
        for (const auto& table: tableJson["tables"].GetArray()) {
            const auto& tableName = table["name"].GetString();
            SubstGlobal(query, 
                TStringBuilder() << "{{" << tableName << "}}", 
                TStringBuilder() << Params.GetTablePathQuote(Params.GetSyntax()) << Params.GetPath() << "/" << tableName << Params.GetTablePathQuote(Params.GetSyntax())
            );
        }
        result.emplace_back();
        result.back().Query = query;
    }
    return result;
}

TQueryInfoList TExternalGenerator::GetWorkload(int type) {
    const auto runPath = Params.GetDataPath() / "run";
    if (type || !runPath.IsDirectory()) {
        return {};
    }
    return GetWorkloadFromDir(runPath);
}

TVector<IWorkloadQueryGenerator::TWorkloadType> TExternalGenerator::GetSupportedWorkloadTypes() const {
    return {
        IWorkloadQueryGenerator::TWorkloadType(0, "root", "Requests from external source", IWorkloadQueryGenerator::TWorkloadType::EKind::Benchmark)
    };
}

TString TExternalGenerator::GetTablesYaml() const {
    const auto createPath = Params.GetDataPath() / "create";
    if (!createPath.Exists()) {
        return {};
    }
    TVector<TFsPath> children;
    createPath.List(children);
    for (const auto& c: children) {
        if (c.IsFile() && (c.GetExtension() == "yaml" || c.GetExtension() == "yml")) {
            TFileInput fi(c.GetPath());
            return fi.ReadAll();
        }
    }
    return {};
}

TWorkloadGeneratorBase::TSpecialDataTypes TExternalGenerator::GetSpecialDataTypes() const {
    return {};
}

TQueryInfoList TExternalGenerator::GetInitialData() {
    return {};
}

} // namespace NExternal

} // namespace NYdbWorkload
