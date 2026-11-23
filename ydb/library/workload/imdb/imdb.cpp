#include "imdb.h"
#include "data_generator.h"

#include <library/cpp/resource/resource.h>
#include <util/string/escape.h>
#include <util/string/split.h>

namespace NYdbWorkload {

TImdbWorkloadParams::TImdbWorkloadParams() {
    SetPath("imdb");
}

THolder<IWorkloadQueryGenerator> TImdbWorkloadParams::CreateGenerator() const {
    return MakeHolder<TImdbWorkloadGenerator>(*this);
}

TWorkloadDataInitializer::TList TImdbWorkloadParams::CreateDataInitializers() const {
    return {std::make_shared<TImdbWorkloadDataInitializer>(*this)};
}

TString TImdbWorkloadParams::GetWorkloadName() const {
    return "IMDb";
}

TImdbWorkloadGenerator::TImdbWorkloadGenerator(const TImdbWorkloadParams& params)
    : TWorkloadGeneratorBase(params)
    , Params(params)
{}

TString TImdbWorkloadGenerator::GetTablesYaml() const {
    return NResource::Find("imdb/schema.yaml");
}

TWorkloadGeneratorBase::TSpecialDataTypes TImdbWorkloadGenerator::GetSpecialDataTypes() const {
    return {};
}

TQueryInfoList TImdbWorkloadGenerator::GetInitialData() {
    return {};
}

TVector<TString> TImdbWorkloadGenerator::GetAnalyzeQueries() const {
    TVector<TString> queries;
    const auto tables = GetTablesJson();
    for (const auto& table: tables["tables"].GetArray()) {
        auto path = EscapeC(Params.GetFullTableName(table["name"].GetString().c_str()));
        SubstGlobal(path, "`", "\\`");
        queries.emplace_back(TStringBuilder() << "ANALYZE `" << path << "`;");
    }
    return queries;
}

TQueryInfoList TImdbWorkloadGenerator::GetWorkload(int type) {
    TQueryInfoList result;
    if (type != 0) {
        return result;
    }
    if (Params.GetCheckCanonical()) {
        throw yexception() << "Canonical results are not available for the IMDb workload.";
    }
    const auto names = StringSplitter(NResource::Find("imdb/queries.list")).Split('\n').SkipEmpty().ToList<TString>();
    for (const auto& name: names) {
        auto& query = result.emplace_back();
        query.QueryName = name;
        // JOB uses comma-separated tables with join predicates in WHERE.
        query.Query = TStringBuilder() << "--!syntax_v1\n"
            << "PRAGMA TablePathPrefix = \"" << EscapeC(Params.GetFullTableName(nullptr)) << "\";\n"
            << "PRAGMA AnsiImplicitCrossJoin;\n"
            << NResource::Find("resfs/file/imdb/queries/" + name + ".sql");
    }
    return result;
}

TVector<IWorkloadQueryGenerator::TWorkloadType> TImdbWorkloadGenerator::GetSupportedWorkloadTypes() const {
    return {TWorkloadType(0, "bench", "Run the Join Order Benchmark", TWorkloadType::EKind::Benchmark)};
}

} // namespace NYdbWorkload
