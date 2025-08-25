#include "clickbench.h"
#include "data_generator.h"
#include <ydb/library/workload/benchmark_base/workload.h_serialized.h>

#include <library/cpp/resource/resource.h>
#include <library/cpp/string_utils/csv/csv.h>
#include <util/stream/file.h>
#include <util/random/random.h>

namespace NYdbWorkload {
TClickbenchWorkloadParams::TClickbenchWorkloadParams() {
    SetPath("clickbench/hits");
}

TClickbenchWorkloadGenerator::TClickbenchWorkloadGenerator(const TClickbenchWorkloadParams& params)
    : TWorkloadGeneratorBase(params)
    , Params(params)
{}

TString TClickbenchWorkloadGenerator::GetTablesYaml() const {
    return NResource::Find("click_bench_schema.yaml");
}

TWorkloadGeneratorBase::TSpecialDataTypes TClickbenchWorkloadGenerator::GetSpecialDataTypes() const {
    return {};
}

TQueryInfoList TClickbenchWorkloadGenerator::GetInitialData() {
    return {};
}

TQueryInfoList TClickbenchWorkloadGenerator::GetWorkload(int type) {
    TQueryInfoList result;
    if (type) {
        return result;
    }

    TVector<TString> queries;
    TString resourceName = "click_bench_queries.sql";
    if (Params.GetSyntax() == TWorkloadBaseParams::EQuerySyntax::PG) {
        resourceName = "click_bench_queries_pg.sql";
    } else if (Params.GetCheckCanonical()) {
        resourceName = "queries-deterministic.sql";
    }
    queries = StringSplitter(NResource::Find(resourceName)).Split(';').ToList<TString>();
    const auto tablePath = Params.GetTablePathQuote(Params.GetSyntax()) + Params.GetPath() + Params.GetTablePathQuote(Params.GetSyntax());
    for (ui32 i = 0; i < queries.size(); ++i) {
        auto& query = queries[i];
        if (Params.GetSyntax() == TWorkloadBaseParams::EQuerySyntax::PG) {
            query = "--!syntax_pg\n" + query;
        }
        SubstGlobal(query, "{table}", tablePath);
        SubstGlobal(query, "$data", tablePath);
        result.emplace_back();
        result.back().Query = query;
        const auto resultKey = "click_bench_canonical/q" + ToString(i) + ".result";
        if (Params.GetCheckCanonical() && NResource::Has(resultKey)) {
            result.back().ExpectedResult = NResource::Find(resultKey);
        }
    }
    return result;
}

TVector<IWorkloadQueryGenerator::TWorkloadType> TClickbenchWorkloadGenerator::GetSupportedWorkloadTypes() const {
    return {TWorkloadType(0, "bench", "Perform benchmark", TWorkloadType::EKind::Benchmark)};
}

void TClickbenchWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
    TWorkloadBaseParams::ConfigureOpts(opts, commandType, workloadType);
    switch (commandType) {
    case TWorkloadParams::ECommandType::Run:
        opts.AddLongOption( "syntax", "Query syntax [" + GetEnumAllNames<EQuerySyntax>() + "].")
            .StoreResult(&Syntax).DefaultValue(Syntax);
        break;
    default:
        break;
    }
}


THolder<IWorkloadQueryGenerator> TClickbenchWorkloadParams::CreateGenerator() const {
    return MakeHolder<TClickbenchWorkloadGenerator>(*this);
}

TWorkloadDataInitializer::TList TClickbenchWorkloadParams::CreateDataInitializers() const {
    return {std::make_shared<TClickbenchWorkloadDataInitializerGenerator>(*this)};
}

TString TClickbenchWorkloadParams::GetWorkloadName() const {
    return "ClickBench";
}

}
