#include "tpch.h"
#include "data_generator.h"

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <library/cpp/resource/resource.h>
#include <util/stream/file.h>

namespace NYdbWorkload {

TTpchWorkloadGenerator::TTpchWorkloadGenerator(const TTpchWorkloadParams& params)
    : TTpcBaseWorkloadGenerator(params)
    , Params(params)
{}

TString TTpchWorkloadGenerator::DoGetDDLQueries() const {
    auto schema = NResource::Find("tpch_schema.sql");
    TString floatType;
    switch (Params.GetFloatMode()) {
    case TTpcBaseWorkloadParams::EFloatMode::FLOAT:
        floatType = "Double";
        break;
    case TTpcBaseWorkloadParams::EFloatMode::DECIMAL:
        floatType = "Decimal(12,2)";
        break;
    case TTpcBaseWorkloadParams::EFloatMode::DECIMAL_YDB:
        floatType = "Decimal(" + ::ToString(NKikimr::NScheme::DECIMAL_PRECISION)
                     + "," + ::ToString(NKikimr::NScheme::DECIMAL_SCALE) + ")";
        break;
    }
    SubstGlobal(schema, "{float_type}", floatType);
    return schema;
}

void TTpchWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
    TTpcBaseWorkloadParams::ConfigureOpts(opts, commandType, workloadType);
    switch (commandType) {
    case TWorkloadParams::ECommandType::Run:
        opts.AddLongOption('c', "check-canonical", "Use deterministic queries and check results with canonical ones.")
            .NoArgument().StoreTrue(&CheckCanonical);
        break;
    default:
        break;
    }
}

TVector<TString> TTpchWorkloadGenerator::GetTablesList() const {
    return {
        "customer",
        "lineitem",
        "nation",
        "orders",
        "part",
        "partsupp",
        "region",
        "supplier"
    };
}

THolder<IWorkloadQueryGenerator> TTpchWorkloadParams::CreateGenerator() const {
    return MakeHolder<TTpchWorkloadGenerator>(*this);
}

TString TTpchWorkloadParams::GetWorkloadName() const {
    return "TPC-H";
}

TWorkloadDataInitializer::TList TTpchWorkloadParams::CreateDataInitializers() const {
    return {std::make_shared<TTpchWorkloadDataInitializerGenerator>(*this)};
}

}
