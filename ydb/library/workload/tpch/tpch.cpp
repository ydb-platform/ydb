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

TString TTpchWorkloadGenerator::GetTablesYaml() const {
    return NResource::Find("tpch_schema.yaml");
}

TWorkloadGeneratorBase::TSpecialDataTypes TTpchWorkloadGenerator::GetSpecialDataTypes() const {
    switch (Params.GetFloatMode()) {
    case TTpcBaseWorkloadParams::EFloatMode::FLOAT:
        return {{"float_type", "Double"}};
    case TTpcBaseWorkloadParams::EFloatMode::DECIMAL:
        return {{"float_type", "Decimal(12,2)"}};
    case TTpcBaseWorkloadParams::EFloatMode::DECIMAL_YDB:
        return {{"float_type", "Decimal(" + ::ToString(NKikimr::NScheme::DECIMAL_PRECISION)
                     + "," + ::ToString(NKikimr::NScheme::DECIMAL_SCALE) + ")"}};
    }
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
