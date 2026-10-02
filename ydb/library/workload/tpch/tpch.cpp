#include "tpch.h"
#include "data_generator.h"

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

std::pair<TString, TString> TTpchWorkloadGenerator::GetTableAndColumnForDetectFloatMode() const {
    return std::make_pair("lineitem", "l_tax");
}

TWorkloadGeneratorBase::TSpecialDataTypes TTpchWorkloadGenerator::GetSpecialDataTypes() const {
    switch (Params.GetFloatMode()) {
    case TTpcBaseWorkloadParams::EFloatMode::DOUBLE:
        return {{"float_type", "Double"}};
    case TTpcBaseWorkloadParams::EFloatMode::DECIMAL:
        return {{"float_type", "Decimal(12,2)"}};
    }
}

ui32 TTpchWorkloadGenerator::GetDefaultPartitionsCount(const TString& /*tableName*/) const {
    return Params.GetScale() <= 10 ? 64 : 256;
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
