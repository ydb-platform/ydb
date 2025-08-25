#include "tpcds.h"
#include "data_generator.h"

#include <library/cpp/resource/resource.h>
#include <util/stream/file.h>

namespace NYdbWorkload {

TTpcdsWorkloadGenerator::TTpcdsWorkloadGenerator(const TTpcdsWorkloadParams& params)
    : TTpcBaseWorkloadGenerator(params)
    , Params(params)
{}

TString TTpcdsWorkloadGenerator::GetTablesYaml() const {
    return NResource::Find("tpcds_schema.yaml");
}

std::pair<TString, TString> TTpcdsWorkloadGenerator::GetTableAndColumnForDetectFloatMode() const {
    return std::make_pair("call_center", "cc_gmt_offset");
}

TWorkloadGeneratorBase::TSpecialDataTypes TTpcdsWorkloadGenerator::GetSpecialDataTypes() const {
    TString decimalType_5_2, decimalType_7_2, decimalType_15_2;
    switch (Params.GetFloatMode()) {
    case TTpcBaseWorkloadParams::EFloatMode::DOUBLE:
        decimalType_5_2 = decimalType_7_2 = decimalType_15_2 = "Double";
        break;
    case TTpcBaseWorkloadParams::EFloatMode::DECIMAL:
        decimalType_5_2 = "Decimal(5,2)";
        decimalType_7_2 = "Decimal(7,2)";
        decimalType_15_2 = "Decimal(15,2)";
        break;
    }
    return {
        {"decimal_5_2_type", decimalType_5_2},
        {"decimal_7_2_type", decimalType_7_2},
        {"decimal_15_2_type", decimalType_15_2}
    };
}

THolder<IWorkloadQueryGenerator> TTpcdsWorkloadParams::CreateGenerator() const {
    return MakeHolder<TTpcdsWorkloadGenerator>(*this);
}

TString TTpcdsWorkloadParams::GetWorkloadName() const {
    return "TPC-DS";
}

TWorkloadDataInitializer::TList TTpcdsWorkloadParams::CreateDataInitializers() const {
    return {std::make_shared<TTpcdsWorkloadDataInitializerGenerator>(*this)};
}

}
