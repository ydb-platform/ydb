#include "watermark_runtime_data.h"

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

const std::string CustomRuntimeDataWatermarkKey("watermark");

////////////////////////////////////////////////////////////////////////////////

void TWatermarkRuntimeDataConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("column_name", &TThis::ColumnName);
    registrar.Parameter("watermark", &TThis::Watermark);
    registrar.Parameter("comparison_operator", &TThis::ComparisonOperator)
        .Default(EWatermarkComparisonOperator::Less);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
