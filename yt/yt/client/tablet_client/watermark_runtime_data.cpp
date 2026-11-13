#include "watermark_runtime_data.h"

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

const std::string CustomRuntimeDataWatermarkKey("watermark");

////////////////////////////////////////////////////////////////////////////////

TStringBuf GetWatermarkComparisonOperatorTextForm(EWatermarkComparisonOperator op)
{
    switch (op) {
        case EWatermarkComparisonOperator::Less:
            return "<";

        case EWatermarkComparisonOperator::LessEqual:
            return "<=";

        case EWatermarkComparisonOperator::Greater:
            return ">";

        case EWatermarkComparisonOperator::GreaterEqual:
            return ">=";
    }
}

void TWatermarkRuntimeDataConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("column_name", &TThis::ColumnName);
    registrar.Parameter("watermark", &TThis::Watermark);
    registrar.Parameter("comparison_operator", &TThis::ComparisonOperator)
        .Default(EWatermarkComparisonOperator::Less);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
