#include "watermark_runtime_data.h"

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

void TWatermarkRuntimeDataConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("column_name", &TThis::ColumnName);
    registrar.Parameter("watermark", &TThis::Watermark);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
