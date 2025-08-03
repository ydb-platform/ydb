#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EWatermarkComparisonOperator,
    (Less)
    (LessEqual)
    (Greater)
    (GreaterEqual)
);

////////////////////////////////////////////////////////////////////////////////

struct TWatermarkRuntimeDataConfig
    : public NYTree::TYsonStructLite
{
    std::string ColumnName;
    ui64 Watermark;
    EWatermarkComparisonOperator ComparisonOperator;

    REGISTER_YSON_STRUCT_LITE(TWatermarkRuntimeDataConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TWatermarkRuntimeData
{
    ui64 Watermark;
    int ColumnIndex;
    EWatermarkComparisonOperator ComparisonOpeator;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
