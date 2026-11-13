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

TStringBuf GetWatermarkComparisonOperatorTextForm(EWatermarkComparisonOperator op);

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf BackgroundDeletePredicateKey = "background_delete_predicate";

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

} // namespace NYT::NTabletClient
