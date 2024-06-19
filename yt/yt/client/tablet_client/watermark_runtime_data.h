#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

struct TWatermarkRuntimeDataConfig
    : public NYTree::TYsonStructLite
{
    TString ColumnName;
    ui64 Watermark;

    REGISTER_YSON_STRUCT_LITE(TWatermarkRuntimeDataConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TWatermarkRuntimeData
{
    ui64 Watermark;
    int ColumnIndex;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
