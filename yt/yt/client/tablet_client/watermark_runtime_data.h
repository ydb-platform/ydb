#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

struct TWatermarkRuntimeData
    : public NYTree::TYsonStructLite
{
    TString ColumnName;
    ui64 Watermark;

    // Should be set manually using table schema after ColumnName is known.
    int ColumnIndex;

    REGISTER_YSON_STRUCT_LITE(TWatermarkRuntimeData);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
