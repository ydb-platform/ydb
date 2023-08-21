#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TTDigestConfig
    : public NYTree::TYsonStruct
{
public:
    double Delta;
    double CompressionFrequency;

    REGISTER_YSON_STRUCT(TTDigestConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTDigestConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
