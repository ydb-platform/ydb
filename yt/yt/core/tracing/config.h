#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

class TTracingConfig
    : public NYTree::TYsonStruct
{
public:
    bool SendBaggage;

    REGISTER_YSON_STRUCT(TTracingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTracingConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
