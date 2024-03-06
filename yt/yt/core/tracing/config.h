#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

class TTracingTransportConfig
    : public NYTree::TYsonStruct
{
public:
    bool SendBaggage;

    REGISTER_YSON_STRUCT(TTracingTransportConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTracingTransportConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
