#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

struct TResourceTrackerConfig
    : public NYTree::TYsonStruct
{
    bool Enable;
    std::optional<double> CpuToVCpuFactor;

    REGISTER_YSON_STRUCT(TResourceTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TResourceTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
