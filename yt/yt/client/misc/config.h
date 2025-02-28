#pragma once

#include "workload.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TWorkloadConfig
    : public virtual NYTree::TYsonStruct
{
    TWorkloadDescriptor WorkloadDescriptor;

    REGISTER_YSON_STRUCT(TWorkloadConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TWorkloadConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
