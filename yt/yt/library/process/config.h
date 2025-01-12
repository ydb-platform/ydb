#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NPipes {

////////////////////////////////////////////////////////////////////////////////

class TIODispatcherConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration ThreadPoolPollingPeriod;

    TIODispatcherConfigPtr ApplyDynamic(const TIODispatcherDynamicConfigPtr& dynamicConfig) const;

    REGISTER_YSON_STRUCT(TIODispatcherConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TIODispatcherConfig)

////////////////////////////////////////////////////////////////////////////////

class TIODispatcherDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<TDuration> ThreadPoolPollingPeriod;

    REGISTER_YSON_STRUCT(TIODispatcherDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TIODispatcherDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipes
