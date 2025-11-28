#pragma once

#include "public.h"

#include <yt/yt/core/misc/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

struct TBaggageManagerConfig
    : public NYTree::TYsonStruct
{
    // Determines whether new keys can be added to the baggage.
    // The keys that were already in the baggage remain in it.
    //
    // Setting this value to true may increase the network consumption
    // if the trace context is transmitted.
    bool EnableBaggageAddition;

    TBaggageManagerConfigPtr ApplyDynamic(const TBaggageManagerDynamicConfigPtr& dynamicConfig) const;
    void ApplyDynamicInplace(const TBaggageManagerDynamicConfigPtr& dynamicConfig);

    REGISTER_YSON_STRUCT(TBaggageManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBaggageManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TBaggageManagerDynamicConfig
    : public NYTree::TYsonStruct
{
    std::optional<bool> EnableBaggageAddition;

    REGISTER_YSON_STRUCT(TBaggageManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBaggageManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
