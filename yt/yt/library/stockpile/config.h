#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/stockpile/stockpile.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TStockpileConfig
    : public TStockpileOptions
    , public NYTree::TYsonStruct
{
    TStockpileConfigPtr ApplyDynamic(const TStockpileDynamicConfigPtr& dynamicConfig) const;

    REGISTER_YSON_STRUCT(TStockpileConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStockpileConfig)

////////////////////////////////////////////////////////////////////////////////

struct TStockpileDynamicConfig
    : public NYTree::TYsonStruct
{
    std::optional<i64> BufferSize;
    std::optional<int> ThreadCount;
    std::optional<EStockpileStrategy> Strategy;
    std::optional<TDuration> Period;

    REGISTER_YSON_STRUCT(TStockpileDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStockpileDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
