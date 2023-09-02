#pragma once

#include "public.h"

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TExecutorConfig
    : public NYTree::TYsonStruct
{
public:
    TSlruCacheConfigPtr CGCache;

    REGISTER_YSON_STRUCT(TExecutorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TExecutorConfig)

////////////////////////////////////////////////////////////////////////////////

class TColumnEvaluatorCacheConfig
    : public NYTree::TYsonStruct
{
public:
    TSlruCacheConfigPtr CGCache;

    REGISTER_YSON_STRUCT(TColumnEvaluatorCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TColumnEvaluatorCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TColumnEvaluatorCacheDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    TSlruCacheDynamicConfigPtr CGCache;

    REGISTER_YSON_STRUCT(TColumnEvaluatorCacheDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TColumnEvaluatorCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
