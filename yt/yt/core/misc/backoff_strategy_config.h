#pragma once

#include "backoff_strategy.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSerializableExponentialBackoffOptions
    : public virtual NYTree::TYsonStruct
    , public TExponentialBackoffOptions
{
public:
    REGISTER_YSON_STRUCT(TSerializableExponentialBackoffOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSerializableExponentialBackoffOptions)

////////////////////////////////////////////////////////////////////////////////

class TSerializableConstantBackoffOptions
    : public virtual NYTree::TYsonStruct
    , public TConstantBackoffOptions
{
public:
    REGISTER_YSON_STRUCT(TSerializableConstantBackoffOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSerializableConstantBackoffOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
