#pragma once

#include "backoff_strategy.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TExponentialBackoffOptionsSerializer
    : public NYTree::TExternalizedYsonStruct
{
public:
    REGISTER_EXTERNALIZED_YSON_STRUCT(TExponentialBackoffOptions, TExponentialBackoffOptionsSerializer);

    static void Register(TRegistrar registrar);
};

ASSIGN_EXTERNAL_YSON_SERIALIZER(TExponentialBackoffOptions, TExponentialBackoffOptionsSerializer);

////////////////////////////////////////////////////////////////////////////////

class TConstantBackoffOptionsSerializer
    : public NYTree::TExternalizedYsonStruct
{
public:
    REGISTER_EXTERNALIZED_YSON_STRUCT(TConstantBackoffOptions, TConstantBackoffOptionsSerializer);

    static void Register(TRegistrar registrar);
};

ASSIGN_EXTERNAL_YSON_SERIALIZER(TConstantBackoffOptions, TConstantBackoffOptionsSerializer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
