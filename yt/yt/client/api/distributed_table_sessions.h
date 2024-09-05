#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TDistributedWriteSessionId, TGuid);

////////////////////////////////////////////////////////////////////////////////

class TDistributedWriteCookie
    : public NYTree::TYsonStruct
{
public:
    REGISTER_YSON_STRUCT(TDistributedWriteCookie);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDistributedWriteCookie);

////////////////////////////////////////////////////////////////////////////////

class TDistributedWriteSession
    : public NYTree::TYsonStruct
{
public:
    REGISTER_YSON_STRUCT(TDistributedWriteSession);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDistributedWriteSession);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
