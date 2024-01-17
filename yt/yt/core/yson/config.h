#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/memory/public.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

// TODO(kmokrov): Drop after YTORM-843
class TProtobufInteropDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    // Check if string field contain actual UTF-8 string.
    EUtf8Check Utf8Check;

    REGISTER_YSON_STRUCT(TProtobufInteropDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProtobufInteropDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // NYT::NYson
