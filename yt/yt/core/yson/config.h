#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/memory/public.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

// TODO(kmokrov): Drop Utf8Check after YTORM-843
class TProtobufInteropConfig
    : public NYTree::TYsonStruct
{
public:
    // Default enum storage type for protobuf to yson conversion.
    EEnumYsonStorageType DefaultEnumYsonStorageType;
    // Check if string field contains actual UTF-8 string.
    EUtf8Check Utf8Check;

    TProtobufInteropConfigPtr ApplyDynamic(const TProtobufInteropDynamicConfigPtr& dynamicConfig) const;

    REGISTER_YSON_STRUCT(TProtobufInteropConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProtobufInteropConfig)

////////////////////////////////////////////////////////////////////////////////

// TODO(kmokrov): Drop Utf8Check after YTORM-843
class TProtobufInteropDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    // Check if string field contains actual UTF-8 string.
    std::optional<EUtf8Check> Utf8Check;

    REGISTER_YSON_STRUCT(TProtobufInteropDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProtobufInteropDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
