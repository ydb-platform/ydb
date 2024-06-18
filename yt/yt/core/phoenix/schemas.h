#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NPhoenix2 {

////////////////////////////////////////////////////////////////////////////////

struct TFieldSchema
    : public NYTree::TYsonStruct
{
    TString Name;
    TFieldTag Tag;
    bool Deprecated;

    REGISTER_YSON_STRUCT(TFieldSchema);
    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFieldSchema);

////////////////////////////////////////////////////////////////////////////////

struct TTypeSchema
    : public NYTree::TYsonStruct
{
    TString Name;
    TTypeTag Tag;
    std::vector<TFieldSchemaPtr> Fields;
    std::vector<TTypeTag> BaseTypeTags;
    bool Template;

    REGISTER_YSON_STRUCT(TTypeSchema);
    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTypeSchema);

////////////////////////////////////////////////////////////////////////////////

struct TUniverseSchema
    : public NYTree::TYsonStruct
{
    std::vector<TTypeSchemaPtr> Types;

    REGISTER_YSON_STRUCT(TUniverseSchema);
    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUniverseSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2
