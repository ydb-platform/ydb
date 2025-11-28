#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NJson {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJsonFormat,
    (Text)
    (Pretty)
);

DEFINE_ENUM(EJsonAttributesMode,
    (Always)
    (Never)
    (OnDemand)
);

struct TJsonFormatConfig
    : public NYTree::TYsonStruct
{
    EJsonFormat Format;
    EJsonAttributesMode AttributesMode;
    bool Plain;
    bool EncodeUtf8;

    i64 MemoryLimit;
    int NestingLevelLimit;
    std::optional<int> StringLengthLimit;

    bool Stringify;
    bool AnnotateWithTypes;

    bool SupportInfinity;
    bool StringifyNanAndInfinity;

    // Size of buffer used read out input stream in parser.
    // NB: In case of parsing long string yajl holds in memory whole string prefix and copy it on every parse call.
    // Therefore parsing long strings works faster with larger buffer.
    int BufferSize;

    //! Only works for tabular data.
    bool SkipNullValues;

    REGISTER_YSON_STRUCT(TJsonFormatConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJsonFormatConfig)

////////////////////////////////////////////////////////////////////////////////


struct TWebJsonFormatConfig
    : public NYTree::TYsonStruct
{
    EJsonFormat Format;

    REGISTER_YSON_STRUCT(TWebJsonFormatConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TWebJsonFormatConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJson
