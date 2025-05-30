#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>
#include <yt/yt/core/yson/public.h>

#include <functional>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

struct TResolveProtobufElementByYPathOptions
{
    bool AllowUnknownYsonFields = false;
    bool AllowAsterisks = false;
};

struct TProtobufWriterOptions
{
    //! Keep: all unknown fields found during YSON parsing
    //! are translated into Protobuf unknown fields (each has number UnknownYsonFieldNumber
    //! and is a key-value pair with field name being its key and YSON being the value).
    //!
    //! Skip: all unknown fields are silently skipped.
    //!
    //! Fail: an exception is thrown whenever an unknown field is found.
    //!
    //! Forward: current key/index is kept, the children are considered by resolver recursively.
    //! Forward in a scalar leaf is interpreted as a Fail.
    using TUnknownYsonFieldModeResolver = std::function<EUnknownYsonFieldsMode(const NYPath::TYPath&)>;

    static TUnknownYsonFieldModeResolver CreateConstantUnknownYsonFieldModeResolver(EUnknownYsonFieldsMode mode);

    TProtobufWriterOptions CreateChildOptions(const NYPath::TYPath& path) const;

    TUnknownYsonFieldModeResolver UnknownYsonFieldModeResolver = CreateConstantUnknownYsonFieldModeResolver(EUnknownYsonFieldsMode::Fail);
    //! If |true| then required fields not found in protobuf metadata are
    //! silently skipped; otherwise an exception is thrown.
    bool SkipRequiredFields = false;

    //! Convert yson keys from snake case to camel case.
    bool ConvertSnakeToCamelCase = false;

    //! Check if string field contains actual UTF-8 string. Overrides option from config if provided.
    std::optional<EUtf8Check> Utf8Check;
};

struct TProtobufParserOptions
{
    //! If |true| then fields with numbers not found in protobuf metadata are
    //! silently skipped; otherwise an exception is thrown.
    bool SkipUnknownFields = false;

    //! If |true| then required fields not found in protobuf metadata are
    //! silently skipped; otherwise an exception is thrown.
    bool SkipRequiredFields = false;

    //! Check if string field contains actual UTF-8 string. Overrides option from config if provided.
    std::optional<EUtf8Check> Utf8Check;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
