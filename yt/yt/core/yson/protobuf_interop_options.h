#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>
#include <yt/yt/core/yson/public.h>

#include <functional>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

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

    TUnknownYsonFieldModeResolver UnknownYsonFieldModeResolver = CreateConstantUnknownYsonFieldModeResolver(EUnknownYsonFieldsMode::Fail);
    //! If |true| then required fields not found in protobuf metadata are
    //! silently skipped; otherwise an exception is thrown.
    bool SkipRequiredFields = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
