#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NYaml {

////////////////////////////////////////////////////////////////////////////////

struct TYamlFormatConfig
    : public NYTree::TYsonStruct
{
    //! Write explicit tag "!yt/uint64" for uint64 data type.
    //! Use this option if you want to preserve information about
    //! the original YT type (without it, numbers in range [0, 2^63-1]
    //! will always be written as integers).
    //! Option has no effect for parsing.
    bool WriteUintTag;

    REGISTER_YSON_STRUCT(TYamlFormatConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYamlFormatConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYaml
