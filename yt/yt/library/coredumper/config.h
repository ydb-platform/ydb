#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NCoreDump {

////////////////////////////////////////////////////////////////////////////////

struct TCoreDumperConfig
    : public NYTree::TYsonStruct
{
    //! A path to store the core files.
    std::string Path;

    //! A name under which the core file should be placed.
    //! Some of the Porto variables like %CORE_PID, %CORE_TID etc are supported, refer to the implementation.
    std::string Pattern;

    REGISTER_YSON_STRUCT(TCoreDumperConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCoreDumperConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCoreDump
