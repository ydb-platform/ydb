#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

#include <util/stream/fwd.h>

namespace NYT::NYaml {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NYson::IFlushableYsonConsumer> CreateYamlWriter(
    IZeroCopyOutput* output,
    NYson::EYsonType type,
    TYamlFormatConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYaml
