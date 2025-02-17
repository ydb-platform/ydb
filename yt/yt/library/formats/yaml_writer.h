#pragma once

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/client/formats/public.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NYson::IFlushableYsonConsumer> CreateYamlWriter(
    IZeroCopyOutput* output,
    NYson::EYsonType type,
    TYamlFormatConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
