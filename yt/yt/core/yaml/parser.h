#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

#include <util/stream/fwd.h>

namespace NYT::NYaml {

////////////////////////////////////////////////////////////////////////////////

//! Parses a YAML stream in pull mode (may be used for structured driver commands).
void ParseYaml(
    IInputStream* input,
    NYson::IYsonConsumer* consumer,
    TYamlFormatConfigPtr config,
    NYson::EYsonType ysonType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYaml
