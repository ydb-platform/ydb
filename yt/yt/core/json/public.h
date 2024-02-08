#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NJson {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJsonFormatConfig)

struct IJsonConsumer;
struct IJsonWriter;

// YSON with attributes is represented in JSON with additional nested objects. It leads to doubled nesting levels.
constexpr int NestingLevelLimit = NYson::NewNestingLevelLimit * 2;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJson
