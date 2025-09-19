#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NJson {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TJsonFormatConfig)
DECLARE_REFCOUNTED_STRUCT(TWebJsonFormatConfig)

struct IJsonConsumer;
struct IJsonWriter;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJson
