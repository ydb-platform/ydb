#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NComplexTypes {

////////////////////////////////////////////////////////////////////////////////

template <typename... TArgs>
using TComplexTypeYsonScanner = std::function<void(NYson::TYsonPullParserCursor*, TArgs...)>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes