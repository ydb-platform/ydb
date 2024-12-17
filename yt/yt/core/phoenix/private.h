#pragma once

#include "public.h"

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/yt/misc/global.h>

namespace NYT::NPhoenix2 {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, PhoenixLogger, "Phoenix");

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TObjectId, ui32);
constexpr auto InlineObjectIdMask = TObjectId(0x80000000);
constexpr auto NullObjectId = TObjectId(0x00000000);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2

