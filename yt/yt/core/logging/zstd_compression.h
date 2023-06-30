#pragma once

#include "public.h"

#include <util/generic/size_literals.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

constexpr i64 MaxZstdFrameUncompressedLength = 5_MBs;
constexpr const int DefaultZstdCompressionLevel = 3;

ILogCompressionCodecPtr CreateZstdCompressionCodec(int compressionLevel = DefaultZstdCompressionLevel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
