#pragma once

#include "public.h"

#include <util/generic/size_literals.h>

namespace NYT::NHttp::NDetail {

////////////////////////////////////////////////////////////////////////////////

// NB: some codecs (e.g. lzop) accept ui16 as buffer size.
constexpr size_t DefaultCompressionBufferSize = 32_KB;

std::unique_ptr<IOutputStream> TryDetectOptionalCompressors(
    const TContentEncoding& contentEncoding,
    IOutputStream* inner);
std::unique_ptr<IInputStream> TryDetectOptionalDecompressors(
    const TContentEncoding& contentEncoding,
    IInputStream* inner);

const std::vector<TContentEncoding>& GetInternallySupportedContentEncodings();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp::NDetail
