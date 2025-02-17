#include "compression_detail.h"

#include "compression.h"

namespace NYT::NHttp::NDetail {

////////////////////////////////////////////////////////////////////////////////

const std::vector<TContentEncoding>& GetInternallySupportedContentEncodings()
{
    static const std::vector<TContentEncoding> result = {
        "gzip",
        IdentityContentEncoding,
        "br",
        "deflate",
    };
    return result;
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IOutputStream> TryDetectOptionalCompressors(
    const TContentEncoding& /*contentEncoding*/,
    IOutputStream* /*inner*/)
{
    return nullptr;
}

std::unique_ptr<IInputStream> TryDetectOptionalDecompressors(
    const TContentEncoding& /*contentEncoding*/,
    IInputStream* /*inner*/)
{
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp::NDetail
