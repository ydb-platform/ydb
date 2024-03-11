#include "compression.h"

namespace NYT::NHttp {

////////////////////////////////////////////////////////////////////////////////

const std::vector<TContentEncoding> SupportedCompressions = {
    "gzip",
    IdentityContentEncoding,
    "br",
    "deflate",
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IOutputStream> TryDetectOptionalCompressors(
    TContentEncoding /*contentEncoding*/,
    IOutputStream* /*inner*/)
{
    return nullptr;
}

std::unique_ptr<IInputStream> TryDetectOptionalDecompressors(
    TContentEncoding /*contentEncoding*/,
    IInputStream* /*inner*/)
{
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
