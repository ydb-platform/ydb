#include "factory.h"
#include "lz4io.h"
#include "brotli.h"
#include "zstd.h"
#include "bzip2.h"

namespace NYql {

std::unique_ptr<NDB::ReadBuffer> MakeDecompressor(NDB::ReadBuffer& input, const std::string_view& compression) {
    if ("lz4" == compression)
        return std::make_unique<NLz4::TReadBuffer>(input);
    if ("brotli" == compression)
        return std::make_unique<NBrotli::TReadBuffer>(input);
    if ("zstd" == compression)
        return std::make_unique<NZstd::TReadBuffer>(input);
    if ("bzip2" == compression)
        return std::make_unique<NBzip2::TReadBuffer>(input);

    return nullptr;
}

}
