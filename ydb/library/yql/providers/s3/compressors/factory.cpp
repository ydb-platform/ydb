#include "factory.h"
#include "lz4io.h"

namespace NYql {

std::unique_ptr<NDB::ReadBuffer> MakeDecompressor(NDB::ReadBuffer& input, const std::string_view& compression) {
    if ("lz4" == compression)
            return std::make_unique<NLz4::TReadBuffer>(input);

    return nullptr;
}

}

