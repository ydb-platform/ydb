#include "parser.h"

#include <yt/yt/core/yson/consumer.h>

#include <array>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

static const size_t ParseBufferSize = 1 << 16;

void Parse(IInputStream* input, IParser* parser)
{
    std::array<char, ParseBufferSize> buffer;
    while (true) {
        size_t bytesRead = input->Read(buffer.data(), ParseBufferSize);
        if (bytesRead == 0) {
            break;
        }
        parser->Read(TStringBuf(buffer.data(), bytesRead));
    }
    parser->Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
