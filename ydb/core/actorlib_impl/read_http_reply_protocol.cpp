#include "read_http_reply_protocol.h"

#include <cstring>

namespace NActors {

bool TReadHTTPReplyProtocol::CatchReadDataComplete(
        const TActorContext& ctx, size_t amount) noexcept
{
    Filled += amount;
    Y_ABORT_UNLESS(Buf.size() >= Filled);

    if (Filled < MINIMUM_HTTP_REPLY_SIZE) {
        if (Buf.size() - Filled < FREE_SPACE_LOW_WATER_MARK)
        {
            Buf.resize(Filled + FREE_SPACE_HIGH_WATER_MARK);
        }

        ResetReadBuf(&Buf[Filled], Buf.size() - Filled);
        return false;
    }

    if (Filled == Buf.size()) {
        Buf.push_back(0);
    } else {
        Buf[Filled] = 0;
    }

    /* null char ensured, it's safe to use strstr */
    auto result = strstr(Buf.data(), "\r\n\r\n");

    if (result == nullptr) {
        /* Check if there no null char in data read from a socket */
        if (strlen(Buf.data()) != Filled) {
            /* got garbage from the socket */
            CatchReadDataError("null char in HTTP reply");
            return true;
        }

        /* Check maximum size limit */
        if (Filled >= HTTPReplySizeLimit) {
            CatchReadDataError("HTTP reply is too large");
            return true;
        }

        if (Buf.size() - Filled < FREE_SPACE_LOW_WATER_MARK)
        {
            Buf.resize(Filled + FREE_SPACE_HIGH_WATER_MARK);
        }

        ResetReadBuf(&Buf[Filled], Buf.size() - Filled);
        return false;
    }

    const size_t messageSize = result - Buf.data() + 4;
    Buf.resize(Filled + 1);
    CatchHTTPReply(ctx, std::move(Buf), messageSize);
    return true;
}


void TReadHTTPReplyProtocol::CatchReadDataClosed() noexcept
{
    CatchReadDataError("Socket closed prematurely");
}

}
