#pragma once

#include "read_data_protocol.h"

namespace NActors {

class TReadHTTPReplyProtocol
    : public TReadDataProtocol<TReadHTTPReplyProtocol>
{
public:
    template <typename TOrigActor>
    void ReadHTTPReply(
            TOrigActor* orig,
            const TActorContext& ctx,
            TIntrusivePtr<NInterconnect::TStreamSocket> socket,
            TVector<char> readyBuf = TVector<char>()
                ) noexcept
    {
        Socket = socket;
        if (readyBuf.size() == 0) {
            Filled = 0;
            Buf.resize(FREE_SPACE_HIGH_WATER_MARK);
        } else {
            Buf = std::move(readyBuf);
            Filled = Buf.size();
            Buf.resize(FREE_SPACE_HIGH_WATER_MARK + Buf.size());
            if (CatchReadDataComplete(ctx, 0)) {
                return;
            }
        }
        ReadData<TOrigActor>(this, orig, ctx, Socket.Get(),
                             Buf.data(), Buf.size() - Filled);
    }

    virtual void CatchReadDataError(TString error) noexcept = 0;

    virtual void CatchHTTPReply(
        const TActorContext& ctx,
        TVector<char> buf,
        size_t httpMessageSize) noexcept = 0;

    size_t HTTPReplySizeLimit = DEFAULT_MAXIMUM_HTTP_REPLY_SIZE;

    virtual ~TReadHTTPReplyProtocol() = default;

private:
    friend class TReadDataProtocol<TReadHTTPReplyProtocol>;

    bool CatchReadDataComplete(
        const TActorContext& ctx, size_t amount) noexcept;

    void CatchReadDataClosed() noexcept;


    TIntrusivePtr<NInterconnect::TStreamSocket> Socket;
    TVector<char> Buf;
    size_t Filled = 0;

    static constexpr size_t FREE_SPACE_LOW_WATER_MARK = 1024;
    static constexpr size_t FREE_SPACE_HIGH_WATER_MARK = 2048;

    /* HTTP/X.X XXX X\r\n\r\n */
    static constexpr size_t MINIMUM_HTTP_REPLY_SIZE = 18;
    static constexpr size_t DEFAULT_MAXIMUM_HTTP_REPLY_SIZE = 65536;
};

}
