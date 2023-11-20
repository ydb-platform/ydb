#pragma once

#include "name_service_client_protocol.h"
#include "connect_socket_protocol.h"
#include "send_data_protocol.h"
#include "read_http_reply_protocol.h"

namespace NActors {

template <typename TOrigActor>
class THTTPRequestProtocol
        : public TResolveClientProtocol
        , public TConnectSocketProtocol
        , public TSendDataProtocol
        , public TReadHTTPReplyProtocol
        , public TReadDataProtocol<THTTPRequestProtocol<TOrigActor>>
{
    friend class TResolveClientProtocol;
    friend class TConnectSocketProtocol;
    friend class TSendDataProtocol;
    friend class TReadHTTPReplyProtocol;
    friend class TReadDataProtocol<THTTPRequestProtocol<TOrigActor>>;
public:

    void HTTPRequest(
            TOrigActor* orig,
            const TActorContext& ctx,
            TString host,
            TString url,
            TString headers = TString(),
            ui16 port = 80) noexcept
    {
        OriginalActor = orig;

        orig->template AddMsgProtocol<TEvHTTPProtocolRetry>(
            &TOrigActor::template CallProtocolStateFunc<
                TOrigActor,
                THTTPRequestProtocol<TOrigActor>,
                &THTTPRequestProtocol<TOrigActor>::ProtocolFunc>);

        Host = std::move(host);
        Url = std::move(url);
        Port = port;
        HttpRequestMessage = Sprintf(
            "%s HTTP/1.1\r\n"
            "Host: %s\r\n"
            "%s"
            "Connection: close\r\n"
            "\r\n",
            Url.data(), Host.data(), headers.data());

        MemLogPrintF("HTTPRequest %s", HttpRequestMessage.data());

        WriteTask = EWriteTask::COMPLETE;
        NumberOfTriesLeft = 4;
        RetryCall = [=](const TActorContext& ctx) {
            SendResolveMessage<TOrigActor>(orig, ctx, Host, port);
        };
        SendResolveMessage<TOrigActor>(orig, ctx, Host, port);
    }

    void HTTPReadContent(
            TOrigActor* orig,
            const TActorContext& ctx,
            ui64 amount) noexcept
    {
        OriginalActor = orig;
        ReadChunks = false;
        ReadBuffer.resize(amount);
        Filled = 0;
        TReadDataProtocol<THTTPRequestProtocol<TOrigActor>>::ReadData(
            this, OriginalActor, ctx,
            Socket.Get(), ReadBuffer.data(), amount);
    }

    void HTTPWriteContent(
            TOrigActor* orig,
            const TActorContext& ctx,
            const char* data,
            size_t len,
            bool complete) noexcept
    {
        OriginalActor = orig;
        WriteTask = complete ? EWriteTask::COMPLETE : EWriteTask::CONTINUE;
        SendData<TOrigActor>(OriginalActor, ctx, Socket.Get(), data, len);
    }

    void HTTPExpectReply(
            TOrigActor* orig,
            const TActorContext& ctx,
            TVector<char> buf = TVector<char>()) noexcept
    {
        ReadHTTPReply<TOrigActor>(orig, ctx, Socket, buf);
    }

    virtual void CatchHTTPRequestError(TString error) noexcept = 0;

    virtual void CatchHTTPContent(
        const TActorContext& ctx, TVector<char> buf) noexcept = 0;

    virtual void CatchHTTPWriteComplete(const TActorContext&) noexcept {}

    virtual void CatchHTTPConnectionClosed() noexcept = 0;

public:
    void CatchHostAddress(
            const TActorContext& ctx,
            NAddr::IRemoteAddrPtr address) noexcept override
    {
        Y_ABORT_UNLESS(address.Get() != nullptr);

        NAddr::IRemoteAddrRef addr(address.Release());

        Y_ABORT_UNLESS(addr.Get() != nullptr);

        MemLogPrintF("%s"
                     ", actorId #%s"
                     ", address %s",
                     __func__,
                     ctx.SelfID.ToString().data(),
                     PrintHostAndPort(*addr).data());

        NumberOfTriesLeft = 4;
        RetryCall = [=](const TActorContext& ctx) {
            ConnectSocket<TOrigActor>(OriginalActor, ctx, addr);
        };

        Y_ABORT_UNLESS(addr.Get() != nullptr);

        ConnectSocket<TOrigActor>(OriginalActor, ctx, addr);
    }


    void CatchResolveError(
            const TActorContext& ctx, TString error) noexcept override
    {
        if (NumberOfTriesLeft && --NumberOfTriesLeft > 0) {
            MemLogPrintF("%s"
                         ", schedule retry"
                         ", actorId #%s"
                         ", error %s",
                         __func__,
                         ctx.SelfID.ToString().data(),
                         error.data());

            ctx.Schedule(TDuration::Seconds(1), new TEvHTTPProtocolRetry);
            return;
        } else {
            RetryCall = std::function<void(const TActorContext&)>();
        }

        MemLogPrintF("%s"
                     ", actorId #%s"
                     ", error %s",
                     __func__,
                     ctx.SelfID.ToString().data(),
                     error.data());

        CatchHTTPRequestError(std::move(error));
    }


    void CatchConnectError(
            const TActorContext& ctx, TString error) noexcept override
    {
        if (--NumberOfTriesLeft > 0) {
            ctx.Schedule(TDuration::Seconds(1), new TEvHTTPProtocolRetry);
            return;
        } else {
            RetryCall = std::function<void(const TActorContext& ctx)>();
        }

        CatchHTTPRequestError(std::move(error));
    }


    void CatchConnectedSocket(
            const TActorContext& ctx,
            TIntrusivePtr<NInterconnect::TStreamSocket> socket
                 ) noexcept override
    {
        Socket = std::move(socket);
        SendData<TOrigActor>(OriginalActor, ctx, Socket.Get(),
                             HttpRequestMessage.data(), HttpRequestMessage.size());
        ReadHTTPReply<TOrigActor>(OriginalActor, ctx, Socket);
    }

    void CatchSendDataError(TString error) noexcept override
    {
        CatchHTTPRequestError(std::move(error));
    }

    void CatchReadDataError(TString error) noexcept override
    {
        CatchHTTPRequestError(std::move(error));
    }

    void CatchSendDataComplete(const TActorContext& ctx) noexcept override {
        switch (WriteTask) {
        case EWriteTask::COMPLETE:
            MemLogPrintF("CatchSendDataComplete, EWriteTask::COMPLETE");
            break;

        case EWriteTask::CONTINUE:
            MemLogPrintF("CatchSendDataComplete, EWriteTask::COMPLETE");

            CatchHTTPWriteComplete(ctx);
            break;
        }
    }

    bool CatchReadDataComplete(
            const TActorContext& ctx, size_t amount) noexcept
    {
        if (ReadChunks == false) {
            Filled += amount;
            if (Filled == ReadBuffer.size()) {
                CatchHTTPContent(ctx, std::move(ReadBuffer));
                return true;
            } else {
                return false;
            }
        }
        return true;
    }

    void CatchReadDataClosed() noexcept {
        CatchHTTPConnectionClosed();
    }

private:
    void ProtocolFunc(
            TAutoPtr<NActors::IEventHandle>& ev) noexcept
    {
        auto ctx(NActors::TActivationContext::AsActorContext());
        switch (ev->GetTypeRewrite()) {
            CFuncCtx(TEvHTTPProtocolRetry::EventType, Retry, ctx);
        default:
            Y_ABORT("Unknown message type dispatched");
        }
    }

    void Retry(const TActorContext& ctx) noexcept {
        Y_ABORT_UNLESS(RetryCall);

        RetryCall(ctx);
    }

    TOrigActor* OriginalActor;
    IActor::TReceiveFunc DefaultStateFunc;
    TString Host;
    TString Url;
    ui16 Port;
    TString HttpRequestMessage;

    TVector<char> ReadBuffer;
    size_t Filled;
    bool ReadChunks;

    TIntrusivePtr<NInterconnect::TStreamSocket> Socket;

    enum class EWriteTask {
        COMPLETE,
        CONTINUE,
    };

    EWriteTask WriteTask;

    std::function<void(const TActorContext&)> RetryCall;
    ui32 NumberOfTriesLeft;
};

}
