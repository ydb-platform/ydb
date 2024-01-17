#pragma once

#include <ydb/library/actors/interconnect/events_local.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect_stream.h>

namespace NActors {

struct IReadDataCatch {
    virtual void CatchReadDataError(TString error) noexcept = 0;
    /* returns false if the protocol should continue to read data */
    virtual bool CatchReadDataComplete(
        const TActorContext& ctx, size_t amount) noexcept = 0;

    virtual void CatchReadDataClosed() noexcept = 0;
};

class TReadDataProtocolImpl {
public:
    template <typename TOrigActor>
    void ReadData(
            IReadDataCatch* catcher,
            TOrigActor* orig,
            const TActorContext& ctx,
            NInterconnect::TStreamSocket* socket,
            char* data,
            size_t len) noexcept
    {
        Catcher = catcher;
        Socket = socket;
        Data = data;
        Len = len;
        Filled = 0;
        Cancelled = false;

        orig->template AddMsgProtocol<TEvSocketReadyRead>(
            &TOrigActor::template CallProtocolStateFunc<
                TOrigActor,
                TReadDataProtocolImpl,
                &TReadDataProtocolImpl::ProtocolFunc>);

        TryAgain(ctx);
    }

    void ResetReadBuf(char* data, size_t len) {
        Data = data;
        Len = len;
        Filled = 0;
    }

    void CancelReadData(const TActorContext& ctx) noexcept;

private:
    void ProtocolFunc(
        TAutoPtr<NActors::IEventHandle>& ev) noexcept;

    void TryAgain(const TActorContext& ctx) noexcept;

    IReadDataCatch* Catcher;
    NInterconnect::TStreamSocket* Socket;
    char* Data;
    size_t Len;
    size_t Filled;
    bool Cancelled;
};

template <typename TDerived = void>
class TReadDataProtocol: virtual public TReadDataProtocolImpl {
public:
    template <typename TOrigActor>
    void ReadData(
            TOrigActor* orig,
            const TActorContext& ctx,
            NInterconnect::TStreamSocket* socket,
            char* data,
            size_t len) noexcept
    {
        CatchRelay.Catcher = static_cast<TDerived*>(orig);
        TReadDataProtocolImpl::ReadData(
            &CatchRelay, orig, ctx, socket, data, len);
    }

    template <typename TOrigActor>
    void ReadData(
            TDerived* catcher,
            TOrigActor* orig,
            const TActorContext& ctx,
            NInterconnect::TStreamSocket* socket,
            char* data,
            size_t len) noexcept
    {
        CatchRelay.Catcher = catcher;
        TReadDataProtocolImpl::ReadData(
            &CatchRelay, orig, ctx, socket, data, len);
    }

    using TReadDataProtocolImpl::ResetReadBuf;

private:
    struct TCatch: public IReadDataCatch {
        TDerived* Catcher;

        virtual void CatchReadDataError(TString error) noexcept override
        {
            Catcher->CatchReadDataError(error);
        }

        /* returns false if the protocol should continue to read data */
        virtual bool CatchReadDataComplete(
                const TActorContext& ctx, size_t amount) noexcept override
        {
            return Catcher->CatchReadDataComplete(ctx, amount);
        }

        virtual void CatchReadDataClosed() noexcept override
        {
            Catcher->CatchReadDataClosed();
        }
    };

    TCatch CatchRelay;
};


template<>
class TReadDataProtocol<void>: virtual public TReadDataProtocolImpl {
public:
    template <typename TOrigActor>
    void ReadData(
            TOrigActor* orig,
            const TActorContext& ctx,
            NInterconnect::TStreamSocket* socket,
            char* data,
            size_t len) noexcept
    {
        ReadData(orig, orig, ctx, socket, data, len);
    }

    void ResetReadBuf(char* data, size_t len);
};


}
