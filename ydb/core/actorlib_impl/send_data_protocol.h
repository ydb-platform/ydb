#pragma once

#include <ydb/library/actors/interconnect/events_local.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect_stream.h>

namespace NActors {

class TSendDataProtocol {
public:
    template <typename TOrigActor>
    void SendData(
        TOrigActor* orig,
        const TActorContext& ctx,
        NInterconnect::TStreamSocket* socket,
        const char* data,
        size_t len) noexcept
    {
        Socket = socket;
        Data = data;
        Len = len;
        Cancelled = false;

        orig->template AddMsgProtocol<TEvSocketReadyWrite>(
            &TOrigActor::template CallProtocolStateFunc<
                TOrigActor,
                TSendDataProtocol,
                &TSendDataProtocol::ProtocolFunc>);

        TryAgain(ctx);
    }

    virtual void CatchSendDataError(TString error) noexcept = 0;

    virtual void CatchSendDataComplete(
        const TActorContext& ctx) noexcept = 0;

    void CancelSendData(const TActorContext& ctx) noexcept;

    virtual ~TSendDataProtocol() = default;

private:
    void ProtocolFunc(
        TAutoPtr<NActors::IEventHandle>& ev) noexcept;

    void TryAgain(const TActorContext& ctx) noexcept;

    NInterconnect::TStreamSocket* Socket;
    const char* Data;
    size_t Len;
    bool Cancelled;
};

}
