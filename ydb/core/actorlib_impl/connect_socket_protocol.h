#pragma once

#include <ydb/library/actors/interconnect/events_local.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect_stream.h>

namespace NActors {

class TConnectSocketProtocol {
public:
    template <typename TOrigActor>
    void ConnectSocket(
        TOrigActor* orig,
        const TActorContext& ctx,
        NAddr::IRemoteAddrRef address) noexcept
    {
        Y_ABORT_UNLESS(address.Get() != nullptr);
        Y_ABORT_UNLESS(orig != nullptr);

        orig->template AddMsgProtocol<TEvConnectWakeup>(
            &TOrigActor::template CallProtocolStateFunc<
                TOrigActor,
                TConnectSocketProtocol,
                &TConnectSocketProtocol::ProtocolFunc>);

        orig->template AddMsgProtocol<TEvSocketConnect>(
            &TOrigActor::template CallProtocolStateFunc<
                TOrigActor,
                TConnectSocketProtocol,
                &TConnectSocketProtocol::ProtocolFunc>);

        orig->template AddMsgProtocol<TEvSocketError>(
            &TOrigActor::template CallProtocolStateFunc<
                TOrigActor,
                TConnectSocketProtocol,
                &TConnectSocketProtocol::ProtocolFunc>);

        Address = std::move(address);
        TryAgain(ctx);
    }

    virtual void CatchConnectError(
        const TActorContext& ctx, TString error) noexcept = 0;

    virtual void CatchConnectedSocket(
        const TActorContext& ctx,
        TIntrusivePtr<NInterconnect::TStreamSocket> socket) noexcept = 0;

    virtual ~TConnectSocketProtocol() = default;

private:
    void ProtocolFunc(
        TAutoPtr<NActors::IEventHandle>& ev) noexcept;

    void TryAgain(const TActorContext& ctx) noexcept;
    void CheckRetry(const TActorContext& ctx, TString explain) noexcept;
    bool CheckConnectResult(const TActorContext& ctx, int result) noexcept;

    NAddr::IRemoteAddrRef Address;
    ui16 Retries = 0;
    TIntrusivePtr<NInterconnect::TStreamSocket> Socket;
};

}
