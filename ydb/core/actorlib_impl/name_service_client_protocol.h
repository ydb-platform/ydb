#pragma once

#include <ydb/library/actors/memory_log/memlog.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/interconnect/events_local.h>

namespace NActors {

class TResolveClientProtocol {
public:
    template <typename TOrigActor>
    void SendResolveMessage(
        TOrigActor* orig,
        const TActorContext& ctx,
        TString address,
        ui16 port) noexcept
    {
        auto msg = new TEvResolveAddress;
        msg->Address = std::move(address);
        msg->Port = port;

        MemLogPrintF("TResolveClientProtocol send name request: %s, %u",
                     msg->Address.data(), (int)msg->Port);

        ctx.Send(GetNameserviceActorId(), msg);

        orig->template AddMsgProtocol<TEvAddressInfo>(
            &TOrigActor::template CallProtocolStateFunc<
                TOrigActor,
                TResolveClientProtocol,
                &TResolveClientProtocol::ProtocolFunc>);

        orig->template AddMsgProtocol<TEvResolveError>(
            &TOrigActor::template CallProtocolStateFunc<
                TOrigActor,
                TResolveClientProtocol,
                &TResolveClientProtocol::ProtocolFunc>);
    }

    virtual void CatchHostAddress(
        const TActorContext& ctx, NAddr::IRemoteAddrPtr address) noexcept = 0;

    virtual void CatchResolveError(
        const TActorContext& ctx, TString error) noexcept = 0;

    virtual ~TResolveClientProtocol() = default;

private:
    void ProtocolFunc(
        TAutoPtr<NActors::IEventHandle>& ev) noexcept;
};

}
