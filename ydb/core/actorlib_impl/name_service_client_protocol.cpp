#include "name_service_client_protocol.h"

namespace NActors {

void TResolveClientProtocol::ProtocolFunc(
        TAutoPtr<NActors::IEventHandle>& ev,
        const TActorContext& ctx) noexcept
{
    switch (ev->GetTypeRewrite()) {

    case TEvAddressInfo::EventType:
        Y_VERIFY(ev->Get<TEvAddressInfo>() != nullptr);
        Y_VERIFY(ev->Get<TEvAddressInfo>()->Address.Get() != nullptr);

        MemLogPrintF("TResolveClientProtocol received address info");

        CatchHostAddress(ctx, std::move(ev->Get<TEvAddressInfo>()->Address));
        break;

    case TEvResolveError::EventType:
        CatchResolveError(ctx, std::move(ev->Get<TEvResolveError>()->Explain));
        break;

    default:
        Y_FAIL("Unknown message type dispatched");
    }
}

}
