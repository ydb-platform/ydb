#include "name_service_client_protocol.h"

namespace NActors {

void TResolveClientProtocol::ProtocolFunc(
        TAutoPtr<NActors::IEventHandle>& ev) noexcept
{
    switch (ev->GetTypeRewrite()) {

    case TEvAddressInfo::EventType:
        Y_ABORT_UNLESS(ev->Get<TEvAddressInfo>() != nullptr);
        Y_ABORT_UNLESS(ev->Get<TEvAddressInfo>()->Address.Get() != nullptr);

        MemLogPrintF("TResolveClientProtocol received address info");

        CatchHostAddress(TlsActivationContext->AsActorContext(), std::move(ev->Get<TEvAddressInfo>()->Address));
        break;

    case TEvResolveError::EventType:
        CatchResolveError(TlsActivationContext->AsActorContext(), std::move(ev->Get<TEvResolveError>()->Explain));
        break;

    default:
        Y_ABORT("Unknown message type dispatched");
    }
}

}
