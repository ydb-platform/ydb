#include "oidc_client.h"
#include "oidc_protected_page_handler.h"
#include "oidc_session_create_handler.h"

namespace NMVP {
namespace NOIDC {

void InitOIDC(NActors::TActorSystem& actorSystem,
              const NActors::TActorId& httpProxyId,
              const TOpenIdConnectSettings& settings) {
    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/auth/callback",
                         actorSystem.Register(new TSessionCreateHandler(httpProxyId, settings))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/impersonate/start",
                         actorSystem.Register(new THandlerImpersonateStart(httpProxyId, settings))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/impersonate/stop",
                         actorSystem.Register(new THandlerImpersonateStop(httpProxyId, settings))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                        "/",
                        actorSystem.Register(new TProtectedPageHandler(httpProxyId, settings))
                        )
                    );
}

}  // NOIDC
}  // NMVP
