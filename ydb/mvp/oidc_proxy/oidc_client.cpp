#include "oidc_client.h"
#include "oidc_protected_page.h"
#include "oidc_session_create.h"

void InitOIDC(NActors::TActorSystem& actorSystem,
              const NActors::TActorId& httpProxyId,
              const TOpenIdConnectSettings& settings) {
    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/auth/callback",
                         actorSystem.Register(new NMVP::TSessionCreator(httpProxyId, settings))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                        "/",
                        actorSystem.Register(new NMVP::TProtectedPageHandler(httpProxyId, settings))
                        )
                    );
}
