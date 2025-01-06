#include "oidc_client.h"
#include "oidc_protected_page_handler.h"
#include "oidc_session_create_handler.h"
#include "oidc_cleanup_page.h"
#include "oidc_impersonate_start_page_nebius.h"
#include "oidc_impersonate_stop_page_nebius.h"

namespace NMVP::NOIDC {

void InitOIDC(NActors::TActorSystem& actorSystem,
              const NActors::TActorId& httpProxyId,
              const TOpenIdConnectSettings& settings) {
    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/auth/callback",
                         actorSystem.Register(new TSessionCreateHandler(httpProxyId, settings))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/auth/cleanup",
                         actorSystem.Register(new TCleanupPageHandler(httpProxyId, settings))
                         )
                     );

    if (settings.AccessServiceType == NMvp::nebius_v1) {
        actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                            "/impersonate/start",
                            actorSystem.Register(new TImpersonateStartPageHandler(httpProxyId, settings))
                            )
                        );

        actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                            "/impersonate/stop",
                            actorSystem.Register(new TImpersonateStopPageHandler(httpProxyId, settings))
                            )
                        );
    }

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                        "/",
                        actorSystem.Register(new TProtectedPageHandler(httpProxyId, settings))
                        )
                    );
}

} // NMVP::NOIDC
