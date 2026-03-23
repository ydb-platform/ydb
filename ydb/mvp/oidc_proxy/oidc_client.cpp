#include "oidc_client.h"
#include "oidc_protected_page_handler.h"
#include "oidc_session_create_handler.h"
#include "oidc_cleanup_page.h"
#include "oidc_impersonate_start_page_nebius.h"
#include "oidc_impersonate_stop_page_nebius.h"

#include <ydb/mvp/core/http_proxy_gateway.h>
#include <ydb/library/actors/http/http_proxy.h>

namespace NMVP::NOIDC {

void InitOIDC(NActors::TActorSystem& actorSystem,
              const NActors::TActorId& baseHttpProxyId,
              const TOpenIdConnectSettings& settings) {
    const auto httpProxyId = actorSystem.Register(NHttp::CreateHttpProxy());

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/auth/callback",
                         actorSystem.Register(new TSessionCreateHandler(baseHttpProxyId, settings))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/auth/cleanup",
                         actorSystem.Register(new TCleanupPageHandler(baseHttpProxyId, settings))
                         )
                     );

    if (settings.AccessServiceType == NMvp::nebius_v1) {
        actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                            "/impersonate/start",
                            actorSystem.Register(new TImpersonateStartPageHandler(baseHttpProxyId, settings))
                            )
                        );

        actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                            "/impersonate/stop",
                            actorSystem.Register(new TImpersonateStopPageHandler(baseHttpProxyId, settings))
                            )
                        );
    }

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                        "/",
                        actorSystem.Register(new TProtectedPageHandler(baseHttpProxyId, settings))
                        )
                    );

    const auto gatewayId = actorSystem.Register(new NMVP::THttpProxyGateway(httpProxyId));

    actorSystem.Send(baseHttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                        "/",
                        gatewayId
                        )
                    );
}

} // NMVP::NOIDC
