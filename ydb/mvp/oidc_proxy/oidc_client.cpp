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
              const NActors::TActorId& httpProxyId,
              const TOpenIdConnectSettings& settings) {
    const auto internalHttpProxyId = actorSystem.Register(NHttp::CreateHttpProxy());

    actorSystem.Send(internalHttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/auth/callback",
                         actorSystem.Register(new TSessionCreateHandler(httpProxyId, settings))
                         )
                     );

    actorSystem.Send(internalHttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/auth/cleanup",
                         actorSystem.Register(new TCleanupPageHandler(httpProxyId, settings))
                         )
                     );

    if (settings.AccessServiceType == NMvp::nebius_v1) {
        actorSystem.Send(internalHttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                            "/impersonate/start",
                            actorSystem.Register(new TImpersonateStartPageHandler(httpProxyId, settings))
                            )
                        );

        actorSystem.Send(internalHttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                            "/impersonate/stop",
                            actorSystem.Register(new TImpersonateStopPageHandler(httpProxyId, settings))
                            )
                        );
    }

    actorSystem.Send(internalHttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                        "/",
                        actorSystem.Register(new TProtectedPageHandler(httpProxyId, settings))
                        )
                    );

    const auto gatewayId = actorSystem.Register(new NMVP::THttpProxyGateway(internalHttpProxyId));

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/auth/callback",
                         gatewayId
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/auth/cleanup",
                         gatewayId
                         )
                     );

    if (settings.AccessServiceType == NMvp::nebius_v1) {
        actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                            "/impersonate/start",
                            gatewayId
                            )
                        );

        actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                            "/impersonate/stop",
                            gatewayId
                            )
                        );
    }

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                        "/",
                        gatewayId
                        )
                    );
}

} // NMVP::NOIDC
