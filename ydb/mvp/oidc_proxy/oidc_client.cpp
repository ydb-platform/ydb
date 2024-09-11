#include "oidc_client.h"
#include "oidc_protected_page_handler.h"
#include "oidc_session_create_handler.h"
#include "restore_context_handler.h"

namespace NMVP {
namespace NOIDC {

void InitOIDC(NActors::TActorSystem& actorSystem,
              const NActors::TActorId& httpProxyId,
              const TOpenIdConnectSettings& settings,
              TContextStorage* const contextStorage) {
    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/auth/callback",
                         actorSystem.Register(new TSessionCreateHandler(httpProxyId, settings, contextStorage))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/context",
                         actorSystem.Register(new TRestoreContextHandler(httpProxyId, contextStorage))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                        "/",
                        actorSystem.Register(new TProtectedPageHandler(httpProxyId, settings, contextStorage))
                        )
                    );
}

}  // NOIDC
}  // NMVP
