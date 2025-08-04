#include "service_legacy.h"

#include <ydb/core/client/server/msgbus_server.h>
#include <ydb/public/lib/base/msgbus.h>

namespace NKikimr::NGRpcService {

namespace NLegacyGrpcService {

void DoConsoleRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    NKikimr::NMsgBusProxy::TBusMessageContext ctx(std::move(p), NMsgBusProxy::MTYPE_CLIENT_CONSOLE_REQUEST);
    f.RegisterActor(CreateMessageBusConsoleRequest(ctx));
}

} // namespace NLegacyGrpcService

} // namespace NKikimr::NGRpcService
