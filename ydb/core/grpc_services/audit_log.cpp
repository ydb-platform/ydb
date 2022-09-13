#include "defs.h"
#include "audit_log.h"

#include "base/base.h"

namespace NKikimr {
namespace NGRpcService {

void AuditLog(const IRequestProxyCtx* reqCtx, const TString& database,
              const TString& subject, const TActorContext& ctx)
{
    LOG_NOTICE_S(ctx, NKikimrServices::GRPC_SERVER, "AUDIT: "
        << "request name: " << reqCtx->GetRequestName()
        << ", database: " << database
        << ", peer: " << reqCtx->GetPeerName()
        << ", subject: " << subject);
}

}
}

