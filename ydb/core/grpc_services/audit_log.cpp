#include "defs.h"
#include "audit_log.h"

#include "base/base.h"

#include <ydb/core/audit/audit_log.h>

namespace NKikimr {
namespace NGRpcService {

void AuditLog(const IRequestProxyCtx* reqCtx, const TString& database,
              const TString& userSID)
{
    static const TString GrpcServerComponentName = "grpc-server";

    //NOTE: EmptyValue couldn't be an empty string as AUDIT_PART() skips parts with an empty values
    static const TString EmptyValue = "{none}";

    const auto peerName = reqCtx->GetPeerName();

    AUDIT_LOG(
        AUDIT_PART("component", GrpcServerComponentName)
        AUDIT_PART("remote_address", (!peerName.empty() ? peerName : EmptyValue) )
        AUDIT_PART("subject", (!userSID.empty() ? userSID : EmptyValue))
        AUDIT_PART("database", database)
        AUDIT_PART("operation", reqCtx->GetRequestName())
        // AUDIT_PART("status", "SUCCESS")
        // AUDIT_PART("detailed_status", NKikimrScheme::EStatus_Name(response->Record.GetStatus()))
        // AUDIT_PART("reason", response->Record.GetReason(), response->Record.HasReason())
    );
}

}
}

