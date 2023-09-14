#pragma once
#include "defs.h"

namespace NKikimr {
namespace NGRpcService {

class IRequestProxyCtx;

void AuditLog(const IRequestProxyCtx* reqCtx, const TString& database,
              const TString& userSID);

}
}
