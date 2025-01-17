#pragma once
#include "defs.h"

namespace Ydb::Auth {

class LoginRequest;
class LoginResponse;

}

namespace NKikimr::NGRpcService {

class IAuditCtx;

// logins log
void AuditLogLogin(IAuditCtx* ctx, const TString& database, const Ydb::Auth::LoginRequest& request,
                   const Ydb::Auth::LoginResponse& response, const TString& errorDetails,
                   const TString& sanitizedToken, bool isAdmin = false);

} // namespace NKikimr::NGRpcService
