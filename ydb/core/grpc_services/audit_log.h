#pragma once
#include "defs.h"

#include <optional>

namespace NKikimr {
namespace NGRpcService {

class IRequestProxyCtx;
class IRequestCtxMtSafe;

// grpc "connections" log
void AuditLogConn(const IRequestProxyCtx* reqCtx, const TString& database, const TString& userSID, const TString& sanitizedToken);

using TAuditLogParts = TVector<std::pair<TString, TString>>;

// grpc "operations" log
void AuditLog(std::optional<ui32> status, const TAuditLogParts& parts);
void AuditLogConnectDbAccessDenied(const IRequestProxyCtx* reqCtx, const TString& database, const TString& userSID, const TString& sanitizedToken);

}
}
