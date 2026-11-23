#pragma once
#include "defs.h"
#include <ydb/core/protos/config.pb.h>

#include <util/generic/hash.h>

#include <optional>

namespace NKikimr {
namespace NGRpcService {

class IRequestProxyCtx;
class IRequestCtxMtSafe;

// grpc "connections" log
void AuditLogConn(const IRequestProxyCtx* reqCtx, const TString& database, const TString& userSID, const TString& sanitizedToken);

using TAuditLogParts = THashMap<TString, TString>;

// grpc "operations" log
void AuditLog(NKikimrConfig::TAuditConfig::TLogClassConfig::ELogClass logClass,
    std::optional<ui32> status, const TAuditLogParts& parts);
void AuditLogConnectDbAccessDenied(
    const IRequestProxyCtx* reqCtx,
    const TString& database,
    const TString& userSID,
    const TString& sanitizedToken,
    const TString& reason
);

}
}
