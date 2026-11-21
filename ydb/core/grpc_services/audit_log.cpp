#include "defs.h"

#include <ydb/core/util/address_classifier.h>
#include <ydb/core/audit/audit_log.h>

#include "base/base.h"
#include "audit_log.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::GRPC_SERVER

namespace NKikimr {
namespace NGRpcService {

//NOTE: EmptyValue couldn't be an empty string as AUDIT_PART() skips parts with an empty values
static const TString EmptyValue = "{none}";

namespace {

using TLogClassConfig = TAuditMode::TLogClassConfig;

struct TConnectionFields {
    const IRequestProxyCtx* Context;
    const TString& Database;
    const TString& UserSID;
    const TString& SanitizedToken;
};

void AddConnectionParts(TAuditLogParts& auditParts, const TConnectionFields& fields) {
    AUDIT_PART("component", "grpc-conn")
    AUDIT_PART("remote_address", NKikimr::NAddressClassifier::ExtractAddress(fields.Context->GetPeerName()))
    AUDIT_PART("subject", fields.UserSID)
    AUDIT_PART("sanitized_token", (!fields.SanitizedToken.empty() ? fields.SanitizedToken : EmptyValue))
    AUDIT_PART("database", fields.Database)
    AUDIT_PART("operation", fields.Context->GetRequestName())
}

void AddOperationParts(TAuditLogParts& auditParts, std::optional<ui32> status, const TAuditLogParts& parts) {
    AUDIT_PART("component", "grpc-proxy")

    // All supplied parts are required; preserve missing values explicitly.
    for (const auto& [name, value] : parts) {
        AUDIT_PART(name, (!value.empty() ? value : EmptyValue))
    }
    if (status) {
        AUDIT_PART("status", (*status == Ydb::StatusIds::SUCCESS ? "SUCCESS" : "ERROR"))
        AUDIT_PART("detailed_status", (Ydb::StatusIds::StatusCode_IsValid(*status)
            ? TString(Ydb::StatusIds::StatusCode_Name(*status))
            : ToString(*status)))
    } else {
        AUDIT_PART("status", "IN-PROCESS")
    }
}

void AuditConnClusterAdmin(const TConnectionFields& fields) {
    AUDIT_LOG(AddConnectionParts(auditParts, fields););
}

void AuditClusterAdmin(std::optional<ui32> status, const TAuditLogParts& parts) {
    AUDIT_LOG(AddOperationParts(auditParts, status, parts););
}

void AuditConnDatabaseAdmin(const TConnectionFields& fields) {
    AUDIT_LOG(AddConnectionParts(auditParts, fields););
}

void AuditDatabaseAdmin(std::optional<ui32> status, const TAuditLogParts& parts) {
    AUDIT_LOG(AddOperationParts(auditParts, status, parts););
}

void AuditConnLogin(const TConnectionFields& fields) {
    AUDIT_LOG(AddConnectionParts(auditParts, fields););
}

void AuditLogin(std::optional<ui32> status, const TAuditLogParts& parts) {
    AUDIT_LOG(AddOperationParts(auditParts, status, parts););
}

void AuditConnNodeRegistration(const TConnectionFields& fields) {
    AUDIT_LOG(AddConnectionParts(auditParts, fields););
}

void AuditNodeRegistration(std::optional<ui32> status, const TAuditLogParts& parts) {
    AUDIT_LOG(AddOperationParts(auditParts, status, parts););
}

void AuditConnDdl(const TConnectionFields& fields) {
    AUDIT_LOG(AddConnectionParts(auditParts, fields););
}

void AuditDdl(std::optional<ui32> status, const TAuditLogParts& parts) {
    AUDIT_LOG(AddOperationParts(auditParts, status, parts););
}

void AuditConnDml(const TConnectionFields& fields) {
    AUDIT_LOG(AddConnectionParts(auditParts, fields););
}

void AuditDml(std::optional<ui32> status, const TAuditLogParts& parts) {
    AUDIT_LOG(AddOperationParts(auditParts, status, parts););
}

void AuditConnOperations(const TConnectionFields& fields) {
    AUDIT_LOG(AddConnectionParts(auditParts, fields););
}

void AuditOperations(std::optional<ui32> status, const TAuditLogParts& parts) {
    AUDIT_LOG(AddOperationParts(auditParts, status, parts););
}

void AuditConnExportImport(const TConnectionFields& fields) {
    AUDIT_LOG(AddConnectionParts(auditParts, fields););
}

void AuditExportImport(std::optional<ui32> status, const TAuditLogParts& parts) {
    AUDIT_LOG(AddOperationParts(auditParts, status, parts););
}

void AuditConnAcl(const TConnectionFields& fields) {
    AUDIT_LOG(AddConnectionParts(auditParts, fields););
}

void AuditAcl(std::optional<ui32> status, const TAuditLogParts& parts) {
    AUDIT_LOG(AddOperationParts(auditParts, status, parts););
}

void AuditConnHeartbeat(const TConnectionFields& fields) {
    AUDIT_LOG(AddConnectionParts(auditParts, fields););
}

void AuditHeartbeat(std::optional<ui32> status, const TAuditLogParts& parts) {
    AUDIT_LOG(AddOperationParts(auditParts, status, parts););
}

void AuditConnDefault(const TConnectionFields& fields) {
    AUDIT_LOG(AddConnectionParts(auditParts, fields););
}

void AuditDefault(std::optional<ui32> status, const TAuditLogParts& parts) {
    AUDIT_LOG(AddOperationParts(auditParts, status, parts););
}

struct TAuditWriters {
    void (*Connection)(const TConnectionFields&);
    void (*Operation)(std::optional<ui32>, const TAuditLogParts&);
};

TAuditWriters GetAuditWriters(TLogClassConfig::ELogClass logClass) {
    switch (logClass) {
    case TLogClassConfig::ClusterAdmin:
        return {AuditConnClusterAdmin, AuditClusterAdmin};
    case TLogClassConfig::DatabaseAdmin:
        return {AuditConnDatabaseAdmin, AuditDatabaseAdmin};
    case TLogClassConfig::Login:
        return {AuditConnLogin, AuditLogin};
    case TLogClassConfig::NodeRegistration:
        return {AuditConnNodeRegistration, AuditNodeRegistration};
    case TLogClassConfig::Ddl:
        return {AuditConnDdl, AuditDdl};
    case TLogClassConfig::Dml:
        return {AuditConnDml, AuditDml};
    case TLogClassConfig::Operations:
        return {AuditConnOperations, AuditOperations};
    case TLogClassConfig::ExportImport:
        return {AuditConnExportImport, AuditExportImport};
    case TLogClassConfig::Acl:
        return {AuditConnAcl, AuditAcl};
    case TLogClassConfig::AuditHeartbeat:
        return {AuditConnHeartbeat, AuditHeartbeat};
    case TLogClassConfig::Default:
        return {AuditConnDefault, AuditDefault};
    }
    return {AuditConnDefault, AuditDefault};
}

} // namespace

void AuditLogConn(const IRequestProxyCtx* ctx, const TString& database, const TString& userSID, const TString& sanitizedToken)
{
    GetAuditWriters(ctx->GetAuditMode().LogClass).Connection({ctx, database, userSID, sanitizedToken});

    // and transitional, to be removed, output to the common log
    YDB_LOG_NOTICE("AUDIT: request",
        {"name", ctx->GetRequestName()},
        {"database", database},
        {"peer", ctx->GetPeerName()},
        {"subject", (userSID ? userSID : "no subject")});
}

void AuditLog(TLogClassConfig::ELogClass logClass, std::optional<ui32> status, const TAuditLogParts& parts)
{
    GetAuditWriters(logClass).Operation(status, parts);
}

void AuditLogConnectDbAccessDenied(
    const IRequestProxyCtx* ctx,
    const TString& database,
    const TString& userSID,
    const TString& sanitizedToken,
    const TString& reason
) {
    if (::NKikimr::NAudit::AUDIT_LOG_ENABLED.load()) {
        AuditLog(ctx->GetAuditMode().LogClass, Ydb::StatusIds::UNAUTHORIZED, {
            {"remote_address", NKikimr::NAddressClassifier::ExtractAddress(ctx->GetPeerName())},
            {"subject", userSID},
            {"sanitized_token", (!sanitizedToken.empty() ? sanitizedToken : EmptyValue)},
            {"database", database},
            {"operation", ctx->GetRequestName()},
            {"reason", reason},
        });
    }
}

}
}
