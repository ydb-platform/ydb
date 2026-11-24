#include "defs.h"

#include <ydb/core/util/address_classifier.h>
#include <ydb/core/audit/audit_log.h>

#include "base/base.h"
#include "audit_log.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::GRPC_SERVER

namespace NKikimr::NGRpcService {

namespace {

using TLogClassConfig = TAuditMode::TLogClassConfig;

// Missing or empty required fields use {none}; missing or empty optional fields are omitted.
// A failure reason is required only for ERROR records.
void AuditClusterAdmin(bool connection, const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool error = parts.contains("status") && parts.at("status") == "ERROR";
        AUDIT_PART("component", (connection ? "grpc-conn" : "grpc-proxy"))
        AUDIT_PART("remote_address", (parts.contains("remote_address") && !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (parts.contains("subject") && !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (parts.contains("sanitized_token") && !parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", parts.at("database"), parts.contains("database"))
        AUDIT_PART("operation", (parts.contains("operation") && !parts.at("operation").empty() ? parts.at("operation") : "{none}"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (parts.contains("reason") && !parts.at("reason").empty() ? parts.at("reason") : "{none}"),
            error || (parts.contains("reason") && !parts.at("reason").empty()))
        AUDIT_PART("tablet_id", parts.at("tablet_id"), parts.contains("tablet_id"))
        AUDIT_PART("program_text", parts.at("program_text"), parts.contains("program_text"))
        AUDIT_PART("schema_changes", parts.at("schema_changes"), parts.contains("schema_changes"))
        AUDIT_PART("storage_pool", parts.at("storage_pool"), parts.contains("storage_pool"))
        AUDIT_PART("group", parts.at("group"), parts.contains("group"))
        AUDIT_PART("hive_id", parts.at("hive_id"), parts.contains("hive_id"))
        AUDIT_PART("request", parts.at("request"), parts.contains("request"))
        AUDIT_PART("status", (parts.contains("status") && !parts.at("status").empty() ? parts.at("status") : "{none}"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

void AuditDatabaseAdmin(bool connection, const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool error = parts.contains("status") && parts.at("status") == "ERROR";
        AUDIT_PART("component", (connection ? "grpc-conn" : "grpc-proxy"))
        AUDIT_PART("remote_address", (parts.contains("remote_address") && !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (parts.contains("subject") && !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (parts.contains("sanitized_token") && !parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", parts.at("database"), parts.contains("database"))
        AUDIT_PART("operation", (parts.contains("operation") && !parts.at("operation").empty() ? parts.at("operation") : "{none}"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (parts.contains("reason") && !parts.at("reason").empty() ? parts.at("reason") : "{none}"),
            error || (parts.contains("reason") && !parts.at("reason").empty()))
        AUDIT_PART("status", (parts.contains("status") && !parts.at("status").empty() ? parts.at("status") : "{none}"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

void AuditLogin(bool connection, const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool error = parts.contains("status") && parts.at("status") == "ERROR";
        AUDIT_PART("component", (connection ? "grpc-conn" : "grpc-proxy"))
        AUDIT_PART("remote_address", (parts.contains("remote_address") && !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (parts.contains("subject") && !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (parts.contains("sanitized_token") && !parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", parts.at("database"), parts.contains("database"))
        AUDIT_PART("operation", (parts.contains("operation") && !parts.at("operation").empty() ? parts.at("operation") : "{none}"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (parts.contains("reason") && !parts.at("reason").empty() ? parts.at("reason") : "{none}"),
            error || (parts.contains("reason") && !parts.at("reason").empty()))
        AUDIT_PART("status", (parts.contains("status") && !parts.at("status").empty() ? parts.at("status") : "{none}"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

void AuditNodeRegistration(bool connection, const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool error = parts.contains("status") && parts.at("status") == "ERROR";
        AUDIT_PART("component", (connection ? "grpc-conn" : "grpc-proxy"))
        AUDIT_PART("remote_address", (parts.contains("remote_address") && !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (parts.contains("subject") && !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (parts.contains("sanitized_token") && !parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", parts.at("database"), parts.contains("database"))
        AUDIT_PART("operation", (parts.contains("operation") && !parts.at("operation").empty() ? parts.at("operation") : "{none}"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (parts.contains("reason") && !parts.at("reason").empty() ? parts.at("reason") : "{none}"),
            error || (parts.contains("reason") && !parts.at("reason").empty()))
        AUDIT_PART("request", parts.at("request"), parts.contains("request"))
        AUDIT_PART("status", (parts.contains("status") && !parts.at("status").empty() ? parts.at("status") : "{none}"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

void AuditDdl(bool connection, const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool error = parts.contains("status") && parts.at("status") == "ERROR";
        AUDIT_PART("component", (connection ? "grpc-conn" : "grpc-proxy"))
        AUDIT_PART("remote_address", (parts.contains("remote_address") && !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (parts.contains("subject") && !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (parts.contains("sanitized_token") && !parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", parts.at("database"), parts.contains("database"))
        AUDIT_PART("operation", (parts.contains("operation") && !parts.at("operation").empty() ? parts.at("operation") : "{none}"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (parts.contains("reason") && !parts.at("reason").empty() ? parts.at("reason") : "{none}"),
            error || (parts.contains("reason") && !parts.at("reason").empty()))
        AUDIT_PART("status", (parts.contains("status") && !parts.at("status").empty() ? parts.at("status") : "{none}"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

void AuditDml(bool connection, const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool error = parts.contains("status") && parts.at("status") == "ERROR";
        AUDIT_PART("component", (connection ? "grpc-conn" : "grpc-proxy"))
        AUDIT_PART("remote_address", (parts.contains("remote_address") && !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (parts.contains("subject") && !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (parts.contains("sanitized_token") && !parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", parts.at("database"), parts.contains("database"))
        AUDIT_PART("operation", (parts.contains("operation") && !parts.at("operation").empty() ? parts.at("operation") : "{none}"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (parts.contains("reason") && !parts.at("reason").empty() ? parts.at("reason") : "{none}"),
            error || (parts.contains("reason") && !parts.at("reason").empty()))
        AUDIT_PART("query_text", parts.at("query_text"), parts.contains("query_text"))
        AUDIT_PART("prepared_query_id", parts.at("prepared_query_id"), parts.contains("prepared_query_id"))
        AUDIT_PART("tx_id", parts.at("tx_id"), parts.contains("tx_id"))
        AUDIT_PART("begin_tx", parts.at("begin_tx"), parts.contains("begin_tx"))
        AUDIT_PART("commit_tx", parts.at("commit_tx"), parts.contains("commit_tx"))
        AUDIT_PART("table", parts.at("table"), parts.contains("table"))
        AUDIT_PART("row_count", parts.at("row_count"), parts.contains("row_count"))
        AUDIT_PART("request", parts.at("request"), parts.contains("request"))
        AUDIT_PART("status", (parts.contains("status") && !parts.at("status").empty() ? parts.at("status") : "{none}"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

void AuditOperations(bool connection, const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool error = parts.contains("status") && parts.at("status") == "ERROR";
        AUDIT_PART("component", (connection ? "grpc-conn" : "grpc-proxy"))
        AUDIT_PART("remote_address", (parts.contains("remote_address") && !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (parts.contains("subject") && !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (parts.contains("sanitized_token") && !parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", parts.at("database"), parts.contains("database"))
        AUDIT_PART("operation", (parts.contains("operation") && !parts.at("operation").empty() ? parts.at("operation") : "{none}"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (parts.contains("reason") && !parts.at("reason").empty() ? parts.at("reason") : "{none}"),
            error || (parts.contains("reason") && !parts.at("reason").empty()))
        AUDIT_PART("hive_id", parts.at("hive_id"), parts.contains("hive_id"))
        AUDIT_PART("hive_generation", parts.at("hive_generation"), parts.contains("hive_generation"))
        AUDIT_PART("reassign_id", parts.at("reassign_id"), parts.contains("reassign_id"))
        AUDIT_PART("status", (parts.contains("status") && !parts.at("status").empty() ? parts.at("status") : "{none}"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

void AuditExportImport(bool connection, const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool error = parts.contains("status") && parts.at("status") == "ERROR";
        AUDIT_PART("component", (connection ? "grpc-conn" : "grpc-proxy"))
        AUDIT_PART("remote_address", (parts.contains("remote_address") && !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (parts.contains("subject") && !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (parts.contains("sanitized_token") && !parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", parts.at("database"), parts.contains("database"))
        AUDIT_PART("operation", (parts.contains("operation") && !parts.at("operation").empty() ? parts.at("operation") : "{none}"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (parts.contains("reason") && !parts.at("reason").empty() ? parts.at("reason") : "{none}"),
            error || (parts.contains("reason") && !parts.at("reason").empty()))
        AUDIT_PART("status", (parts.contains("status") && !parts.at("status").empty() ? parts.at("status") : "{none}"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

void AuditAcl(bool connection, const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool error = parts.contains("status") && parts.at("status") == "ERROR";
        AUDIT_PART("component", (connection ? "grpc-conn" : "grpc-proxy"))
        AUDIT_PART("remote_address", (parts.contains("remote_address") && !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (parts.contains("subject") && !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (parts.contains("sanitized_token") && !parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", parts.at("database"), parts.contains("database"))
        AUDIT_PART("operation", (parts.contains("operation") && !parts.at("operation").empty() ? parts.at("operation") : "{none}"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (parts.contains("reason") && !parts.at("reason").empty() ? parts.at("reason") : "{none}"),
            error || (parts.contains("reason") && !parts.at("reason").empty()))
        AUDIT_PART("status", (parts.contains("status") && !parts.at("status").empty() ? parts.at("status") : "{none}"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

void AuditHeartbeat(bool connection, const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool error = parts.contains("status") && parts.at("status") == "ERROR";
        AUDIT_PART("component", (connection ? "grpc-conn" : "grpc-proxy"))
        AUDIT_PART("remote_address", (parts.contains("remote_address") && !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (parts.contains("subject") && !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (parts.contains("sanitized_token") && !parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", parts.at("database"), parts.contains("database"))
        AUDIT_PART("operation", (parts.contains("operation") && !parts.at("operation").empty() ? parts.at("operation") : "{none}"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (parts.contains("reason") && !parts.at("reason").empty() ? parts.at("reason") : "{none}"),
            error || (parts.contains("reason") && !parts.at("reason").empty()))
        AUDIT_PART("status", (parts.contains("status") && !parts.at("status").empty() ? parts.at("status") : "{none}"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

void AuditDefault(bool connection, const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool error = parts.contains("status") && parts.at("status") == "ERROR";
        AUDIT_PART("component", (connection ? "grpc-conn" : "grpc-proxy"))
        AUDIT_PART("remote_address", (parts.contains("remote_address") && !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (parts.contains("subject") && !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (parts.contains("sanitized_token") && !parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", parts.at("database"), parts.contains("database"))
        AUDIT_PART("operation", (parts.contains("operation") && !parts.at("operation").empty() ? parts.at("operation") : "{none}"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (parts.contains("reason") && !parts.at("reason").empty() ? parts.at("reason") : "{none}"),
            error || (parts.contains("reason") && !parts.at("reason").empty()))
        AUDIT_PART("status", (parts.contains("status") && !parts.at("status").empty() ? parts.at("status") : "{none}"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

using TAuditWriter = void (*)(bool connection, const TAuditLogParts&);

TAuditWriter GetAuditWriter(TLogClassConfig::ELogClass logClass) {
    switch (logClass) {
    case TLogClassConfig::ClusterAdmin:
        return AuditClusterAdmin;
    case TLogClassConfig::DatabaseAdmin:
        return AuditDatabaseAdmin;
    case TLogClassConfig::Login:
        return AuditLogin;
    case TLogClassConfig::NodeRegistration:
        return AuditNodeRegistration;
    case TLogClassConfig::Ddl:
        return AuditDdl;
    case TLogClassConfig::Dml:
        return AuditDml;
    case TLogClassConfig::Operations:
        return AuditOperations;
    case TLogClassConfig::ExportImport:
        return AuditExportImport;
    case TLogClassConfig::Acl:
        return AuditAcl;
    case TLogClassConfig::AuditHeartbeat:
        return AuditHeartbeat;
    case TLogClassConfig::Default:
        return AuditDefault;
    }
    return AuditDefault;
}

} // namespace

void AuditLogConn(const IRequestProxyCtx* ctx, const TString& database,
        const TString& userSID, const TString& sanitizedToken) {
    if (NAudit::AUDIT_LOG_ENABLED.load()) {
        GetAuditWriter(ctx->GetAuditMode().LogClass)(true, {
            {"status", "SUCCESS"},
            {"remote_address", NAddressClassifier::ExtractAddress(ctx->GetPeerName())},
            {"subject", userSID},
            {"sanitized_token", sanitizedToken},
            {"database", database},
            {"operation", ctx->GetRequestName()},
        });
    }

    // and transitional, to be removed, output to the common log
    YDB_LOG_NOTICE("AUDIT: request",
        {"name", ctx->GetRequestName()},
        {"database", database},
        {"peer", ctx->GetPeerName()},
        {"subject", (userSID ? userSID : "no subject")});
}

void AuditLog(TLogClassConfig::ELogClass logClass, std::optional<ui32> status,
        const TAuditLogParts& parts) {
    if (NAudit::AUDIT_LOG_ENABLED.load()) {
        auto logParts = parts;
        if (status) {
            logParts["status"] = *status == Ydb::StatusIds::SUCCESS ? "SUCCESS" : "ERROR";
            logParts["detailed_status"] = Ydb::StatusIds::StatusCode_IsValid(*status)
                ? TString(Ydb::StatusIds::StatusCode_Name(*status))
                : ToString(*status);
        } else {
            logParts["status"] = "IN-PROCESS";
        }
        GetAuditWriter(logClass)(false, logParts);
    }
}

void AuditLogConnectDbAccessDenied(
    const IRequestProxyCtx* ctx,
    const TString& database,
    const TString& userSID,
    const TString& sanitizedToken,
    const TString& reason
) {
    if (NAudit::AUDIT_LOG_ENABLED.load()) {
        AuditLog(ctx->GetAuditMode().LogClass, Ydb::StatusIds::UNAUTHORIZED, {
            {"remote_address", NAddressClassifier::ExtractAddress(ctx->GetPeerName())},
            {"subject", userSID},
            {"sanitized_token", sanitizedToken},
            {"database", database},
            {"operation", ctx->GetRequestName()},
            {"reason", reason},
        });
    }
}

} // namespace NKikimr::NGRpcService
