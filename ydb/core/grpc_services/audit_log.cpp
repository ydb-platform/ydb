#include "defs.h"

#include <ydb/core/util/address_classifier.h>
#include <ydb/core/audit/audit_log.h>

#include "base/base.h"
#include "audit_log.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::GRPC_SERVER

namespace NKikimr::NGRpcService {

namespace {

using TLogClassConfig = TAuditMode::TLogClassConfig;

// Common request fields are supplied by AuditContextStart, AuditLogConn,
// and AuditLogConnectDbAccessDenied. Fields specific to an RPC or phase
// are emitted only when supplied.
void AuditClusterAdmin(const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool connection = parts.at("component") == "grpc-conn";
        AUDIT_PART("component", parts.at("component"))
        AUDIT_PART("remote_address", (connection || !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (connection || !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (!parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", (connection || !parts.at("database").empty() ? parts.at("database") : "{none}"))
        AUDIT_PART("operation", parts.at("operation"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (!parts.at("reason").empty() ? parts.at("reason") : "{none}"), parts.contains("reason"))
        AUDIT_PART("tablet_id", parts.at("tablet_id"), parts.contains("tablet_id"))
        AUDIT_PART("program_text", (!parts.at("program_text").empty() ? parts.at("program_text") : "{none}"), parts.contains("program_text"))
        AUDIT_PART("schema_changes", (!parts.at("schema_changes").empty() ? parts.at("schema_changes") : "{none}"), parts.contains("schema_changes"))
        AUDIT_PART("storage_pool", (!parts.at("storage_pool").empty() ? parts.at("storage_pool") : "{none}"), parts.contains("storage_pool"))
        AUDIT_PART("group", parts.at("group"), parts.contains("group"))
        AUDIT_PART("hive_id", parts.at("hive_id"), parts.contains("hive_id"))
        AUDIT_PART("request", (!parts.at("request").empty() ? parts.at("request") : "{none}"), parts.contains("request"))
        AUDIT_PART("status", parts.at("status"), parts.contains("status"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

void AuditDatabaseAdmin(const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool connection = parts.at("component") == "grpc-conn";
        AUDIT_PART("component", parts.at("component"))
        AUDIT_PART("remote_address", (connection || !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (connection || !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (!parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", (connection || !parts.at("database").empty() ? parts.at("database") : "{none}"))
        AUDIT_PART("operation", parts.at("operation"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (!parts.at("reason").empty() ? parts.at("reason") : "{none}"), parts.contains("reason"))
        AUDIT_PART("status", parts.at("status"), parts.contains("status"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

void AuditLogin(const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool connection = parts.at("component") == "grpc-conn";
        AUDIT_PART("component", parts.at("component"))
        AUDIT_PART("remote_address", (connection || !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (connection || !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (!parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", (connection || !parts.at("database").empty() ? parts.at("database") : "{none}"))
        AUDIT_PART("operation", parts.at("operation"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (!parts.at("reason").empty() ? parts.at("reason") : "{none}"), parts.contains("reason"))
        AUDIT_PART("status", parts.at("status"), parts.contains("status"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

void AuditNodeRegistration(const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool connection = parts.at("component") == "grpc-conn";
        AUDIT_PART("component", parts.at("component"))
        AUDIT_PART("remote_address", (connection || !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (connection || !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (!parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", (connection || !parts.at("database").empty() ? parts.at("database") : "{none}"))
        AUDIT_PART("operation", parts.at("operation"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (!parts.at("reason").empty() ? parts.at("reason") : "{none}"), parts.contains("reason"))
        AUDIT_PART("request", (!parts.at("request").empty() ? parts.at("request") : "{none}"), parts.contains("request"))
        AUDIT_PART("status", parts.at("status"), parts.contains("status"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

void AuditDdl(const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool connection = parts.at("component") == "grpc-conn";
        AUDIT_PART("component", parts.at("component"))
        AUDIT_PART("remote_address", (connection || !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (connection || !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (!parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", (connection || !parts.at("database").empty() ? parts.at("database") : "{none}"))
        AUDIT_PART("operation", parts.at("operation"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (!parts.at("reason").empty() ? parts.at("reason") : "{none}"), parts.contains("reason"))
        AUDIT_PART("status", parts.at("status"), parts.contains("status"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

void AuditDml(const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool connection = parts.at("component") == "grpc-conn";
        AUDIT_PART("component", parts.at("component"))
        AUDIT_PART("remote_address", (connection || !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (connection || !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (!parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", (connection || !parts.at("database").empty() ? parts.at("database") : "{none}"))
        AUDIT_PART("operation", parts.at("operation"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (!parts.at("reason").empty() ? parts.at("reason") : "{none}"), parts.contains("reason"))
        AUDIT_PART("query_text", (!parts.at("query_text").empty() ? parts.at("query_text") : "{none}"), parts.contains("query_text"))
        AUDIT_PART("prepared_query_id", (!parts.at("prepared_query_id").empty() ? parts.at("prepared_query_id") : "{none}"), parts.contains("prepared_query_id"))
        AUDIT_PART("tx_id", (!parts.at("tx_id").empty() ? parts.at("tx_id") : "{none}"), parts.contains("tx_id"))
        AUDIT_PART("begin_tx", parts.at("begin_tx"), parts.contains("begin_tx"))
        AUDIT_PART("commit_tx", parts.at("commit_tx"), parts.contains("commit_tx"))
        AUDIT_PART("table", (!parts.at("table").empty() ? parts.at("table") : "{none}"), parts.contains("table"))
        AUDIT_PART("row_count", parts.at("row_count"), parts.contains("row_count"))
        AUDIT_PART("request", (!parts.at("request").empty() ? parts.at("request") : "{none}"), parts.contains("request"))
        AUDIT_PART("status", parts.at("status"), parts.contains("status"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

void AuditOperations(const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool connection = parts.at("component") == "grpc-conn";
        AUDIT_PART("component", parts.at("component"))
        AUDIT_PART("remote_address", (connection || !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (connection || !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (!parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", (connection || !parts.at("database").empty() ? parts.at("database") : "{none}"))
        AUDIT_PART("operation", parts.at("operation"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (!parts.at("reason").empty() ? parts.at("reason") : "{none}"), parts.contains("reason"))
        AUDIT_PART("hive_id", parts.at("hive_id"), parts.contains("hive_id"))
        AUDIT_PART("hive_generation", parts.at("hive_generation"), parts.contains("hive_generation"))
        AUDIT_PART("reassign_id", parts.at("reassign_id"), parts.contains("reassign_id"))
        AUDIT_PART("status", parts.at("status"), parts.contains("status"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

void AuditExportImport(const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool connection = parts.at("component") == "grpc-conn";
        AUDIT_PART("component", parts.at("component"))
        AUDIT_PART("remote_address", (connection || !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (connection || !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (!parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", (connection || !parts.at("database").empty() ? parts.at("database") : "{none}"))
        AUDIT_PART("operation", parts.at("operation"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (!parts.at("reason").empty() ? parts.at("reason") : "{none}"), parts.contains("reason"))
        AUDIT_PART("status", parts.at("status"), parts.contains("status"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

void AuditAcl(const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool connection = parts.at("component") == "grpc-conn";
        AUDIT_PART("component", parts.at("component"))
        AUDIT_PART("remote_address", (connection || !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (connection || !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (!parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", (connection || !parts.at("database").empty() ? parts.at("database") : "{none}"))
        AUDIT_PART("operation", parts.at("operation"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (!parts.at("reason").empty() ? parts.at("reason") : "{none}"), parts.contains("reason"))
        AUDIT_PART("status", parts.at("status"), parts.contains("status"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

void AuditHeartbeat(const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool connection = parts.at("component") == "grpc-conn";
        AUDIT_PART("component", parts.at("component"))
        AUDIT_PART("remote_address", (connection || !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (connection || !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (!parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", (connection || !parts.at("database").empty() ? parts.at("database") : "{none}"))
        AUDIT_PART("operation", parts.at("operation"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (!parts.at("reason").empty() ? parts.at("reason") : "{none}"), parts.contains("reason"))
        AUDIT_PART("status", parts.at("status"), parts.contains("status"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

void AuditDefault(const TAuditLogParts& parts) {
    AUDIT_LOG(
        const bool connection = parts.at("component") == "grpc-conn";
        AUDIT_PART("component", parts.at("component"))
        AUDIT_PART("remote_address", (connection || !parts.at("remote_address").empty() ? parts.at("remote_address") : "{none}"))
        AUDIT_PART("subject", (connection || !parts.at("subject").empty() ? parts.at("subject") : "{none}"))
        AUDIT_PART("sanitized_token", (!parts.at("sanitized_token").empty() ? parts.at("sanitized_token") : "{none}"))
        AUDIT_PART("database", (connection || !parts.at("database").empty() ? parts.at("database") : "{none}"))
        AUDIT_PART("operation", parts.at("operation"))
        AUDIT_PART("grpc_method", parts.at("grpc_method"), parts.contains("grpc_method"))
        AUDIT_PART("start_time", parts.at("start_time"), parts.contains("start_time"))
        AUDIT_PART("end_time", parts.at("end_time"), parts.contains("end_time"))
        AUDIT_PART("cloud_id", parts.at("cloud_id"), parts.contains("cloud_id"))
        AUDIT_PART("folder_id", parts.at("folder_id"), parts.contains("folder_id"))
        AUDIT_PART("resource_id", parts.at("resource_id"), parts.contains("resource_id"))
        AUDIT_PART("reason", (!parts.at("reason").empty() ? parts.at("reason") : "{none}"), parts.contains("reason"))
        AUDIT_PART("status", parts.at("status"), parts.contains("status"))
        AUDIT_PART("detailed_status", parts.at("detailed_status"), parts.contains("detailed_status"))
    );
}

using TAuditWriter = void (*)(const TAuditLogParts&);

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
        GetAuditWriter(ctx->GetAuditMode().LogClass)({
            {"component", "grpc-conn"},
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
        logParts["component"] = "grpc-proxy";
        if (status) {
            logParts["status"] = *status == Ydb::StatusIds::SUCCESS ? "SUCCESS" : "ERROR";
            logParts["detailed_status"] = Ydb::StatusIds::StatusCode_IsValid(*status)
                ? TString(Ydb::StatusIds::StatusCode_Name(*status))
                : ToString(*status);
        } else {
            logParts["status"] = "IN-PROCESS";
        }
        GetAuditWriter(logClass)(logParts);
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
