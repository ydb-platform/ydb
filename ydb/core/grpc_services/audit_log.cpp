#include "defs.h"

#include <ydb/core/util/address_classifier.h>
#include <ydb/core/audit/audit_log.h>

#include "base/base.h"
#include "audit_log.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::GRPC_SERVER

namespace NKikimr::NGRpcService {

namespace {

using TLogClassConfig = TAuditMode::TLogClassConfig;

// AUDIT_PART skips empty values. A supplied but empty operation field must
// therefore be distinguished from a field that was never supplied.
const TString EmptyValue = "{none}";

struct TGrpcAuditContext {
    TString Component;
    TString Status;
    TString DetailedStatus;

    TString RemoteAddress;
    TString Subject;
    TString SanitizedToken;
    TString Database;
    TString Operation;
    TString GrpcMethod;
    TString StartTime;
    TString EndTime;
    TString CloudId;
    TString FolderId;
    TString ResourceId;
    TString Reason;
    TString QueryText;
    TString PreparedQueryId;
    TString TxId;
    TString BeginTx;
    TString CommitTx;
    TString Table;
    TString RowCount;
    TString TabletId;
    TString ProgramText;
    TString SchemaChanges;
    TString StoragePool;
    TString Group;
    TString HiveId;
    TString HiveGeneration;
    TString ReassignId;
    TString Request;
};

TString GetOperationField(const TAuditLogParts& parts, TStringBuf name) {
    for (const auto& [partName, value] : parts) {
        if (partName == name) {
            return value.empty() ? EmptyValue : value;
        }
    }
    return {};
}

TGrpcAuditContext MakeOperationContext(std::optional<ui32> status, const TAuditLogParts& parts) {
    TGrpcAuditContext context;
    context.Component = "grpc-proxy";
    if (status) {
        context.Status = *status == Ydb::StatusIds::SUCCESS ? "SUCCESS" : "ERROR";
        context.DetailedStatus = Ydb::StatusIds::StatusCode_IsValid(*status)
            ? TString(Ydb::StatusIds::StatusCode_Name(*status))
            : ToString(*status);
    } else {
        context.Status = "IN-PROCESS";
    }
    context.RemoteAddress = GetOperationField(parts, "remote_address");
    context.Subject = GetOperationField(parts, "subject");
    context.SanitizedToken = GetOperationField(parts, "sanitized_token");
    context.Database = GetOperationField(parts, "database");
    context.Operation = GetOperationField(parts, "operation");
    context.GrpcMethod = GetOperationField(parts, "grpc_method");
    context.StartTime = GetOperationField(parts, "start_time");
    context.EndTime = GetOperationField(parts, "end_time");
    context.CloudId = GetOperationField(parts, "cloud_id");
    context.FolderId = GetOperationField(parts, "folder_id");
    context.ResourceId = GetOperationField(parts, "resource_id");
    context.Reason = GetOperationField(parts, "reason");
    context.QueryText = GetOperationField(parts, "query_text");
    context.PreparedQueryId = GetOperationField(parts, "prepared_query_id");
    context.TxId = GetOperationField(parts, "tx_id");
    context.BeginTx = GetOperationField(parts, "begin_tx");
    context.CommitTx = GetOperationField(parts, "commit_tx");
    context.Table = GetOperationField(parts, "table");
    context.RowCount = GetOperationField(parts, "row_count");
    context.TabletId = GetOperationField(parts, "tablet_id");
    context.ProgramText = GetOperationField(parts, "program_text");
    context.SchemaChanges = GetOperationField(parts, "schema_changes");
    context.StoragePool = GetOperationField(parts, "storage_pool");
    context.Group = GetOperationField(parts, "group");
    context.HiveId = GetOperationField(parts, "hive_id");
    context.HiveGeneration = GetOperationField(parts, "hive_generation");
    context.ReassignId = GetOperationField(parts, "reassign_id");
    context.Request = GetOperationField(parts, "request");
    return context;
}

TGrpcAuditContext MakeConnectionContext(const IRequestProxyCtx* request,
        const TString& database, const TString& userSID, const TString& sanitizedToken) {
    TGrpcAuditContext context;
    context.Component = "grpc-conn";
    context.RemoteAddress = NAddressClassifier::ExtractAddress(request->GetPeerName());
    context.Subject = userSID;
    context.SanitizedToken = sanitizedToken.empty() ? EmptyValue : sanitizedToken;
    context.Database = database;
    context.Operation = request->GetRequestName();
    return context;
}

void AuditClusterAdmin(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address", context.RemoteAddress)
        AUDIT_PART("subject", context.Subject)
        AUDIT_PART("sanitized_token", context.SanitizedToken)
        AUDIT_PART("database", context.Database)
        AUDIT_PART("operation", context.Operation)
        AUDIT_PART("grpc_method", context.GrpcMethod)
        AUDIT_PART("start_time", context.StartTime)
        AUDIT_PART("end_time", context.EndTime)
        AUDIT_PART("cloud_id", context.CloudId)
        AUDIT_PART("folder_id", context.FolderId)
        AUDIT_PART("resource_id", context.ResourceId)
        AUDIT_PART("reason", context.Reason)
        AUDIT_PART("tablet_id", context.TabletId)
        AUDIT_PART("program_text", context.ProgramText)
        AUDIT_PART("schema_changes", context.SchemaChanges)
        AUDIT_PART("storage_pool", context.StoragePool)
        AUDIT_PART("group", context.Group)
        AUDIT_PART("hive_id", context.HiveId)
        AUDIT_PART("request", context.Request)
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditDatabaseAdmin(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address", context.RemoteAddress)
        AUDIT_PART("subject", context.Subject)
        AUDIT_PART("sanitized_token", context.SanitizedToken)
        AUDIT_PART("database", context.Database)
        AUDIT_PART("operation", context.Operation)
        AUDIT_PART("grpc_method", context.GrpcMethod)
        AUDIT_PART("start_time", context.StartTime)
        AUDIT_PART("end_time", context.EndTime)
        AUDIT_PART("cloud_id", context.CloudId)
        AUDIT_PART("folder_id", context.FolderId)
        AUDIT_PART("resource_id", context.ResourceId)
        AUDIT_PART("reason", context.Reason)
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditLogin(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address", context.RemoteAddress)
        AUDIT_PART("subject", context.Subject)
        AUDIT_PART("sanitized_token", context.SanitizedToken)
        AUDIT_PART("database", context.Database)
        AUDIT_PART("operation", context.Operation)
        AUDIT_PART("grpc_method", context.GrpcMethod)
        AUDIT_PART("start_time", context.StartTime)
        AUDIT_PART("end_time", context.EndTime)
        AUDIT_PART("cloud_id", context.CloudId)
        AUDIT_PART("folder_id", context.FolderId)
        AUDIT_PART("resource_id", context.ResourceId)
        AUDIT_PART("reason", context.Reason)
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditNodeRegistration(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address", context.RemoteAddress)
        AUDIT_PART("subject", context.Subject)
        AUDIT_PART("sanitized_token", context.SanitizedToken)
        AUDIT_PART("database", context.Database)
        AUDIT_PART("operation", context.Operation)
        AUDIT_PART("grpc_method", context.GrpcMethod)
        AUDIT_PART("start_time", context.StartTime)
        AUDIT_PART("end_time", context.EndTime)
        AUDIT_PART("cloud_id", context.CloudId)
        AUDIT_PART("folder_id", context.FolderId)
        AUDIT_PART("resource_id", context.ResourceId)
        AUDIT_PART("reason", context.Reason)
        AUDIT_PART("request", context.Request)
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditDdl(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address", context.RemoteAddress)
        AUDIT_PART("subject", context.Subject)
        AUDIT_PART("sanitized_token", context.SanitizedToken)
        AUDIT_PART("database", context.Database)
        AUDIT_PART("operation", context.Operation)
        AUDIT_PART("grpc_method", context.GrpcMethod)
        AUDIT_PART("start_time", context.StartTime)
        AUDIT_PART("end_time", context.EndTime)
        AUDIT_PART("cloud_id", context.CloudId)
        AUDIT_PART("folder_id", context.FolderId)
        AUDIT_PART("resource_id", context.ResourceId)
        AUDIT_PART("reason", context.Reason)
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditDml(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address", context.RemoteAddress)
        AUDIT_PART("subject", context.Subject)
        AUDIT_PART("sanitized_token", context.SanitizedToken)
        AUDIT_PART("database", context.Database)
        AUDIT_PART("operation", context.Operation)
        AUDIT_PART("grpc_method", context.GrpcMethod)
        AUDIT_PART("start_time", context.StartTime)
        AUDIT_PART("end_time", context.EndTime)
        AUDIT_PART("cloud_id", context.CloudId)
        AUDIT_PART("folder_id", context.FolderId)
        AUDIT_PART("resource_id", context.ResourceId)
        AUDIT_PART("reason", context.Reason)
        AUDIT_PART("query_text", context.QueryText)
        AUDIT_PART("prepared_query_id", context.PreparedQueryId)
        AUDIT_PART("tx_id", context.TxId)
        AUDIT_PART("begin_tx", context.BeginTx)
        AUDIT_PART("commit_tx", context.CommitTx)
        AUDIT_PART("table", context.Table)
        AUDIT_PART("row_count", context.RowCount)
        AUDIT_PART("request", context.Request)
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditOperations(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address", context.RemoteAddress)
        AUDIT_PART("subject", context.Subject)
        AUDIT_PART("sanitized_token", context.SanitizedToken)
        AUDIT_PART("database", context.Database)
        AUDIT_PART("operation", context.Operation)
        AUDIT_PART("grpc_method", context.GrpcMethod)
        AUDIT_PART("start_time", context.StartTime)
        AUDIT_PART("end_time", context.EndTime)
        AUDIT_PART("cloud_id", context.CloudId)
        AUDIT_PART("folder_id", context.FolderId)
        AUDIT_PART("resource_id", context.ResourceId)
        AUDIT_PART("reason", context.Reason)
        AUDIT_PART("hive_id", context.HiveId)
        AUDIT_PART("hive_generation", context.HiveGeneration)
        AUDIT_PART("reassign_id", context.ReassignId)
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditExportImport(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address", context.RemoteAddress)
        AUDIT_PART("subject", context.Subject)
        AUDIT_PART("sanitized_token", context.SanitizedToken)
        AUDIT_PART("database", context.Database)
        AUDIT_PART("operation", context.Operation)
        AUDIT_PART("grpc_method", context.GrpcMethod)
        AUDIT_PART("start_time", context.StartTime)
        AUDIT_PART("end_time", context.EndTime)
        AUDIT_PART("cloud_id", context.CloudId)
        AUDIT_PART("folder_id", context.FolderId)
        AUDIT_PART("resource_id", context.ResourceId)
        AUDIT_PART("reason", context.Reason)
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditAcl(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address", context.RemoteAddress)
        AUDIT_PART("subject", context.Subject)
        AUDIT_PART("sanitized_token", context.SanitizedToken)
        AUDIT_PART("database", context.Database)
        AUDIT_PART("operation", context.Operation)
        AUDIT_PART("grpc_method", context.GrpcMethod)
        AUDIT_PART("start_time", context.StartTime)
        AUDIT_PART("end_time", context.EndTime)
        AUDIT_PART("cloud_id", context.CloudId)
        AUDIT_PART("folder_id", context.FolderId)
        AUDIT_PART("resource_id", context.ResourceId)
        AUDIT_PART("reason", context.Reason)
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditHeartbeat(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address", context.RemoteAddress)
        AUDIT_PART("subject", context.Subject)
        AUDIT_PART("sanitized_token", context.SanitizedToken)
        AUDIT_PART("database", context.Database)
        AUDIT_PART("operation", context.Operation)
        AUDIT_PART("grpc_method", context.GrpcMethod)
        AUDIT_PART("start_time", context.StartTime)
        AUDIT_PART("end_time", context.EndTime)
        AUDIT_PART("cloud_id", context.CloudId)
        AUDIT_PART("folder_id", context.FolderId)
        AUDIT_PART("resource_id", context.ResourceId)
        AUDIT_PART("reason", context.Reason)
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditDefault(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address", context.RemoteAddress)
        AUDIT_PART("subject", context.Subject)
        AUDIT_PART("sanitized_token", context.SanitizedToken)
        AUDIT_PART("database", context.Database)
        AUDIT_PART("operation", context.Operation)
        AUDIT_PART("grpc_method", context.GrpcMethod)
        AUDIT_PART("start_time", context.StartTime)
        AUDIT_PART("end_time", context.EndTime)
        AUDIT_PART("cloud_id", context.CloudId)
        AUDIT_PART("folder_id", context.FolderId)
        AUDIT_PART("resource_id", context.ResourceId)
        AUDIT_PART("reason", context.Reason)
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

using TAuditWriter = void (*)(const TGrpcAuditContext&);

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
        const auto context = MakeConnectionContext(ctx, database, userSID, sanitizedToken);
        GetAuditWriter(ctx->GetAuditMode().LogClass)(context);
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
        const auto context = MakeOperationContext(status, parts);
        GetAuditWriter(logClass)(context);
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
            {"sanitized_token", (!sanitizedToken.empty() ? sanitizedToken : EmptyValue)},
            {"database", database},
            {"operation", ctx->GetRequestName()},
            {"reason", reason},
        });
    }
}

} // namespace NKikimr::NGRpcService
