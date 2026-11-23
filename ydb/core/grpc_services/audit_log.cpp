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

    // Keep repeated values from the append-only request audit context.
    TVector<TString> RemoteAddress;
    TVector<TString> Subject;
    TVector<TString> SanitizedToken;
    TVector<TString> Database;
    TVector<TString> Operation;
    TVector<TString> GrpcMethod;
    TVector<TString> StartTime;
    TVector<TString> EndTime;
    TVector<TString> CloudId;
    TVector<TString> FolderId;
    TVector<TString> ResourceId;
    TVector<TString> Reason;
    TVector<TString> QueryText;
    TVector<TString> PreparedQueryId;
    TVector<TString> TxId;
    TVector<TString> BeginTx;
    TVector<TString> CommitTx;
    TVector<TString> Table;
    TVector<TString> RowCount;
    TVector<TString> TabletId;
    TVector<TString> ProgramText;
    TVector<TString> SchemaChanges;
    TVector<TString> StoragePool;
    TVector<TString> Group;
    TVector<TString> HiveId;
    TVector<TString> HiveGeneration;
    TVector<TString> ReassignId;
    TVector<TString> Request;
};

TVector<TString> GetOperationField(const TAuditLogParts& parts, TStringBuf name) {
    TVector<TString> values;
    for (const auto& [partName, value] : parts) {
        if (partName == name) {
            values.push_back(value.empty() ? EmptyValue : value);
        }
    }
    return values;
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
    context.RemoteAddress.push_back(NAddressClassifier::ExtractAddress(request->GetPeerName()));
    context.Subject.push_back(userSID);
    context.SanitizedToken.push_back(sanitizedToken.empty() ? EmptyValue : sanitizedToken);
    context.Database.push_back(database);
    context.Operation.push_back(request->GetRequestName());
    return context;
}

void AuditClusterAdmin(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        for (const auto& value : context.RemoteAddress) {
            AUDIT_PART("remote_address", value)
        }
        for (const auto& value : context.Subject) {
            AUDIT_PART("subject", value)
        }
        for (const auto& value : context.SanitizedToken) {
            AUDIT_PART("sanitized_token", value)
        }
        for (const auto& value : context.Database) {
            AUDIT_PART("database", value)
        }
        for (const auto& value : context.Operation) {
            AUDIT_PART("operation", value)
        }
        for (const auto& value : context.GrpcMethod) {
            AUDIT_PART("grpc_method", value)
        }
        for (const auto& value : context.StartTime) {
            AUDIT_PART("start_time", value)
        }
        for (const auto& value : context.EndTime) {
            AUDIT_PART("end_time", value)
        }
        for (const auto& value : context.CloudId) {
            AUDIT_PART("cloud_id", value)
        }
        for (const auto& value : context.FolderId) {
            AUDIT_PART("folder_id", value)
        }
        for (const auto& value : context.ResourceId) {
            AUDIT_PART("resource_id", value)
        }
        for (const auto& value : context.Reason) {
            AUDIT_PART("reason", value)
        }
        for (const auto& value : context.TabletId) {
            AUDIT_PART("tablet_id", value)
        }
        for (const auto& value : context.ProgramText) {
            AUDIT_PART("program_text", value)
        }
        for (const auto& value : context.SchemaChanges) {
            AUDIT_PART("schema_changes", value)
        }
        for (const auto& value : context.StoragePool) {
            AUDIT_PART("storage_pool", value)
        }
        for (const auto& value : context.Group) {
            AUDIT_PART("group", value)
        }
        for (const auto& value : context.HiveId) {
            AUDIT_PART("hive_id", value)
        }
        for (const auto& value : context.Request) {
            AUDIT_PART("request", value)
        }
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditDatabaseAdmin(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        for (const auto& value : context.RemoteAddress) {
            AUDIT_PART("remote_address", value)
        }
        for (const auto& value : context.Subject) {
            AUDIT_PART("subject", value)
        }
        for (const auto& value : context.SanitizedToken) {
            AUDIT_PART("sanitized_token", value)
        }
        for (const auto& value : context.Database) {
            AUDIT_PART("database", value)
        }
        for (const auto& value : context.Operation) {
            AUDIT_PART("operation", value)
        }
        for (const auto& value : context.GrpcMethod) {
            AUDIT_PART("grpc_method", value)
        }
        for (const auto& value : context.StartTime) {
            AUDIT_PART("start_time", value)
        }
        for (const auto& value : context.EndTime) {
            AUDIT_PART("end_time", value)
        }
        for (const auto& value : context.CloudId) {
            AUDIT_PART("cloud_id", value)
        }
        for (const auto& value : context.FolderId) {
            AUDIT_PART("folder_id", value)
        }
        for (const auto& value : context.ResourceId) {
            AUDIT_PART("resource_id", value)
        }
        for (const auto& value : context.Reason) {
            AUDIT_PART("reason", value)
        }
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditLogin(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        for (const auto& value : context.RemoteAddress) {
            AUDIT_PART("remote_address", value)
        }
        for (const auto& value : context.Subject) {
            AUDIT_PART("subject", value)
        }
        for (const auto& value : context.SanitizedToken) {
            AUDIT_PART("sanitized_token", value)
        }
        for (const auto& value : context.Database) {
            AUDIT_PART("database", value)
        }
        for (const auto& value : context.Operation) {
            AUDIT_PART("operation", value)
        }
        for (const auto& value : context.GrpcMethod) {
            AUDIT_PART("grpc_method", value)
        }
        for (const auto& value : context.StartTime) {
            AUDIT_PART("start_time", value)
        }
        for (const auto& value : context.EndTime) {
            AUDIT_PART("end_time", value)
        }
        for (const auto& value : context.CloudId) {
            AUDIT_PART("cloud_id", value)
        }
        for (const auto& value : context.FolderId) {
            AUDIT_PART("folder_id", value)
        }
        for (const auto& value : context.ResourceId) {
            AUDIT_PART("resource_id", value)
        }
        for (const auto& value : context.Reason) {
            AUDIT_PART("reason", value)
        }
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditNodeRegistration(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        for (const auto& value : context.RemoteAddress) {
            AUDIT_PART("remote_address", value)
        }
        for (const auto& value : context.Subject) {
            AUDIT_PART("subject", value)
        }
        for (const auto& value : context.SanitizedToken) {
            AUDIT_PART("sanitized_token", value)
        }
        for (const auto& value : context.Database) {
            AUDIT_PART("database", value)
        }
        for (const auto& value : context.Operation) {
            AUDIT_PART("operation", value)
        }
        for (const auto& value : context.GrpcMethod) {
            AUDIT_PART("grpc_method", value)
        }
        for (const auto& value : context.StartTime) {
            AUDIT_PART("start_time", value)
        }
        for (const auto& value : context.EndTime) {
            AUDIT_PART("end_time", value)
        }
        for (const auto& value : context.CloudId) {
            AUDIT_PART("cloud_id", value)
        }
        for (const auto& value : context.FolderId) {
            AUDIT_PART("folder_id", value)
        }
        for (const auto& value : context.ResourceId) {
            AUDIT_PART("resource_id", value)
        }
        for (const auto& value : context.Reason) {
            AUDIT_PART("reason", value)
        }
        for (const auto& value : context.Request) {
            AUDIT_PART("request", value)
        }
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditDdl(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        for (const auto& value : context.RemoteAddress) {
            AUDIT_PART("remote_address", value)
        }
        for (const auto& value : context.Subject) {
            AUDIT_PART("subject", value)
        }
        for (const auto& value : context.SanitizedToken) {
            AUDIT_PART("sanitized_token", value)
        }
        for (const auto& value : context.Database) {
            AUDIT_PART("database", value)
        }
        for (const auto& value : context.Operation) {
            AUDIT_PART("operation", value)
        }
        for (const auto& value : context.GrpcMethod) {
            AUDIT_PART("grpc_method", value)
        }
        for (const auto& value : context.StartTime) {
            AUDIT_PART("start_time", value)
        }
        for (const auto& value : context.EndTime) {
            AUDIT_PART("end_time", value)
        }
        for (const auto& value : context.CloudId) {
            AUDIT_PART("cloud_id", value)
        }
        for (const auto& value : context.FolderId) {
            AUDIT_PART("folder_id", value)
        }
        for (const auto& value : context.ResourceId) {
            AUDIT_PART("resource_id", value)
        }
        for (const auto& value : context.Reason) {
            AUDIT_PART("reason", value)
        }
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditDml(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        for (const auto& value : context.RemoteAddress) {
            AUDIT_PART("remote_address", value)
        }
        for (const auto& value : context.Subject) {
            AUDIT_PART("subject", value)
        }
        for (const auto& value : context.SanitizedToken) {
            AUDIT_PART("sanitized_token", value)
        }
        for (const auto& value : context.Database) {
            AUDIT_PART("database", value)
        }
        for (const auto& value : context.Operation) {
            AUDIT_PART("operation", value)
        }
        for (const auto& value : context.GrpcMethod) {
            AUDIT_PART("grpc_method", value)
        }
        for (const auto& value : context.StartTime) {
            AUDIT_PART("start_time", value)
        }
        for (const auto& value : context.EndTime) {
            AUDIT_PART("end_time", value)
        }
        for (const auto& value : context.CloudId) {
            AUDIT_PART("cloud_id", value)
        }
        for (const auto& value : context.FolderId) {
            AUDIT_PART("folder_id", value)
        }
        for (const auto& value : context.ResourceId) {
            AUDIT_PART("resource_id", value)
        }
        for (const auto& value : context.Reason) {
            AUDIT_PART("reason", value)
        }
        for (const auto& value : context.QueryText) {
            AUDIT_PART("query_text", value)
        }
        for (const auto& value : context.PreparedQueryId) {
            AUDIT_PART("prepared_query_id", value)
        }
        for (const auto& value : context.TxId) {
            AUDIT_PART("tx_id", value)
        }
        for (const auto& value : context.BeginTx) {
            AUDIT_PART("begin_tx", value)
        }
        for (const auto& value : context.CommitTx) {
            AUDIT_PART("commit_tx", value)
        }
        for (const auto& value : context.Table) {
            AUDIT_PART("table", value)
        }
        for (const auto& value : context.RowCount) {
            AUDIT_PART("row_count", value)
        }
        for (const auto& value : context.Request) {
            AUDIT_PART("request", value)
        }
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditOperations(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        for (const auto& value : context.RemoteAddress) {
            AUDIT_PART("remote_address", value)
        }
        for (const auto& value : context.Subject) {
            AUDIT_PART("subject", value)
        }
        for (const auto& value : context.SanitizedToken) {
            AUDIT_PART("sanitized_token", value)
        }
        for (const auto& value : context.Database) {
            AUDIT_PART("database", value)
        }
        for (const auto& value : context.Operation) {
            AUDIT_PART("operation", value)
        }
        for (const auto& value : context.GrpcMethod) {
            AUDIT_PART("grpc_method", value)
        }
        for (const auto& value : context.StartTime) {
            AUDIT_PART("start_time", value)
        }
        for (const auto& value : context.EndTime) {
            AUDIT_PART("end_time", value)
        }
        for (const auto& value : context.CloudId) {
            AUDIT_PART("cloud_id", value)
        }
        for (const auto& value : context.FolderId) {
            AUDIT_PART("folder_id", value)
        }
        for (const auto& value : context.ResourceId) {
            AUDIT_PART("resource_id", value)
        }
        for (const auto& value : context.Reason) {
            AUDIT_PART("reason", value)
        }
        for (const auto& value : context.HiveId) {
            AUDIT_PART("hive_id", value)
        }
        for (const auto& value : context.HiveGeneration) {
            AUDIT_PART("hive_generation", value)
        }
        for (const auto& value : context.ReassignId) {
            AUDIT_PART("reassign_id", value)
        }
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditExportImport(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        for (const auto& value : context.RemoteAddress) {
            AUDIT_PART("remote_address", value)
        }
        for (const auto& value : context.Subject) {
            AUDIT_PART("subject", value)
        }
        for (const auto& value : context.SanitizedToken) {
            AUDIT_PART("sanitized_token", value)
        }
        for (const auto& value : context.Database) {
            AUDIT_PART("database", value)
        }
        for (const auto& value : context.Operation) {
            AUDIT_PART("operation", value)
        }
        for (const auto& value : context.GrpcMethod) {
            AUDIT_PART("grpc_method", value)
        }
        for (const auto& value : context.StartTime) {
            AUDIT_PART("start_time", value)
        }
        for (const auto& value : context.EndTime) {
            AUDIT_PART("end_time", value)
        }
        for (const auto& value : context.CloudId) {
            AUDIT_PART("cloud_id", value)
        }
        for (const auto& value : context.FolderId) {
            AUDIT_PART("folder_id", value)
        }
        for (const auto& value : context.ResourceId) {
            AUDIT_PART("resource_id", value)
        }
        for (const auto& value : context.Reason) {
            AUDIT_PART("reason", value)
        }
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditAcl(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        for (const auto& value : context.RemoteAddress) {
            AUDIT_PART("remote_address", value)
        }
        for (const auto& value : context.Subject) {
            AUDIT_PART("subject", value)
        }
        for (const auto& value : context.SanitizedToken) {
            AUDIT_PART("sanitized_token", value)
        }
        for (const auto& value : context.Database) {
            AUDIT_PART("database", value)
        }
        for (const auto& value : context.Operation) {
            AUDIT_PART("operation", value)
        }
        for (const auto& value : context.GrpcMethod) {
            AUDIT_PART("grpc_method", value)
        }
        for (const auto& value : context.StartTime) {
            AUDIT_PART("start_time", value)
        }
        for (const auto& value : context.EndTime) {
            AUDIT_PART("end_time", value)
        }
        for (const auto& value : context.CloudId) {
            AUDIT_PART("cloud_id", value)
        }
        for (const auto& value : context.FolderId) {
            AUDIT_PART("folder_id", value)
        }
        for (const auto& value : context.ResourceId) {
            AUDIT_PART("resource_id", value)
        }
        for (const auto& value : context.Reason) {
            AUDIT_PART("reason", value)
        }
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditHeartbeat(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        for (const auto& value : context.RemoteAddress) {
            AUDIT_PART("remote_address", value)
        }
        for (const auto& value : context.Subject) {
            AUDIT_PART("subject", value)
        }
        for (const auto& value : context.SanitizedToken) {
            AUDIT_PART("sanitized_token", value)
        }
        for (const auto& value : context.Database) {
            AUDIT_PART("database", value)
        }
        for (const auto& value : context.Operation) {
            AUDIT_PART("operation", value)
        }
        for (const auto& value : context.GrpcMethod) {
            AUDIT_PART("grpc_method", value)
        }
        for (const auto& value : context.StartTime) {
            AUDIT_PART("start_time", value)
        }
        for (const auto& value : context.EndTime) {
            AUDIT_PART("end_time", value)
        }
        for (const auto& value : context.CloudId) {
            AUDIT_PART("cloud_id", value)
        }
        for (const auto& value : context.FolderId) {
            AUDIT_PART("folder_id", value)
        }
        for (const auto& value : context.ResourceId) {
            AUDIT_PART("resource_id", value)
        }
        for (const auto& value : context.Reason) {
            AUDIT_PART("reason", value)
        }
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditDefault(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        for (const auto& value : context.RemoteAddress) {
            AUDIT_PART("remote_address", value)
        }
        for (const auto& value : context.Subject) {
            AUDIT_PART("subject", value)
        }
        for (const auto& value : context.SanitizedToken) {
            AUDIT_PART("sanitized_token", value)
        }
        for (const auto& value : context.Database) {
            AUDIT_PART("database", value)
        }
        for (const auto& value : context.Operation) {
            AUDIT_PART("operation", value)
        }
        for (const auto& value : context.GrpcMethod) {
            AUDIT_PART("grpc_method", value)
        }
        for (const auto& value : context.StartTime) {
            AUDIT_PART("start_time", value)
        }
        for (const auto& value : context.EndTime) {
            AUDIT_PART("end_time", value)
        }
        for (const auto& value : context.CloudId) {
            AUDIT_PART("cloud_id", value)
        }
        for (const auto& value : context.FolderId) {
            AUDIT_PART("folder_id", value)
        }
        for (const auto& value : context.ResourceId) {
            AUDIT_PART("resource_id", value)
        }
        for (const auto& value : context.Reason) {
            AUDIT_PART("reason", value)
        }
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
