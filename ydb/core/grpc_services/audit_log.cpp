#include "defs.h"

#include <ydb/core/util/address_classifier.h>
#include <ydb/core/audit/audit_log.h>

#include "base/base.h"
#include "audit_log.h"

#include <util/generic/hash.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::GRPC_SERVER

namespace NKikimr::NGRpcService {

namespace {

using TLogClassConfig = TAuditMode::TLogClassConfig;

struct TGrpcAuditContext {
    bool IsConnection = false;
    TString Component;
    TString Status;
    TString DetailedStatus;

    // nullopt means the key was absent. Keep supplied empty strings unchanged;
    // each AUDIT_PART below specifies its own missing-value policy.
    std::optional<TString> RemoteAddress;
    std::optional<TString> Subject;
    std::optional<TString> SanitizedToken;
    std::optional<TString> Database;
    std::optional<TString> Operation;
    std::optional<TString> GrpcMethod;
    std::optional<TString> StartTime;
    std::optional<TString> EndTime;
    std::optional<TString> CloudId;
    std::optional<TString> FolderId;
    std::optional<TString> ResourceId;
    std::optional<TString> Reason;
    std::optional<TString> QueryText;
    std::optional<TString> PreparedQueryId;
    std::optional<TString> TxId;
    std::optional<TString> BeginTx;
    std::optional<TString> CommitTx;
    std::optional<TString> Table;
    std::optional<TString> RowCount;
    std::optional<TString> TabletId;
    std::optional<TString> ProgramText;
    std::optional<TString> SchemaChanges;
    std::optional<TString> StoragePool;
    std::optional<TString> Group;
    std::optional<TString> HiveId;
    std::optional<TString> HiveGeneration;
    std::optional<TString> ReassignId;
    std::optional<TString> Request;
};

using TAuditPartsByName = THashMap<TStringBuf, const TString*>;

std::optional<TString> GetOperationField(const TAuditPartsByName& parts, TStringBuf name) {
    if (const auto it = parts.find(name); it != parts.end()) {
        return *it->second;
    }
    return std::nullopt;
}

TGrpcAuditContext MakeOperationContext(std::optional<ui32> status, const TAuditLogParts& parts) {
    TAuditPartsByName partsByName;
    partsByName.reserve(parts.size());
    for (const auto& [name, value] : parts) {
        partsByName.emplace(TStringBuf(name), &value);
    }

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
    context.RemoteAddress = GetOperationField(partsByName, "remote_address");
    context.Subject = GetOperationField(partsByName, "subject");
    context.SanitizedToken = GetOperationField(partsByName, "sanitized_token");
    context.Database = GetOperationField(partsByName, "database");
    context.Operation = GetOperationField(partsByName, "operation");
    context.GrpcMethod = GetOperationField(partsByName, "grpc_method");
    context.StartTime = GetOperationField(partsByName, "start_time");
    context.EndTime = GetOperationField(partsByName, "end_time");
    context.CloudId = GetOperationField(partsByName, "cloud_id");
    context.FolderId = GetOperationField(partsByName, "folder_id");
    context.ResourceId = GetOperationField(partsByName, "resource_id");
    context.Reason = GetOperationField(partsByName, "reason");
    context.QueryText = GetOperationField(partsByName, "query_text");
    context.PreparedQueryId = GetOperationField(partsByName, "prepared_query_id");
    context.TxId = GetOperationField(partsByName, "tx_id");
    context.BeginTx = GetOperationField(partsByName, "begin_tx");
    context.CommitTx = GetOperationField(partsByName, "commit_tx");
    context.Table = GetOperationField(partsByName, "table");
    context.RowCount = GetOperationField(partsByName, "row_count");
    context.TabletId = GetOperationField(partsByName, "tablet_id");
    context.ProgramText = GetOperationField(partsByName, "program_text");
    context.SchemaChanges = GetOperationField(partsByName, "schema_changes");
    context.StoragePool = GetOperationField(partsByName, "storage_pool");
    context.Group = GetOperationField(partsByName, "group");
    context.HiveId = GetOperationField(partsByName, "hive_id");
    context.HiveGeneration = GetOperationField(partsByName, "hive_generation");
    context.ReassignId = GetOperationField(partsByName, "reassign_id");
    context.Request = GetOperationField(partsByName, "request");
    return context;
}

TGrpcAuditContext MakeConnectionContext(const IRequestProxyCtx* request,
        const TString& database, const TString& userSID, const TString& sanitizedToken) {
    TGrpcAuditContext context;
    context.IsConnection = true;
    context.Component = "grpc-conn";
    context.RemoteAddress = NAddressClassifier::ExtractAddress(request->GetPeerName());
    context.Subject = userSID;
    context.SanitizedToken = sanitizedToken;
    context.Database = database;
    context.Operation = request->GetRequestName();
    return context;
}

void AuditClusterAdmin(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address",
            (!context.IsConnection && context.RemoteAddress->empty() ? "{none}" : *context.RemoteAddress),
            context.RemoteAddress.has_value())
        AUDIT_PART("subject",
            (!context.IsConnection && context.Subject->empty() ? "{none}" : *context.Subject),
            context.Subject.has_value())
        AUDIT_PART("sanitized_token",
            (context.SanitizedToken->empty() ? "{none}" : *context.SanitizedToken),
            context.SanitizedToken.has_value())
        AUDIT_PART("database",
            (!context.IsConnection && context.Database->empty() ? "{none}" : *context.Database),
            context.Database.has_value())
        AUDIT_PART("operation",
            (!context.IsConnection && context.Operation->empty() ? "{none}" : *context.Operation),
            context.Operation.has_value())
        AUDIT_PART("grpc_method",
            (context.GrpcMethod->empty() ? "{none}" : *context.GrpcMethod),
            context.GrpcMethod.has_value())
        AUDIT_PART("start_time",
            (context.StartTime->empty() ? "{none}" : *context.StartTime),
            context.StartTime.has_value())
        AUDIT_PART("end_time",
            (context.EndTime->empty() ? "{none}" : *context.EndTime),
            context.EndTime.has_value())
        AUDIT_PART("cloud_id",
            (context.CloudId->empty() ? "{none}" : *context.CloudId),
            context.CloudId.has_value())
        AUDIT_PART("folder_id",
            (context.FolderId->empty() ? "{none}" : *context.FolderId),
            context.FolderId.has_value())
        AUDIT_PART("resource_id",
            (context.ResourceId->empty() ? "{none}" : *context.ResourceId),
            context.ResourceId.has_value())
        AUDIT_PART("reason",
            (context.Reason->empty() ? "{none}" : *context.Reason),
            context.Reason.has_value())
        AUDIT_PART("tablet_id",
            (context.TabletId->empty() ? "{none}" : *context.TabletId),
            context.TabletId.has_value())
        AUDIT_PART("program_text",
            (context.ProgramText->empty() ? "{none}" : *context.ProgramText),
            context.ProgramText.has_value())
        AUDIT_PART("schema_changes",
            (context.SchemaChanges->empty() ? "{none}" : *context.SchemaChanges),
            context.SchemaChanges.has_value())
        AUDIT_PART("storage_pool",
            (context.StoragePool->empty() ? "{none}" : *context.StoragePool),
            context.StoragePool.has_value())
        AUDIT_PART("group",
            (context.Group->empty() ? "{none}" : *context.Group),
            context.Group.has_value())
        AUDIT_PART("hive_id",
            (context.HiveId->empty() ? "{none}" : *context.HiveId),
            context.HiveId.has_value())
        AUDIT_PART("request",
            (context.Request->empty() ? "{none}" : *context.Request),
            context.Request.has_value())
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditDatabaseAdmin(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address",
            (!context.IsConnection && context.RemoteAddress->empty() ? "{none}" : *context.RemoteAddress),
            context.RemoteAddress.has_value())
        AUDIT_PART("subject",
            (!context.IsConnection && context.Subject->empty() ? "{none}" : *context.Subject),
            context.Subject.has_value())
        AUDIT_PART("sanitized_token",
            (context.SanitizedToken->empty() ? "{none}" : *context.SanitizedToken),
            context.SanitizedToken.has_value())
        AUDIT_PART("database",
            (!context.IsConnection && context.Database->empty() ? "{none}" : *context.Database),
            context.Database.has_value())
        AUDIT_PART("operation",
            (!context.IsConnection && context.Operation->empty() ? "{none}" : *context.Operation),
            context.Operation.has_value())
        AUDIT_PART("grpc_method",
            (context.GrpcMethod->empty() ? "{none}" : *context.GrpcMethod),
            context.GrpcMethod.has_value())
        AUDIT_PART("start_time",
            (context.StartTime->empty() ? "{none}" : *context.StartTime),
            context.StartTime.has_value())
        AUDIT_PART("end_time",
            (context.EndTime->empty() ? "{none}" : *context.EndTime),
            context.EndTime.has_value())
        AUDIT_PART("cloud_id",
            (context.CloudId->empty() ? "{none}" : *context.CloudId),
            context.CloudId.has_value())
        AUDIT_PART("folder_id",
            (context.FolderId->empty() ? "{none}" : *context.FolderId),
            context.FolderId.has_value())
        AUDIT_PART("resource_id",
            (context.ResourceId->empty() ? "{none}" : *context.ResourceId),
            context.ResourceId.has_value())
        AUDIT_PART("reason",
            (context.Reason->empty() ? "{none}" : *context.Reason),
            context.Reason.has_value())
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditLogin(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address",
            (!context.IsConnection && context.RemoteAddress->empty() ? "{none}" : *context.RemoteAddress),
            context.RemoteAddress.has_value())
        AUDIT_PART("subject",
            (!context.IsConnection && context.Subject->empty() ? "{none}" : *context.Subject),
            context.Subject.has_value())
        AUDIT_PART("sanitized_token",
            (context.SanitizedToken->empty() ? "{none}" : *context.SanitizedToken),
            context.SanitizedToken.has_value())
        AUDIT_PART("database",
            (!context.IsConnection && context.Database->empty() ? "{none}" : *context.Database),
            context.Database.has_value())
        AUDIT_PART("operation",
            (!context.IsConnection && context.Operation->empty() ? "{none}" : *context.Operation),
            context.Operation.has_value())
        AUDIT_PART("grpc_method",
            (context.GrpcMethod->empty() ? "{none}" : *context.GrpcMethod),
            context.GrpcMethod.has_value())
        AUDIT_PART("start_time",
            (context.StartTime->empty() ? "{none}" : *context.StartTime),
            context.StartTime.has_value())
        AUDIT_PART("end_time",
            (context.EndTime->empty() ? "{none}" : *context.EndTime),
            context.EndTime.has_value())
        AUDIT_PART("cloud_id",
            (context.CloudId->empty() ? "{none}" : *context.CloudId),
            context.CloudId.has_value())
        AUDIT_PART("folder_id",
            (context.FolderId->empty() ? "{none}" : *context.FolderId),
            context.FolderId.has_value())
        AUDIT_PART("resource_id",
            (context.ResourceId->empty() ? "{none}" : *context.ResourceId),
            context.ResourceId.has_value())
        AUDIT_PART("reason",
            (context.Reason->empty() ? "{none}" : *context.Reason),
            context.Reason.has_value())
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditNodeRegistration(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address",
            (!context.IsConnection && context.RemoteAddress->empty() ? "{none}" : *context.RemoteAddress),
            context.RemoteAddress.has_value())
        AUDIT_PART("subject",
            (!context.IsConnection && context.Subject->empty() ? "{none}" : *context.Subject),
            context.Subject.has_value())
        AUDIT_PART("sanitized_token",
            (context.SanitizedToken->empty() ? "{none}" : *context.SanitizedToken),
            context.SanitizedToken.has_value())
        AUDIT_PART("database",
            (!context.IsConnection && context.Database->empty() ? "{none}" : *context.Database),
            context.Database.has_value())
        AUDIT_PART("operation",
            (!context.IsConnection && context.Operation->empty() ? "{none}" : *context.Operation),
            context.Operation.has_value())
        AUDIT_PART("grpc_method",
            (context.GrpcMethod->empty() ? "{none}" : *context.GrpcMethod),
            context.GrpcMethod.has_value())
        AUDIT_PART("start_time",
            (context.StartTime->empty() ? "{none}" : *context.StartTime),
            context.StartTime.has_value())
        AUDIT_PART("end_time",
            (context.EndTime->empty() ? "{none}" : *context.EndTime),
            context.EndTime.has_value())
        AUDIT_PART("cloud_id",
            (context.CloudId->empty() ? "{none}" : *context.CloudId),
            context.CloudId.has_value())
        AUDIT_PART("folder_id",
            (context.FolderId->empty() ? "{none}" : *context.FolderId),
            context.FolderId.has_value())
        AUDIT_PART("resource_id",
            (context.ResourceId->empty() ? "{none}" : *context.ResourceId),
            context.ResourceId.has_value())
        AUDIT_PART("reason",
            (context.Reason->empty() ? "{none}" : *context.Reason),
            context.Reason.has_value())
        AUDIT_PART("request",
            (context.Request->empty() ? "{none}" : *context.Request),
            context.Request.has_value())
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditDdl(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address",
            (!context.IsConnection && context.RemoteAddress->empty() ? "{none}" : *context.RemoteAddress),
            context.RemoteAddress.has_value())
        AUDIT_PART("subject",
            (!context.IsConnection && context.Subject->empty() ? "{none}" : *context.Subject),
            context.Subject.has_value())
        AUDIT_PART("sanitized_token",
            (context.SanitizedToken->empty() ? "{none}" : *context.SanitizedToken),
            context.SanitizedToken.has_value())
        AUDIT_PART("database",
            (!context.IsConnection && context.Database->empty() ? "{none}" : *context.Database),
            context.Database.has_value())
        AUDIT_PART("operation",
            (!context.IsConnection && context.Operation->empty() ? "{none}" : *context.Operation),
            context.Operation.has_value())
        AUDIT_PART("grpc_method",
            (context.GrpcMethod->empty() ? "{none}" : *context.GrpcMethod),
            context.GrpcMethod.has_value())
        AUDIT_PART("start_time",
            (context.StartTime->empty() ? "{none}" : *context.StartTime),
            context.StartTime.has_value())
        AUDIT_PART("end_time",
            (context.EndTime->empty() ? "{none}" : *context.EndTime),
            context.EndTime.has_value())
        AUDIT_PART("cloud_id",
            (context.CloudId->empty() ? "{none}" : *context.CloudId),
            context.CloudId.has_value())
        AUDIT_PART("folder_id",
            (context.FolderId->empty() ? "{none}" : *context.FolderId),
            context.FolderId.has_value())
        AUDIT_PART("resource_id",
            (context.ResourceId->empty() ? "{none}" : *context.ResourceId),
            context.ResourceId.has_value())
        AUDIT_PART("reason",
            (context.Reason->empty() ? "{none}" : *context.Reason),
            context.Reason.has_value())
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditDml(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address",
            (!context.IsConnection && context.RemoteAddress->empty() ? "{none}" : *context.RemoteAddress),
            context.RemoteAddress.has_value())
        AUDIT_PART("subject",
            (!context.IsConnection && context.Subject->empty() ? "{none}" : *context.Subject),
            context.Subject.has_value())
        AUDIT_PART("sanitized_token",
            (context.SanitizedToken->empty() ? "{none}" : *context.SanitizedToken),
            context.SanitizedToken.has_value())
        AUDIT_PART("database",
            (!context.IsConnection && context.Database->empty() ? "{none}" : *context.Database),
            context.Database.has_value())
        AUDIT_PART("operation",
            (!context.IsConnection && context.Operation->empty() ? "{none}" : *context.Operation),
            context.Operation.has_value())
        AUDIT_PART("grpc_method",
            (context.GrpcMethod->empty() ? "{none}" : *context.GrpcMethod),
            context.GrpcMethod.has_value())
        AUDIT_PART("start_time",
            (context.StartTime->empty() ? "{none}" : *context.StartTime),
            context.StartTime.has_value())
        AUDIT_PART("end_time",
            (context.EndTime->empty() ? "{none}" : *context.EndTime),
            context.EndTime.has_value())
        AUDIT_PART("cloud_id",
            (context.CloudId->empty() ? "{none}" : *context.CloudId),
            context.CloudId.has_value())
        AUDIT_PART("folder_id",
            (context.FolderId->empty() ? "{none}" : *context.FolderId),
            context.FolderId.has_value())
        AUDIT_PART("resource_id",
            (context.ResourceId->empty() ? "{none}" : *context.ResourceId),
            context.ResourceId.has_value())
        AUDIT_PART("reason",
            (context.Reason->empty() ? "{none}" : *context.Reason),
            context.Reason.has_value())
        AUDIT_PART("query_text",
            (context.QueryText->empty() ? "{none}" : *context.QueryText),
            context.QueryText.has_value())
        AUDIT_PART("prepared_query_id",
            (context.PreparedQueryId->empty() ? "{none}" : *context.PreparedQueryId),
            context.PreparedQueryId.has_value())
        AUDIT_PART("tx_id",
            (context.TxId->empty() ? "{none}" : *context.TxId),
            context.TxId.has_value())
        AUDIT_PART("begin_tx",
            (context.BeginTx->empty() ? "{none}" : *context.BeginTx),
            context.BeginTx.has_value())
        AUDIT_PART("commit_tx",
            (context.CommitTx->empty() ? "{none}" : *context.CommitTx),
            context.CommitTx.has_value())
        AUDIT_PART("table",
            (context.Table->empty() ? "{none}" : *context.Table),
            context.Table.has_value())
        AUDIT_PART("row_count",
            (context.RowCount->empty() ? "{none}" : *context.RowCount),
            context.RowCount.has_value())
        AUDIT_PART("request",
            (context.Request->empty() ? "{none}" : *context.Request),
            context.Request.has_value())
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditOperations(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address",
            (!context.IsConnection && context.RemoteAddress->empty() ? "{none}" : *context.RemoteAddress),
            context.RemoteAddress.has_value())
        AUDIT_PART("subject",
            (!context.IsConnection && context.Subject->empty() ? "{none}" : *context.Subject),
            context.Subject.has_value())
        AUDIT_PART("sanitized_token",
            (context.SanitizedToken->empty() ? "{none}" : *context.SanitizedToken),
            context.SanitizedToken.has_value())
        AUDIT_PART("database",
            (!context.IsConnection && context.Database->empty() ? "{none}" : *context.Database),
            context.Database.has_value())
        AUDIT_PART("operation",
            (!context.IsConnection && context.Operation->empty() ? "{none}" : *context.Operation),
            context.Operation.has_value())
        AUDIT_PART("grpc_method",
            (context.GrpcMethod->empty() ? "{none}" : *context.GrpcMethod),
            context.GrpcMethod.has_value())
        AUDIT_PART("start_time",
            (context.StartTime->empty() ? "{none}" : *context.StartTime),
            context.StartTime.has_value())
        AUDIT_PART("end_time",
            (context.EndTime->empty() ? "{none}" : *context.EndTime),
            context.EndTime.has_value())
        AUDIT_PART("cloud_id",
            (context.CloudId->empty() ? "{none}" : *context.CloudId),
            context.CloudId.has_value())
        AUDIT_PART("folder_id",
            (context.FolderId->empty() ? "{none}" : *context.FolderId),
            context.FolderId.has_value())
        AUDIT_PART("resource_id",
            (context.ResourceId->empty() ? "{none}" : *context.ResourceId),
            context.ResourceId.has_value())
        AUDIT_PART("reason",
            (context.Reason->empty() ? "{none}" : *context.Reason),
            context.Reason.has_value())
        AUDIT_PART("hive_id",
            (context.HiveId->empty() ? "{none}" : *context.HiveId),
            context.HiveId.has_value())
        AUDIT_PART("hive_generation",
            (context.HiveGeneration->empty() ? "{none}" : *context.HiveGeneration),
            context.HiveGeneration.has_value())
        AUDIT_PART("reassign_id",
            (context.ReassignId->empty() ? "{none}" : *context.ReassignId),
            context.ReassignId.has_value())
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditExportImport(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address",
            (!context.IsConnection && context.RemoteAddress->empty() ? "{none}" : *context.RemoteAddress),
            context.RemoteAddress.has_value())
        AUDIT_PART("subject",
            (!context.IsConnection && context.Subject->empty() ? "{none}" : *context.Subject),
            context.Subject.has_value())
        AUDIT_PART("sanitized_token",
            (context.SanitizedToken->empty() ? "{none}" : *context.SanitizedToken),
            context.SanitizedToken.has_value())
        AUDIT_PART("database",
            (!context.IsConnection && context.Database->empty() ? "{none}" : *context.Database),
            context.Database.has_value())
        AUDIT_PART("operation",
            (!context.IsConnection && context.Operation->empty() ? "{none}" : *context.Operation),
            context.Operation.has_value())
        AUDIT_PART("grpc_method",
            (context.GrpcMethod->empty() ? "{none}" : *context.GrpcMethod),
            context.GrpcMethod.has_value())
        AUDIT_PART("start_time",
            (context.StartTime->empty() ? "{none}" : *context.StartTime),
            context.StartTime.has_value())
        AUDIT_PART("end_time",
            (context.EndTime->empty() ? "{none}" : *context.EndTime),
            context.EndTime.has_value())
        AUDIT_PART("cloud_id",
            (context.CloudId->empty() ? "{none}" : *context.CloudId),
            context.CloudId.has_value())
        AUDIT_PART("folder_id",
            (context.FolderId->empty() ? "{none}" : *context.FolderId),
            context.FolderId.has_value())
        AUDIT_PART("resource_id",
            (context.ResourceId->empty() ? "{none}" : *context.ResourceId),
            context.ResourceId.has_value())
        AUDIT_PART("reason",
            (context.Reason->empty() ? "{none}" : *context.Reason),
            context.Reason.has_value())
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditAcl(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address",
            (!context.IsConnection && context.RemoteAddress->empty() ? "{none}" : *context.RemoteAddress),
            context.RemoteAddress.has_value())
        AUDIT_PART("subject",
            (!context.IsConnection && context.Subject->empty() ? "{none}" : *context.Subject),
            context.Subject.has_value())
        AUDIT_PART("sanitized_token",
            (context.SanitizedToken->empty() ? "{none}" : *context.SanitizedToken),
            context.SanitizedToken.has_value())
        AUDIT_PART("database",
            (!context.IsConnection && context.Database->empty() ? "{none}" : *context.Database),
            context.Database.has_value())
        AUDIT_PART("operation",
            (!context.IsConnection && context.Operation->empty() ? "{none}" : *context.Operation),
            context.Operation.has_value())
        AUDIT_PART("grpc_method",
            (context.GrpcMethod->empty() ? "{none}" : *context.GrpcMethod),
            context.GrpcMethod.has_value())
        AUDIT_PART("start_time",
            (context.StartTime->empty() ? "{none}" : *context.StartTime),
            context.StartTime.has_value())
        AUDIT_PART("end_time",
            (context.EndTime->empty() ? "{none}" : *context.EndTime),
            context.EndTime.has_value())
        AUDIT_PART("cloud_id",
            (context.CloudId->empty() ? "{none}" : *context.CloudId),
            context.CloudId.has_value())
        AUDIT_PART("folder_id",
            (context.FolderId->empty() ? "{none}" : *context.FolderId),
            context.FolderId.has_value())
        AUDIT_PART("resource_id",
            (context.ResourceId->empty() ? "{none}" : *context.ResourceId),
            context.ResourceId.has_value())
        AUDIT_PART("reason",
            (context.Reason->empty() ? "{none}" : *context.Reason),
            context.Reason.has_value())
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditHeartbeat(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address",
            (!context.IsConnection && context.RemoteAddress->empty() ? "{none}" : *context.RemoteAddress),
            context.RemoteAddress.has_value())
        AUDIT_PART("subject",
            (!context.IsConnection && context.Subject->empty() ? "{none}" : *context.Subject),
            context.Subject.has_value())
        AUDIT_PART("sanitized_token",
            (context.SanitizedToken->empty() ? "{none}" : *context.SanitizedToken),
            context.SanitizedToken.has_value())
        AUDIT_PART("database",
            (!context.IsConnection && context.Database->empty() ? "{none}" : *context.Database),
            context.Database.has_value())
        AUDIT_PART("operation",
            (!context.IsConnection && context.Operation->empty() ? "{none}" : *context.Operation),
            context.Operation.has_value())
        AUDIT_PART("grpc_method",
            (context.GrpcMethod->empty() ? "{none}" : *context.GrpcMethod),
            context.GrpcMethod.has_value())
        AUDIT_PART("start_time",
            (context.StartTime->empty() ? "{none}" : *context.StartTime),
            context.StartTime.has_value())
        AUDIT_PART("end_time",
            (context.EndTime->empty() ? "{none}" : *context.EndTime),
            context.EndTime.has_value())
        AUDIT_PART("cloud_id",
            (context.CloudId->empty() ? "{none}" : *context.CloudId),
            context.CloudId.has_value())
        AUDIT_PART("folder_id",
            (context.FolderId->empty() ? "{none}" : *context.FolderId),
            context.FolderId.has_value())
        AUDIT_PART("resource_id",
            (context.ResourceId->empty() ? "{none}" : *context.ResourceId),
            context.ResourceId.has_value())
        AUDIT_PART("reason",
            (context.Reason->empty() ? "{none}" : *context.Reason),
            context.Reason.has_value())
        AUDIT_PART("status", context.Status)
        AUDIT_PART("detailed_status", context.DetailedStatus)
    );
}

void AuditDefault(const TGrpcAuditContext& context) {
    AUDIT_LOG(
        AUDIT_PART("component", context.Component)
        AUDIT_PART("remote_address",
            (!context.IsConnection && context.RemoteAddress->empty() ? "{none}" : *context.RemoteAddress),
            context.RemoteAddress.has_value())
        AUDIT_PART("subject",
            (!context.IsConnection && context.Subject->empty() ? "{none}" : *context.Subject),
            context.Subject.has_value())
        AUDIT_PART("sanitized_token",
            (context.SanitizedToken->empty() ? "{none}" : *context.SanitizedToken),
            context.SanitizedToken.has_value())
        AUDIT_PART("database",
            (!context.IsConnection && context.Database->empty() ? "{none}" : *context.Database),
            context.Database.has_value())
        AUDIT_PART("operation",
            (!context.IsConnection && context.Operation->empty() ? "{none}" : *context.Operation),
            context.Operation.has_value())
        AUDIT_PART("grpc_method",
            (context.GrpcMethod->empty() ? "{none}" : *context.GrpcMethod),
            context.GrpcMethod.has_value())
        AUDIT_PART("start_time",
            (context.StartTime->empty() ? "{none}" : *context.StartTime),
            context.StartTime.has_value())
        AUDIT_PART("end_time",
            (context.EndTime->empty() ? "{none}" : *context.EndTime),
            context.EndTime.has_value())
        AUDIT_PART("cloud_id",
            (context.CloudId->empty() ? "{none}" : *context.CloudId),
            context.CloudId.has_value())
        AUDIT_PART("folder_id",
            (context.FolderId->empty() ? "{none}" : *context.FolderId),
            context.FolderId.has_value())
        AUDIT_PART("resource_id",
            (context.ResourceId->empty() ? "{none}" : *context.ResourceId),
            context.ResourceId.has_value())
        AUDIT_PART("reason",
            (context.Reason->empty() ? "{none}" : *context.Reason),
            context.Reason.has_value())
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
            {"sanitized_token", sanitizedToken},
            {"database", database},
            {"operation", ctx->GetRequestName()},
            {"reason", reason},
        });
    }
}

} // namespace NKikimr::NGRpcService
