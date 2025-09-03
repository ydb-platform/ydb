#include <util/string/vector.h>

#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/api/protos/ydb_import.pb.h>

#include <ydb/library/actors/http/http.h>

#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/export.pb.h>
#include <ydb/core/protos/import.pb.h>

#include <ydb/core/util/address_classifier.h>
#include <ydb/core/audit/audit_log.h>

#include "schemeshard_path.h"
#include "schemeshard_impl.h"
#include "schemeshard_xxport__helpers.h"
#include "schemeshard_audit_log_fragment.h"
#include "schemeshard_audit_log.h"

namespace NKikimr::NSchemeShard {

namespace {

const TString SchemeshardComponentName = "schemeshard";

//NOTE: EmptyValue couldn't be an empty string as AUDIT_PART() skips parts with an empty values
const TString EmptyValue = "{none}";

TString GeneralStatus(NKikimrScheme::EStatus actualStatus) {
    switch(actualStatus) {
        case NKikimrScheme::EStatus::StatusAlreadyExists:
        case NKikimrScheme::EStatus::StatusSuccess:
            return "SUCCESS";
        case NKikimrScheme::EStatus::StatusAccepted:
        //TODO: reclassify StatusAccepted as IN-PROCCESS when
        // logging of operation completion points will be added
        //     return "IN-PROCCESS";
            return "SUCCESS";
        default:
            return "ERROR";
    }
}

TString RenderList(const TVector<TString>& list) {
    auto result = TStringBuilder();
    result << "[" << JoinStrings(list.begin(), list.end(), ", ") << "]";
    return result;
}

std::tuple<TString, TString, TString> GetDatabaseCloudIds(const TPath &databasePath) {
    if (databasePath.IsEmpty()) {
        return {};
    }
    Y_ABORT_UNLESS(databasePath->IsDomainRoot());
    auto getAttr = [&databasePath](const TString &name) -> TString {
        if (databasePath.Base()->UserAttrs->Attrs.contains(name)) {
            return databasePath.Base()->UserAttrs->Attrs.at(name);
        }
        return {};
    };
    return std::make_tuple(
        getAttr("cloud_id"),
        getAttr("folder_id"),
        getAttr("database_id")
    );
}

TPath DatabasePathFromWorkingDir(TSchemeShard* SS, const TString &opWorkingDir) {
    auto databasePath = TPath::Resolve(opWorkingDir, SS);
    if (!databasePath.IsResolved()) {
        databasePath.RiseUntilFirstResolvedParent();
    }
    //NOTE: operation working dir is usually set to a path of some database/subdomain,
    // so the next lines is only a safety measure
    if (!databasePath.IsEmpty() && !databasePath->IsDomainRoot()) {
        databasePath = TPath::Init(databasePath.GetPathIdForDomain(), SS);
    }
    return databasePath;
}

TPath DatabasePathFromModifySchemeOperation(TSchemeShard* SS, const NKikimrSchemeOp::TModifyScheme& operation) {
    if (operation.GetWorkingDir().empty()) {
        // Moving operations does not have working directory. It is valid to take src or dst as directory for database
        if (operation.HasMoveTable()) {
            return DatabasePathFromWorkingDir(SS, operation.GetMoveTable().GetSrcPath());
        }
        if (operation.HasMoveIndex()) {
            return DatabasePathFromWorkingDir(SS, operation.GetMoveIndex().GetTablePath());
        }
        if (operation.HasMoveTableIndex()) {
            return DatabasePathFromWorkingDir(SS, operation.GetMoveTableIndex().GetSrcPath());
        }
    }

    return DatabasePathFromWorkingDir(SS, operation.GetWorkingDir());
}

}  // anonymous namespace

void AuditLogModifySchemeTransaction(const NKikimrScheme::TEvModifySchemeTransaction& request, const NKikimrScheme::TEvModifySchemeTransactionResult& response, TSchemeShard* SS, const TString& userSID, const TString& sanitizedToken) {
    // Each TEvModifySchemeTransaction.Transaction is a self sufficient operation and should be logged independently
    // (even if it was packed into a single TxProxy transaction with some other operations).
    for (const auto& operation : request.GetTransaction()) {
        auto logEntry = MakeAuditLogFragment(operation);

        TPath databasePath = DatabasePathFromModifySchemeOperation(SS, operation);
        auto [cloud_id, folder_id, database_id] = GetDatabaseCloudIds(databasePath);
        auto peerName = NKikimr::NAddressClassifier::ExtractAddress(request.GetPeerName());

        AUDIT_LOG(
            AUDIT_PART("component", SchemeshardComponentName)
            AUDIT_PART("tx_id", std::to_string(request.GetTxId()))
            AUDIT_PART("remote_address", (!peerName.empty() ? peerName : EmptyValue))
            AUDIT_PART("subject", (!userSID.empty() ? userSID : EmptyValue))
            AUDIT_PART("sanitized_token", (!sanitizedToken.empty() ? sanitizedToken : EmptyValue))
            AUDIT_PART("database", (!databasePath.IsEmpty() ? databasePath.GetDomainPathString() : EmptyValue))
            AUDIT_PART("operation", logEntry.Operation)
            AUDIT_PART("paths", RenderList(logEntry.Paths), !logEntry.Paths.empty())
            AUDIT_PART("status", GeneralStatus(response.GetStatus()))
            AUDIT_PART("detailed_status", NKikimrScheme::EStatus_Name(response.GetStatus()))
            AUDIT_PART("reason", response.GetReason(), response.HasReason())

            AUDIT_PART("cloud_id", cloud_id, !cloud_id.empty());
            AUDIT_PART("folder_id", folder_id, !folder_id.empty());
            AUDIT_PART("resource_id", database_id, !database_id.empty());

            // Additionally:

            // ModifyACL.
            // Technically, non-empty ModifyACL field could come with any ModifyScheme operation.
            // In practice, ModifyACL will get processed only by:
            // 1. explicit operation ESchemeOpModifyACL -- to modify ACL on a path
            // 2. ESchemeOpMkDir or ESchemeOpCreate* operations -- to set rights to newly created paths/entities
            // 3. ESchemeOpCopyTable -- to be checked against acl size limit, not to be applied in any way
            AUDIT_PART("new_owner", logEntry.NewOwner, !logEntry.NewOwner.empty());
            AUDIT_PART("acl_add", RenderList(logEntry.ACLAdd), !logEntry.ACLAdd.empty());
            AUDIT_PART("acl_remove", RenderList(logEntry.ACLRemove), !logEntry.ACLRemove.empty());

            // AlterUserAttributes.
            // 1. explicit operation ESchemeOpAlterUserAttributes -- to modify user attributes on a path
            // 2. ESchemeOpMkDir or some ESchemeOpCreate* operations -- to set user attributes for newly created paths/entities
            AUDIT_PART("user_attrs_add", RenderList(logEntry.UserAttrsAdd), !logEntry.UserAttrsAdd.empty());
            AUDIT_PART("user_attrs_remove", RenderList(logEntry.UserAttrsRemove), !logEntry.UserAttrsRemove.empty());

            // AlterLogin.
            // explicit operation ESchemeOpAlterLogin -- to modify user and groups
            AUDIT_PART("login_user", logEntry.LoginUser);
            AUDIT_PART("login_group", logEntry.LoginGroup);
            AUDIT_PART("login_member", logEntry.LoginMember);
        );
    }
}

//NOTE: Resurrected a way to log audit records into the common log.
// This should be dropped again as soon as auditlog consumers will switch to a proper way.
void AuditLogModifySchemeTransactionDeprecated(const NKikimrScheme::TEvModifySchemeTransaction& request, const NKikimrScheme::TEvModifySchemeTransactionResult& response, TSchemeShard* SS, const TString& userSID) {
    // Each TEvModifySchemeTransaction.Transaction is a self sufficient operation and should be logged independently
    // (even if it was packed into a single TxProxy transaction with some other operations).
    for (const auto& operation : request.GetTransaction()) {
        auto logEntry = MakeAuditLogFragment(operation);

        TPath databasePath = DatabasePathFromModifySchemeOperation(SS, operation);
        auto peerName = request.GetPeerName();

        auto entry = TStringBuilder();

        entry << "txId: " << std::to_string(request.GetTxId());
        if (!databasePath.IsEmpty()) {
            entry << ", database: " << databasePath.GetDomainPathString();
        }
        entry << ", subject: " << userSID;
        entry << ", status: " << NKikimrScheme::EStatus_Name(response.GetStatus());
        if (response.HasReason()) {
            entry << ", reason: " << response.GetReason();
        }
        entry << ", operation: " << logEntry.Operation;
        if (logEntry.Paths.size() > 1) {
            for (const auto& i : logEntry.Paths) {
                entry << ", dst path: " << i;
            }
        } else if (logEntry.Paths.size() == 1) {
            entry << ", path: " << logEntry.Paths.front();
        } else {
            entry << ", no path";
        }
        if (!logEntry.NewOwner.empty()) {
            entry << ", set owner:" << logEntry.NewOwner;
        }
        for (const auto& i : logEntry.ACLAdd) {
            entry << ", add access: " << i;
        }
        for (const auto& i : logEntry.ACLRemove) {
            entry << ", add access: " << i;
        }

        LOG_NOTICE_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD, "AUDIT: " <<  entry);
    }
}

namespace {

struct TXxportRecord {
    TString OperationName;
    ui64 Id;
    TString Uid;
    TString RemoteAddress;
    TString UserSID;
    TString SanitizedToken;
    TString DatabasePath;
    TString Status;
    Ydb::StatusIds::StatusCode DetailedStatus;
    TString Reason;
    TVector<std::pair<TString, TString>> AdditionalParts;
    TString StartTime;
    TString EndTime;
    TString CloudId;
    TString FolderId;
    TString ResourceId;
};

void AuditLogXxport(TXxportRecord&& record) {
    AUDIT_LOG(
        AUDIT_PART("component", SchemeshardComponentName)

        AUDIT_PART("id", std::to_string(record.Id))
        AUDIT_PART("uid", record.Uid);
        AUDIT_PART("remote_address", (!record.RemoteAddress.empty() ? record.RemoteAddress : EmptyValue))
        AUDIT_PART("subject", (!record.UserSID.empty() ? record.UserSID : EmptyValue))
        AUDIT_PART("sanitized_token", (!record.SanitizedToken.empty() ? record.SanitizedToken : EmptyValue))
        AUDIT_PART("database", (!record.DatabasePath.empty() ? record.DatabasePath : EmptyValue))
        AUDIT_PART("operation", record.OperationName)
        AUDIT_PART("status", record.Status)
        AUDIT_PART("detailed_status", Ydb::StatusIds::StatusCode_Name(record.DetailedStatus))
        AUDIT_PART("reason", record.Reason)

        // all parts are considered required, so all empty values are replaced with a special stub
        for (const auto& [name, value] : record.AdditionalParts) {
            AUDIT_PART(name, (!value.empty() ? value : EmptyValue))
        }

        AUDIT_PART("start_time", record.StartTime)
        AUDIT_PART("end_time", record.EndTime)

        AUDIT_PART("cloud_id", record.CloudId);
        AUDIT_PART("folder_id", record.FolderId);
        AUDIT_PART("resource_id", record.ResourceId);
    );
}

using TParts = decltype(TXxportRecord::AdditionalParts);

template <class Proto>
TParts ExportKindSpecificParts(const Proto& proto) {
    //NOTE: intentional switch -- that will help to detect (by breaking the compilation)
    // the moment when and if oneof Settings will be extended
    switch  (proto.GetSettingsCase()) {
        case Proto::kExportToYtSettings:
            return ExportKindSpecificParts(proto.GetExportToYtSettings());
        case Proto::kExportToS3Settings:
            return ExportKindSpecificParts(proto.GetExportToS3Settings());
        case Proto::SETTINGS_NOT_SET:
            return {};
    }
}
template <> TParts ExportKindSpecificParts(const Ydb::Export::ExportToYtSettings& proto) {
    return {
        {"export_type", "yt"},
        {"export_item_count", ToString(proto.items().size())},
        {"export_yt_prefix", ((proto.items().size() > 0) ? proto.items(0).destination_path() : "")},
    };
}
template <> TParts ExportKindSpecificParts(const Ydb::Export::ExportToS3Settings& proto) {
    return {
        {"export_type", "s3"},
        {"export_item_count", ToString(proto.items().size())},
        {"export_s3_bucket", proto.bucket()},
        //NOTE: take first item's destination_prefix as a "good enough approximation"
        // (each item has its own destination_prefix, but in practice they are all the same)
        {"export_s3_prefix", ((proto.items().size() > 0) ? proto.items(0).destination_prefix() : "")},
    };
}

template <class Proto>
TParts ImportKindSpecificParts(const Proto& proto) {
    //NOTE: intentional switch -- that will help to detect (by breaking the compilation)
    // the moment when and if oneof Settings will be extended
    switch  (proto.GetSettingsCase()) {
        case Proto::kImportFromS3Settings:
            return ImportKindSpecificParts(proto.GetImportFromS3Settings());
        case Proto::SETTINGS_NOT_SET:
            return {};
    }
}
template <> TParts ImportKindSpecificParts(const Ydb::Import::ImportFromS3Settings& proto) {
    return {
        {"import_type", "s3"},
        {"export_item_count", ToString(proto.items().size())},
        {"import_s3_bucket", proto.bucket()},
        //NOTE: take first item's source_prefix as a "good enough approximation"
        // (each item has its own source_prefix, but in practice they are all the same)
        {"import_s3_prefix", ((proto.items().size() > 0) ? proto.items(0).source_prefix() : "")},
    };
}

}  // anonymous namespace

template <class Request, class Response>
void _AuditLogXxportStart(const Request& request, const Response& response, const TString& operationName, TParts&& additionalParts, TSchemeShard* SS) {
    TPath databasePath = DatabasePathFromWorkingDir(SS, request.GetDatabaseName());
    auto [cloud_id, folder_id, database_id] = GetDatabaseCloudIds(databasePath);
    auto peerName = NKikimr::NAddressClassifier::ExtractAddress(request.GetPeerName());
    const auto& entry = response.GetResponse().GetEntry();

    AuditLogXxport({
        .OperationName = operationName,
        //NOTE: original request's tx-id is used as an operation id
        .Id = request.GetTxId(),
        .Uid = GetUid(request.GetRequest().GetOperationParams()),
        .RemoteAddress = peerName,
        .UserSID = request.GetUserSID(),
        .SanitizedToken = request.GetSanitizedToken(),
        .DatabasePath = databasePath.PathString(),
        .Status = (entry.GetStatus() == Ydb::StatusIds::SUCCESS ? "SUCCESS" : "ERROR"),
        .DetailedStatus = entry.GetStatus(),
        //NOTE: use main issue (on {ex,im}port itself), ignore issues on individual items
        .Reason = ((entry.IssuesSize() > 0) ? entry.GetIssues(0).message() : ""),

        .AdditionalParts = std::move(additionalParts),

        // no start or end times

        .CloudId = cloud_id,
        .FolderId = folder_id,
        .ResourceId = database_id,
    });
}

void AuditLogExportStart(const NKikimrExport::TEvCreateExportRequest& request, const NKikimrExport::TEvCreateExportResponse& response, TSchemeShard* SS) {
    _AuditLogXxportStart(request, response, "EXPORT START", ExportKindSpecificParts(request.GetRequest()), SS);
}

void AuditLogImportStart(const NKikimrImport::TEvCreateImportRequest& request, const NKikimrImport::TEvCreateImportResponse& response, TSchemeShard* SS) {
    _AuditLogXxportStart(request, response, "IMPORT START", ImportKindSpecificParts(request.GetRequest()), SS);
}

template <class Info>
void _AuditLogXxportEnd(const Info& info, const TString& operationName, TParts&& additionalParts, TSchemeShard* SS) {
    const TPath databasePath = TPath::Init(info.DomainPathId, SS);
    auto [cloud_id, folder_id, database_id] = GetDatabaseCloudIds(databasePath);
    auto peerName = NKikimr::NAddressClassifier::ExtractAddress(info.PeerName);
    TString userSID = *info.UserSID.OrElse(EmptyValue);
    TString startTime = (info.StartTime != TInstant::Zero() ? info.StartTime.ToString() : TString());
    TString endTime = (info.EndTime != TInstant::Zero() ? info.EndTime.ToString() : TString());

    // Info.State can't be anything but Done or Cancelled here
    Y_ABORT_UNLESS(info.State == Info::EState::Done || info.State == Info::EState::Cancelled);
    TString status = TString(info.State == Info::EState::Done ? "SUCCESS" : "ERROR");
    Ydb::StatusIds::StatusCode detailedStatus = (info.State == Info::EState::Done ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::CANCELLED);

    AuditLogXxport({
        .OperationName = operationName,
        .Id = info.Id,
        .Uid = info.Uid,
        .RemoteAddress = peerName,
        .UserSID = userSID,
        .SanitizedToken = info.SanitizedToken,
        .DatabasePath = databasePath.PathString(),
        .Status = status,
        .DetailedStatus = detailedStatus,
        .Reason = info.Issue,

        .AdditionalParts = std::move(additionalParts),

        .StartTime = startTime,
        .EndTime = endTime,

        .CloudId = cloud_id,
        .FolderId = folder_id,
        .ResourceId = database_id,
    });
}

void AuditLogExportEnd(const TExportInfo& info, TSchemeShard* SS) {
    NKikimrExport::TCreateExportRequest proto;
    // TSchemeShard::FromXxportInfo() can not be used here
    switch (info.Kind) {
        case TExportInfo::EKind::YT:
            Y_ABORT_UNLESS(proto.MutableExportToYtSettings()->ParseFromString(info.Settings));
            proto.MutableExportToYtSettings()->clear_token();
            break;
        case TExportInfo::EKind::S3:
            Y_ABORT_UNLESS(proto.MutableExportToS3Settings()->ParseFromString(info.Settings));
            proto.MutableExportToS3Settings()->clear_access_key();
            proto.MutableExportToS3Settings()->clear_secret_key();
            break;
    }
    _AuditLogXxportEnd(info, "EXPORT END", ExportKindSpecificParts(proto), SS);
}
void AuditLogImportEnd(const TImportInfo& info, TSchemeShard* SS) {
    _AuditLogXxportEnd(info, "IMPORT END", ImportKindSpecificParts(info.Settings), SS);
}

}
