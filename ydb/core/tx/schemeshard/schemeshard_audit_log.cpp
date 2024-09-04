#include "schemeshard_audit_log.h"
#include "schemeshard_path.h"
#include "schemeshard_audit_log_fragment.h"

#include <ydb/core/audit/audit_log.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/util/address_classifier.h>
#include <util/string/vector.h>

namespace NKikimr::NSchemeShard {

namespace {
    const TString SchemeshardComponentName = "schemeshard";

    //NOTE: EmptyValue couldn't be an empty string as AUDIT_PART() skips parts with an empty values
    const TString EmptyValue = "{none}";
}

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

void AuditLogModifySchemeTransaction(const NKikimrScheme::TEvModifySchemeTransaction& request, const NKikimrScheme::TEvModifySchemeTransactionResult& response, TSchemeShard* SS, const TString& userSID) {
    // Each TEvModifySchemeTransaction.Transaction is a self sufficient operation and should be logged independently
    // (even if it was packed into a single TxProxy transaction with some other operations).
    for (const auto& operation : request.GetTransaction()) {
        auto logEntry = MakeAuditLogFragment(operation);

        TPath databasePath = DatabasePathFromWorkingDir(SS, operation.GetWorkingDir());
        auto [cloud_id, folder_id, database_id] = GetDatabaseCloudIds(databasePath);
        auto peerName = NKikimr::NAddressClassifier::ExtractAddress(request.GetPeerName());

        AUDIT_LOG(
            AUDIT_PART("component", SchemeshardComponentName)
            AUDIT_PART("tx_id", std::to_string(request.GetTxId()))
            AUDIT_PART("remote_address", (!peerName.empty() ? peerName : EmptyValue))
            AUDIT_PART("subject", (!userSID.empty() ? userSID : EmptyValue))
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

        TPath databasePath = DatabasePathFromWorkingDir(SS, operation.GetWorkingDir());
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

void AuditLogLogin(const NKikimrScheme::TEvLogin& request, const NKikimrScheme::TEvLoginResult& response, TSchemeShard* SS) {
    static const TString LoginOperationName = "LOGIN";

    TPath databasePath = TPath::Root(SS);
    auto peerName = NKikimr::NAddressClassifier::ExtractAddress(request.GetPeerName());

    AUDIT_LOG(
        AUDIT_PART("component", SchemeshardComponentName)
        AUDIT_PART("remote_address", (!peerName.empty() ? peerName : EmptyValue))
        AUDIT_PART("database", (!databasePath.PathString().empty() ? databasePath.PathString() : EmptyValue))
        AUDIT_PART("operation", LoginOperationName)
        AUDIT_PART("status", TString(response.GetError().empty() ? "SUCCESS" : "ERROR"))
        AUDIT_PART("reason", response.GetError(), response.HasError())

        // Login
        AUDIT_PART("login_user", (request.HasUser() ? request.GetUser() : EmptyValue))
        AUDIT_PART("login_auth_domain", (!request.GetExternalAuth().empty() ? request.GetExternalAuth() : EmptyValue))
    );
}

}
