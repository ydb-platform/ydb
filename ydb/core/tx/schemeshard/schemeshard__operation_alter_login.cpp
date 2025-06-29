#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_audit_log.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/auth.h>
#include <ydb/core/base/local_user_token.h>
#include <ydb/core/protos/auth.pb.h>

#include <ydb/library/security/util.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TAlterLogin: public TSubOperationBase {
public:
    using TSubOperationBase::TSubOperationBase;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        NIceDb::TNiceDb db(context.GetTxc().DB); // do not track is there are direct writes happen
        TTabletId ssId = context.SS->SelfTabletId();
        const auto txId = OperationId.GetTxId();
        auto result = MakeHolder<TProposeResponse>(txId, ssId);
        if (!AppData()->AuthConfig.GetEnableLoginAuthentication()) {
            result->SetStatus(NKikimrScheme::StatusPreconditionFailed, "Login authentication is disabled");
        } else if (Transaction.GetWorkingDir() != context.SS->LoginProvider.Audience) {
            result->SetStatus(NKikimrScheme::StatusPreconditionFailed, "Wrong working dir");
        } else {
            const NKikimrConfig::TDomainsConfig::TSecurityConfig& securityConfig = context.SS->GetDomainsConfig().GetSecurityConfig();
            const NKikimrSchemeOp::TAlterLogin& alterLogin = Transaction.GetAlterLogin();

            TParts additionalParts;

            switch (alterLogin.GetAlterCase()) {
                case NKikimrSchemeOp::TAlterLogin::kCreateUser: {
                    const auto& createUser = alterLogin.GetCreateUser();

                    NLogin::TLoginProvider::TCreateUserRequest request;
                    request.User = createUser.GetUser();
                    request.Password = createUser.GetPassword();
                    request.CanLogin = createUser.GetCanLogin();
                    request.IsHashedPassword = createUser.GetIsHashedPassword();

                    auto response = context.SS->LoginProvider.CreateUser(request);

                    if (response.Error) {
                        result->SetStatus(NKikimrScheme::StatusPreconditionFailed, response.Error);
                    } else {
                        auto& sid = context.SS->LoginProvider.Sids[createUser.GetUser()];
                        db.Table<Schema::LoginSids>().Key(sid.Name).Update<Schema::LoginSids::SidType,
                                                                           Schema::LoginSids::SidHash,
                                                                           Schema::LoginSids::CreatedAt,
                                                                           Schema::LoginSids::IsEnabled>(sid.Type, sid.PasswordHash, ToMicroSeconds(sid.CreatedAt), sid.IsEnabled);

                        if (securityConfig.HasAllUsersGroup()) {
                            auto response = context.SS->LoginProvider.AddGroupMembership({
                                .Group = securityConfig.GetAllUsersGroup(),
                                .Member = createUser.GetUser(),
                            });
                            if (!response.Error) {
                                db.Table<Schema::LoginSidMembers>().Key(securityConfig.GetAllUsersGroup(), createUser.GetUser()).Update();
                            }
                        }
                        result->SetStatus(NKikimrScheme::StatusSuccess);

                        AddIsUserAdmin(createUser.GetUser(), context.SS->LoginProvider, additionalParts);
                    }
                    break;
                }
                case NKikimrSchemeOp::TAlterLogin::kModifyUser: {
                    const auto& modifyUser = alterLogin.GetModifyUser();

                    NLogin::TLoginProvider::TModifyUserRequest request;

                    request.User = modifyUser.GetUser();

                    if (modifyUser.HasPassword()) {
                        request.Password = modifyUser.GetPassword();
                        request.IsHashedPassword = modifyUser.GetIsHashedPassword();
                    }

                    if (modifyUser.HasCanLogin()) {
                        request.CanLogin = modifyUser.GetCanLogin();
                    }

                    auto response = context.SS->LoginProvider.ModifyUser(request);
                    if (response.Error) {
                        result->SetStatus(NKikimrScheme::StatusPreconditionFailed, response.Error);
                    } else {
                        auto& sid = context.SS->LoginProvider.Sids[modifyUser.GetUser()];
                        db.Table<Schema::LoginSids>().Key(sid.Name).Update<Schema::LoginSids::SidType,
                                                                           Schema::LoginSids::SidHash,
                                                                           Schema::LoginSids::IsEnabled,
                                                                           Schema::LoginSids::FailedAttemptCount>(sid.Type, sid.PasswordHash, sid.IsEnabled, sid.FailedLoginAttemptCount);
                        result->SetStatus(NKikimrScheme::StatusSuccess);

                        AddIsUserAdmin(modifyUser.GetUser(), context.SS->LoginProvider, additionalParts);
                        AddLastSuccessfulLogin(sid, additionalParts);
                    }
                    break;
                }
                case NKikimrSchemeOp::TAlterLogin::kRemoveUser: {
                    const auto& removeUser = alterLogin.GetRemoveUser();

                    auto sid = context.SS->LoginProvider.Sids.find(removeUser.GetUser());
                    if (context.SS->LoginProvider.Sids.end() != sid) {
                        AddLastSuccessfulLogin(sid->second, additionalParts);
                    }

                    auto response = RemoveUser(context, removeUser, db);
                    if (response.Error) {
                        result->SetStatus(NKikimrScheme::StatusPreconditionFailed, response.Error);
                    } else {
                        result->SetStatus(NKikimrScheme::StatusSuccess);

                        AddIsUserAdmin(removeUser.GetUser(), context.SS->LoginProvider, additionalParts);
                    }
                    break;
                }
                case NKikimrSchemeOp::TAlterLogin::kCreateGroup: {
                    const auto& createGroup = alterLogin.GetCreateGroup();
                    const TString& group = createGroup.GetGroup();
                    auto response = context.SS->LoginProvider.CreateGroup({.Group = group});
                    if (response.Error) {
                        result->SetStatus(NKikimrScheme::StatusPreconditionFailed, response.Error);
                    } else {
                        auto& sid = context.SS->LoginProvider.Sids[group];
                        db.Table<Schema::LoginSids>().Key(sid.Name).Update<Schema::LoginSids::SidType,
                                                                           Schema::LoginSids::CreatedAt>(sid.Type, ToMicroSeconds(sid.CreatedAt));
                        result->SetStatus(NKikimrScheme::StatusSuccess);
                    }
                    break;
                }
                case NKikimrSchemeOp::TAlterLogin::kAddGroupMembership: {
                    const auto& addGroupMembership = alterLogin.GetAddGroupMembership();
                    auto response = context.SS->LoginProvider.AddGroupMembership({
                        .Group = addGroupMembership.GetGroup(),
                        .Member = addGroupMembership.GetMember()
                    });
                    if (response.Error) {
                        result->SetStatus(NKikimrScheme::StatusPreconditionFailed, response.Error);
                    } else {
                        db.Table<Schema::LoginSidMembers>().Key(addGroupMembership.GetGroup(), addGroupMembership.GetMember()).Update();
                        result->SetStatus(NKikimrScheme::StatusSuccess);
                        if (response.Notice) {
                            result->AddNotice(response.Notice);
                        }
                    }
                    break;
                }
                case NKikimrSchemeOp::TAlterLogin::kRemoveGroupMembership: {
                    const auto& removeGroupMembership = alterLogin.GetRemoveGroupMembership();
                    auto response = context.SS->LoginProvider.RemoveGroupMembership({
                        .Group = removeGroupMembership.GetGroup(),
                        .Member = removeGroupMembership.GetMember()
                    });
                    if (response.Error) {
                        result->SetStatus(NKikimrScheme::StatusPreconditionFailed, response.Error);
                    } else {
                        db.Table<Schema::LoginSidMembers>().Key(removeGroupMembership.GetGroup(), removeGroupMembership.GetMember()).Delete();
                        result->SetStatus(NKikimrScheme::StatusSuccess);
                        if (response.Warning) {
                            result->AddWarning(response.Warning);
                        }
                    }
                    break;
                }
                case NKikimrSchemeOp::TAlterLogin::kRenameGroup: {
                    const auto& renameGroup = alterLogin.GetRenameGroup();
                    const TString& group = renameGroup.GetGroup();
                    const TString& newName = renameGroup.GetNewName();
                    auto response = context.SS->LoginProvider.RenameGroup({
                        .Group = group,
                        .NewName = newName
                    });
                    if (response.Error) {
                        result->SetStatus(NKikimrScheme::StatusPreconditionFailed, response.Error);
                    } else {
                        db.Table<Schema::LoginSids>().Key(group).Delete();
                        for (const TString& parent : response.TouchedGroups) {
                            db.Table<Schema::LoginSidMembers>().Key(parent, group).Delete();
                            db.Table<Schema::LoginSidMembers>().Key(parent, newName).Update();
                        }
                        result->SetStatus(NKikimrScheme::StatusSuccess);
                    }
                    break;
                }
                case NKikimrSchemeOp::TAlterLogin::kRemoveGroup: {
                    const auto& removeGroup = alterLogin.GetRemoveGroup();
                    auto response = RemoveGroup(context, removeGroup, db);
                    if (response.Error) {
                        result->SetStatus(NKikimrScheme::StatusPreconditionFailed, response.Error);
                    } else {
                        result->SetStatus(NKikimrScheme::StatusSuccess);
                    }
                    break;
                }
                default: {
                    result->SetStatus(NKikimrScheme::StatusInvalidParameter, "Unknown alter login operation");
                    break;
                }
            }

            TString userSID, sanitizedToken;
            if (context.UserToken) {
                userSID = context.UserToken->GetUserSID();
                sanitizedToken = context.UserToken->GetSanitizedToken();
            }
            const auto status = result->Record.GetStatus();
            const auto reason = result->Record.HasReason() ? result->Record.GetReason() : TString();
            AuditLogModifySchemeOperation(Transaction, status, reason, context.SS, context.PeerName, userSID, sanitizedToken, ui64(txId), additionalParts);
        }

        if (result->Record.GetStatus() == NKikimrScheme::StatusSuccess) {
            TPathId subDomainPathId = context.SS->GetCurrentSubDomainPathId();
            TSubDomainInfo::TPtr domainPtr = context.SS->ResolveDomainInfo(subDomainPathId);
            domainPtr->UpdateSecurityState(context.SS->LoginProvider.GetSecurityState());
            domainPtr->IncSecurityStateVersion();
            context.SS->PersistSubDomainSecurityStateVersion(db, subDomainPathId, *domainPtr);
            context.OnComplete.PublishToSchemeBoard(OperationId, subDomainPathId);
        }

        context.OnComplete.DoneOperation(OperationId);
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TAlterLogin");
    }

    bool ProgressState(TOperationContext&) override {
        Y_ABORT("no progress state for TAlterLogin");
    }

    void AbortUnsafe(TTxId, TOperationContext&) override {
        Y_ABORT("no AbortUnsafe for TAlterLogin");
    }

    NLogin::TLoginProvider::TBasicResponse RemoveUser(TOperationContext& context, const NKikimrSchemeOp::TLoginRemoveUser& removeUser, NIceDb::TNiceDb& db) {
        const TString& user = removeUser.GetUser();

        if (!context.SS->LoginProvider.CheckUserExists(user)) {
            if (removeUser.GetMissingOk()) {
                return {}; // success
            }
            return {.Error = "User not found"};
        }

        if (auto canRemove = CanRemoveSid(context, user, "User"); canRemove.Error) {
            return canRemove;
        }

        auto removeUserResponse = context.SS->LoginProvider.RemoveUser(user);
        if (removeUserResponse.Error) {
            return removeUserResponse;
        }

        db.Table<Schema::LoginSids>().Key(user).Delete();
        for (const TString& group : removeUserResponse.TouchedGroups) {
            db.Table<Schema::LoginSidMembers>().Key(group, user).Delete();
        }

        return {}; // success
    }

    NLogin::TLoginProvider::TBasicResponse RemoveGroup(TOperationContext& context, const NKikimrSchemeOp::TLoginRemoveGroup& removeGroup, NIceDb::TNiceDb& db) {
        const TString& group = removeGroup.GetGroup();

        if (!context.SS->LoginProvider.CheckGroupExists(group)) {
            if (removeGroup.GetMissingOk()) {
                return {}; // success
            }
            return {.Error = "Group not found"};
        }

        if (auto canRemove = CanRemoveSid(context, group, "Group"); canRemove.Error) {
            return canRemove;
        }

        auto removeGroupResponse = context.SS->LoginProvider.RemoveGroup(group);
        if (removeGroupResponse.Error) {
            return removeGroupResponse;
        }

        db.Table<Schema::LoginSids>().Key(group).Delete();
        for (const TString& parent : removeGroupResponse.TouchedGroups) {
            db.Table<Schema::LoginSidMembers>().Key(parent, group).Delete();
        }

        return {}; // success
    }

    NLogin::TLoginProvider::TBasicResponse CanRemoveSid(TOperationContext& context, const TString sid, const TString& sidType) {
        if (!AppData()->FeatureFlags.GetEnableStrictAclCheck()) {
            return {};
        }

        auto subTree = context.SS->ListSubTree(context.SS->RootPathId(), context.Ctx);
        for (auto pathId : subTree) {
            TPathElement::TPtr path = context.SS->PathsById.at(pathId);
            if (path->Owner == sid) {
                auto pathStr = TPath::Init(pathId, context.SS).PathString();
                return {.Error = TStringBuilder() <<
                    sidType << " " << sid << " owns " << pathStr << " and can't be removed"};
            }
            NACLib::TACL acl(path->ACL);
            if (acl.HasAccess(sid)) {
                auto pathStr = TPath::Init(pathId, context.SS).PathString();
                return {.Error = TStringBuilder() <<
                    sidType << " " << sid << " has an ACL record on " << pathStr << " and can't be removed"};
            }
        }

        return {}; // success
    }

    void AddIsUserAdmin(const TString& user, NLogin::TLoginProvider& loginProvider, TParts& additionalParts) {
        const auto userToken = NKikimr::BuildLocalUserToken(loginProvider, user);

        if (IsAdministrator(AppData(), &userToken)) {
            additionalParts.emplace_back("login_user_level", "admin");
        }
    }

    void AddLastSuccessfulLogin(NLogin::TLoginProvider::TSidRecord& sid, TParts& additionalParts) {
        const auto duration = sid.LastSuccessfulLogin.time_since_epoch();
        const auto time = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
        if (time) {
            additionalParts.emplace_back("last_login", TInstant::MicroSeconds(time).ToString());
        }
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateAlterLogin(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterLogin>(id, tx);
}

ISubOperation::TPtr CreateAlterLogin(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state == TTxState::Invalid || state == TTxState::Propose);
    return MakeSubOperation<TAlterLogin>(id);
}

}
