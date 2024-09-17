#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"
#include <ydb/core/protos/auth.pb.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TAlterLogin: public TSubOperationBase {
public:
    using TSubOperationBase::TSubOperationBase;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        NIceDb::TNiceDb db(context.GetTxc().DB); // do not track is there are direct writes happen
        TTabletId ssId = context.SS->SelfTabletId();
        auto result = MakeHolder<TProposeResponse>(OperationId.GetTxId(), ssId);
        if (!AppData()->AuthConfig.GetEnableLoginAuthentication()) {
            result->SetStatus(NKikimrScheme::StatusPreconditionFailed, "Login authentication is disabled");
        } else if (Transaction.GetWorkingDir() != context.SS->LoginProvider.Audience) {
            result->SetStatus(NKikimrScheme::StatusPreconditionFailed, "Wrong working dir");
        } else {
            const NKikimrConfig::TDomainsConfig::TSecurityConfig& securityConfig = context.SS->GetDomainsConfig().GetSecurityConfig();
            const NKikimrSchemeOp::TAlterLogin& alterLogin = Transaction.GetAlterLogin();
            switch (alterLogin.GetAlterCase()) {
                case NKikimrSchemeOp::TAlterLogin::kCreateUser: {
                    const auto& createUser = alterLogin.GetCreateUser();
                    auto response = context.SS->LoginProvider.CreateUser(
                        {.User = createUser.GetUser(), .Password = createUser.GetPassword()});
                    if (response.Error) {
                        result->SetStatus(NKikimrScheme::StatusPreconditionFailed, response.Error);
                    } else {
                        auto& sid = context.SS->LoginProvider.Sids[createUser.GetUser()];
                        db.Table<Schema::LoginSids>().Key(sid.Name).Update<Schema::LoginSids::SidType, Schema::LoginSids::SidHash>(sid.Type, sid.Hash);
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
                    }
                    break;
                }
                case NKikimrSchemeOp::TAlterLogin::kModifyUser: {
                    const auto& modifyUser = alterLogin.GetModifyUser();
                    auto response = context.SS->LoginProvider.ModifyUser({.User = modifyUser.GetUser(), .Password = modifyUser.GetPassword()});
                    if (response.Error) {
                        result->SetStatus(NKikimrScheme::StatusPreconditionFailed, response.Error);
                    } else {
                        auto& sid = context.SS->LoginProvider.Sids[modifyUser.GetUser()];
                        db.Table<Schema::LoginSids>().Key(sid.Name).Update<Schema::LoginSids::SidType, Schema::LoginSids::SidHash>(sid.Type, sid.Hash);
                        result->SetStatus(NKikimrScheme::StatusSuccess);
                    }
                    break;
                }
                case NKikimrSchemeOp::TAlterLogin::kRemoveUser: {
                    const auto& removeUser = alterLogin.GetRemoveUser();
                    const TString& user = removeUser.GetUser();
                    auto response = context.SS->LoginProvider.RemoveUser({
                        .User = user,
                        .MissingOk = removeUser.GetMissingOk()
                    });
                    if (response.Error) {
                        result->SetStatus(NKikimrScheme::StatusPreconditionFailed, response.Error);
                    } else {
                        db.Table<Schema::LoginSids>().Key(user).Delete();
                        for (const TString& group : response.TouchedGroups) {
                            db.Table<Schema::LoginSidMembers>().Key(group, user).Delete();
                        }
                        result->SetStatus(NKikimrScheme::StatusSuccess);
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
                        db.Table<Schema::LoginSids>().Key(sid.Name).Update<Schema::LoginSids::SidType>(sid.Type);
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
                    const TString& group = removeGroup.GetGroup();
                    auto response = context.SS->LoginProvider.RemoveGroup({
                        .Group = group,
                        .MissingOk = removeGroup.GetMissingOk()
                    });
                    if (response.Error) {
                        result->SetStatus(NKikimrScheme::StatusPreconditionFailed, response.Error);
                    } else {
                        db.Table<Schema::LoginSids>().Key(group).Delete();
                        for (const TString& parent : response.TouchedGroups) {
                            db.Table<Schema::LoginSidMembers>().Key(parent, group).Delete();
                        }
                        result->SetStatus(NKikimrScheme::StatusSuccess);
                    }
                    break;
                }
                default: {
                    result->SetStatus(NKikimrScheme::StatusInvalidParameter, "Unknown alter login operation");
                    break;
                }
            }
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
