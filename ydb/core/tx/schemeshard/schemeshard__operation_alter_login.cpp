#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"
#include <ydb/core/protos/auth.pb.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TAlterLogin: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::Done;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        NIceDb::TNiceDb db(context.GetTxc().DB); // do not track is there are direct writes happen
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TAlterLogin Propose"
            << ", opId: " << OperationId
            << ", at schemeshard: " << ssId);

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
                    auto response = RemoveUser(context, removeUser, db);
                    if (response.Error) {
                        result->SetStatus(NKikimrScheme::StatusPreconditionFailed, response.Error);
                    } else {
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

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TAlterLogin Propose"
            << " " << result->Record.GetStatus()
            << ", opId: " << OperationId
            << ", at schemeshard: " << ssId);

        SetState(NextState());
        result->SetStatus(NKikimrScheme::StatusAccepted);
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TAlterLogin");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterLogin AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }

    NLogin::TLoginProvider::TBasicResponse RemoveUser(TOperationContext& context, const NKikimrSchemeOp::TLoginRemoveUser& removeUser, NIceDb::TNiceDb& db) {
        const TString& user = removeUser.GetUser();

        if (!context.SS->LoginProvider.CheckUserExists(user)) {
            if (removeUser.GetMissingOk()) {
                return {}; // success
            }
            return {.Error = "User not found"};
        }

        auto subTree = context.SS->ListSubTree(context.SS->RootPathId(), context.Ctx);
        for (auto pathId : subTree) {
            TPathElement::TPtr path = context.SS->PathsById.at(pathId);
            if (path->Owner == user) {
                auto pathStr = TPath::Init(pathId, context.SS).PathString();
                return {.Error = TStringBuilder() << 
                    "User " << user << " owns " << pathStr << " and can't be removed"};
            }
        }

        auto removeUserResponse = context.SS->LoginProvider.RemoveUser(user);
        if (removeUserResponse.Error) {
            return removeUserResponse;
        }

        for (auto pathId : subTree) {
            TPathElement::TPtr path = context.SS->PathsById.at(pathId);
            NACLib::TACL acl(path->ACL);
            if (acl.TryRemoveAccess(user)) {
                ++path->ACLVersion;
                path->ACL = acl.SerializeAsString();
                context.SS->PersistACL(db, path);
                if (!path->IsPQGroup()) {
                    const auto parent = context.SS->PathsById.at(path->ParentPathId);
                    ++parent->DirAlterVersion;
                    context.SS->PersistPathDirAlterVersion(db, parent);
                    context.SS->ClearDescribePathCaches(parent);
                    context.OnComplete.PublishToSchemeBoard(OperationId, parent->PathId);
                }
            }
            NACLib::TACL effectiveACL(path->CachedEffectiveACL.GetForSelf());
            if (effectiveACL.HasAccess(user)) {
                // publish paths from which the user's access is being removed
                // user access could have been granted directly (ACL, handled by `acl.TryRemoveAccess(user)` above)
                // or it might have been inherited from a parent (effective ACL)
                context.OnComplete.PublishToSchemeBoard(OperationId, pathId);
            }
        }

        context.OnComplete.UpdateTenants(std::move(subTree));
        db.Table<Schema::LoginSids>().Key(user).Delete();
        for (const TString& group : removeUserResponse.TouchedGroups) {
            db.Table<Schema::LoginSidMembers>().Key(group, user).Delete();
        }
        
        return {}; // success
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
