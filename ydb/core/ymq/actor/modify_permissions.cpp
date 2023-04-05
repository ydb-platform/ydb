#include "action.h"

#include <google/protobuf/text_format.h>

#include <util/string/ascii.h>
#include <util/string/vector.h>
#include <util/string/cast.h>
#include <util/string/join.h>

namespace NKikimr::NSQS {

class TModifyPermissionsActor
   : public TActionActor<TModifyPermissionsActor> {
public:
    TModifyPermissionsActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TActionActor(sourceSqsRequest, EAction::ModifyPermissions, std::move(cb))
    {
    }

    static constexpr bool NeedExistingQueue() {
        return false;
    }

    static constexpr bool NeedUserSpecified() {
        return false;
    }

private:
    bool TryConvertSQSPermissionToYdbACLMask(const TString& permission, ui32& mask) const {
        mask = GetACERequiredAccess(permission);
        return mask != 0;
    }

#define PROCESS_MODIFY_ACL_ACTION(PERMISSION_NAME, ACCESS_ACTION_TYPE, CLEAR_FLAG)                                                                     \
    case TPermissionsAction::Y_CAT(k, PERMISSION_NAME): {                                                                                              \
        const auto& SID = action.Y_CAT(Get, PERMISSION_NAME)().GetSubject();                                                                           \
        if (CLEAR_FLAG) {                                                                                                                              \
            ACLDiff_.ClearAccessForSid(SID);                                                                                                           \
        }                                                                                                                                              \
        for (const auto& name : action.Y_CAT(Get, PERMISSION_NAME)().GetPermissionNames()) {                                                           \
            if (!TryConvertSQSPermissionToYdbACLMask(name, mask)) {                                                                                    \
                TString permissionName(Y_STRINGIZE(PERMISSION_NAME));                                                                                  \
                permissionName.to_lower();                                                                                                             \
                const auto errorMsg = Sprintf("ModifyPermissions failed to %s unknown permission %s.", permissionName.c_str(), name.c_str());          \
                MakeError(MutableErrorDesc(), NErrors::INVALID_PARAMETER_VALUE, errorMsg);                                                             \
                return false;                                                                                                                          \
            }                                                                                                                                          \
            ACLDiff_.Y_CAT(ACCESS_ACTION_TYPE, Access)(NACLib::EAccessType::Allow, mask, SID);                                                         \
        }                                                                                                                                              \
        break;                                                                                                                                         \
    }

    bool DoValidate() override {
        if (!Request().GetResource()) {
            MakeError(Response_.MutableModifyPermissions(), NErrors::MISSING_PARAMETER, "No Resource parameter.");
            return false;
        }

        Resource_ = MakeAbsolutePath(Request().GetResource());

        if (IsForbiddenPath(Resource_)) {
            MakeError(MutableErrorDesc(), NErrors::INVALID_PARAMETER_VALUE, Sprintf("Path does not exist: %s.", SanitizeNodePath(Resource_).c_str()));
            return false;
        }

        if (Request().GetClearACL()) {
            ACLDiff_.ClearAccess();
        }

        ui32 mask = 0;
        for (const auto& action : Request().GetActions()) {
            switch (action.Action_case()) {
                PROCESS_MODIFY_ACL_ACTION(Set, Add, true);
                PROCESS_MODIFY_ACL_ACTION(Grant, Add, false);
                PROCESS_MODIFY_ACL_ACTION(Revoke, Remove, false);
                default: {
                    MakeError(MutableErrorDesc(), NErrors::INVALID_PARAMETER_VALUE, "Unknown ModifyPermissions action.");
                    return false;
                }
            }
        }

        return true;
    }

    TError* MutableErrorDesc() override {
        return Response_.MutableModifyPermissions()->MutableError();
    }

    virtual TString GetCustomACLPath() const override {
        return MakeAbsolutePath(Request().GetResource());
    }

    TString DoGetQueueName() const override {
        return {};
    }

    void DoAction() override {
        Become(&TThis::StateFunc);

        TStringBuf workingDir, resource;
        TStringBuf(Resource_).RSplit('/', workingDir, resource);

        auto proposeRequest = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        NKikimrTxUserProxy::TEvProposeTransaction& record = proposeRequest->Record;
        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpModifyACL);
        modifyScheme->SetWorkingDir(TString(workingDir));
        modifyScheme->MutableModifyACL()->SetName(TString(resource));

        modifyScheme->MutableModifyACL()->SetDiffACL(ACLDiff_.SerializeAsString());
        Send(MakeTxProxyID(), proposeRequest.Release());
    }

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup, HandleWakeup);
            hFunc(TEvTxUserProxy::TEvProposeTransactionStatus, HandleProposeTransactionStatus);
        }
    }

    void LogModifyACLRequestResultSafe(bool success) const {
        const TString user = (UserToken_ && UserToken_->GetUserSID()) ? UserToken_->GetUserSID() : "Someone";

        TModifyPermissionsRequest copy;
        copy.CopyFrom(Request());
        copy.ClearCredentials();
        copy.ClearResource();

        if (success) {
            RLOG_SQS_WARN(user << " modified ACL for " << Resource_ << " with request " << copy);
        } else {
            RLOG_SQS_ERROR(user << " failed to modify ACL for " << Resource_ << " with request " << copy);
        }
    }

    void HandleProposeTransactionStatus(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        const TEvTxUserProxy::TEvProposeTransactionStatus* msg = ev->Get();
        const auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(msg->Record.GetStatus());
        switch (status) {
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete:
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress: {
                LogModifyACLRequestResultSafe(true);
                break;
            }

            case TEvTxUserProxy::TResultStatus::ResolveError:
            case TEvTxUserProxy::TResultStatus::AccessDenied:
            case TEvTxUserProxy::TResultStatus::ExecError:
            default: {
                LogModifyACLRequestResultSafe(false);
                MakeError(MutableErrorDesc(), NErrors::INTERNAL_FAILURE);
                break;
            }
        }
        SendReplyAndDie();
    }

    const TModifyPermissionsRequest& Request() const {
        return SourceSqsRequest_.GetModifyPermissions();
    }

private:
    NACLib::TDiffACL ACLDiff_;
    TString Resource_;
};

IActor* CreateModifyPermissionsActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TModifyPermissionsActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
