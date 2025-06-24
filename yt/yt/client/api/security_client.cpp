#include "security_client.h"

namespace NYT::NApi {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TError TCheckPermissionResult::ToError(
    const std::string& user,
    EPermission permission,
    const std::optional<std::string>& column) const
{
    switch (Action) {
        case NSecurityClient::ESecurityAction::Allow:
            return TError();

        case NSecurityClient::ESecurityAction::Deny: {
            TError error;
            if (ObjectName && SubjectName) {
                error = TError(
                    NSecurityClient::EErrorCode::AuthorizationError,
                    "Access denied for user %Qv: %Qlv permission is denied for %Qv by ACE at %v",
                    user,
                    permission,
                    *SubjectName,
                    *ObjectName);
            } else {
                error = TError(
                    NSecurityClient::EErrorCode::AuthorizationError,
                    "Access denied for user %Qv: %Qlv permission is not allowed by any matching ACE",
                    user,
                    permission);
            }
            error <<= TErrorAttribute("user", user);
            error <<= TErrorAttribute("permission", permission);
            if (ObjectId) {
                error <<= TErrorAttribute("denied_by", ObjectId);
            }
            if (SubjectId) {
                error <<= TErrorAttribute("denied_for", SubjectId);
            }
            if (column) {
                error <<= TErrorAttribute("column", *column);
            }
            return error;
        }

        default:
            YT_ABORT();
    }
}

TError TCheckPermissionByAclResult::ToError(const std::string& user, EPermission permission) const
{
    switch (Action) {
        case NSecurityClient::ESecurityAction::Allow:
            return TError();

        case NSecurityClient::ESecurityAction::Deny: {
            TError error;
            if (SubjectName) {
                error = TError(
                    NSecurityClient::EErrorCode::AuthorizationError,
                    "Access denied for user %Qv: %Qlv permission is denied for %Qv by ACL",
                    user,
                    permission,
                    *SubjectName);
            } else {
                error = TError(
                    NSecurityClient::EErrorCode::AuthorizationError,
                    "Access denied for user %Qv: %Qlv permission is not allowed by any matching ACE",
                    user,
                    permission);
            }
            error <<= TErrorAttribute("user", user);
            error <<= TErrorAttribute("permission", permission);
            if (SubjectId) {
                error <<= TErrorAttribute("denied_for", SubjectId);
            }
            return error;
        }

        default:
            YT_ABORT();
    }
}

void TGetCurrentUserResult::Register(TRegistrar registrar)
{
    registrar.Parameter("user", &TThis::User);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
