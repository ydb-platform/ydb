#include "security_client.h"
#include "private.h"

namespace NYT::NApi {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ApiLogger;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TGetCurrentUserResult& result, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("user").Value(result.User)
        .EndMap();
}

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
            error.Add("user", user);
            error.Add("permission", permission);
            if (ObjectId) {
                error.Add("denied_by", ObjectId);
            }
            if (SubjectId) {
                error.Add("denied_for", SubjectId);
            }
            if (column) {
                error.Add("column", *column);
            }
            return error;
        }

        default: {
            auto error = TError(
                NSecurityClient::EErrorCode::AuthorizationError,
                "Unexpected security action %Qlv in permission check result for user %Qv",
                Action,
                user);
            YT_TLOG_ALERT("Unexpected security action in permission check result")
                .With(error);
            return error;
        }
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
            error.Add("user", user);
            error.Add("permission", permission);
            if (SubjectId) {
                error.Add("denied_for", SubjectId);
            }
            return error;
        }

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
