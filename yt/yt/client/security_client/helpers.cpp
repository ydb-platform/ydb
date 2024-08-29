#include "helpers.h"

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/ytree/permission.h>

namespace NYT::NSecurityClient {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TYPath GetUserPath(const std::string& name)
{
    return "//sys/users/" + ToYPathLiteral(name);
}

TYPath GetGroupPath(const TString& name)
{
    return "//sys/groups/" + ToYPathLiteral(name);
}

TYPath GetAccountPath(const TString& name)
{
    return "//sys/accounts/" + ToYPathLiteral(name);
}

////////////////////////////////////////////////////////////////////////////////

ESecurityAction CheckPermissionsByAclAndSubjectClosure(
    const TSerializableAccessControlList& acl,
    const THashSet<TString>& subjectClosure,
    NYTree::EPermissionSet permissions)
{
    NYTree::EPermissionSet allowedPermissions = {};
    NYTree::EPermissionSet deniedPermissions = {};

    for (const auto& ace : acl.Entries) {
        if (ace.Action != NSecurityClient::ESecurityAction::Allow && ace.Action != NSecurityClient::ESecurityAction::Deny) {
            THROW_ERROR_EXCEPTION("Action %Qv is not supported", FormatEnum(ace.Action));
        }
        for (const auto& aceSubject : ace.Subjects) {
            if (subjectClosure.contains(aceSubject)) {
                if (ace.Action == NSecurityClient::ESecurityAction::Allow) {
                    allowedPermissions |= ace.Permissions;
                } else {
                    deniedPermissions |= ace.Permissions;
                }
            }
        }
    }

    return (allowedPermissions & ~deniedPermissions & permissions) == permissions
        ? ESecurityAction::Allow
        : ESecurityAction::Deny;
}

void ValidateSecurityTag(const TSecurityTag& tag)
{
    if (tag.empty()) {
        THROW_ERROR_EXCEPTION("Security tag cannot be empty");
    }
    if (tag.length() > MaxSecurityTagLength) {
        THROW_ERROR_EXCEPTION("Security tag %Qv is too long: %v > %v",
            tag,
            tag.length(),
            MaxSecurityTagLength);
    }
}

void ValidateSecurityTags(const std::vector<TSecurityTag>& tags)
{
    for (const auto& tag : tags) {
        ValidateSecurityTag(tag);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient

