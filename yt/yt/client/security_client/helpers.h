#pragma once

#include "acl.h"
#include "public.h"

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetUserPath(const std::string& name);
NYPath::TYPath GetGroupPath(const std::string& name);
NYPath::TYPath GetAccountPath(const std::string& name);

////////////////////////////////////////////////////////////////////////////////

ESecurityAction CheckPermissionsByAclAndSubjectClosure(
    const TSerializableAccessControlList& acl,
    const THashSet<std::string>& subjectClosure,
    NYTree::EPermissionSet permissions);

void ValidateSecurityTag(const TSecurityTag& tag);
void ValidateSecurityTags(const std::vector<TSecurityTag>& tags);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient

