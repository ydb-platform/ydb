#ifndef ACL_H
#error "Direct inclusion of this file is not allowed, include acl.h"
// For the sake of sane code completion.
#include "acl.h"
#endif

#include <library/cpp/yt/error/error.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

template <class TAce>
TError CheckAceCorrect(const TAce& ace)
{
    using NYTree::EPermission;

    if (ace.Action == ESecurityAction::Undefined) {
        return TError("%Qlv action is not allowed",
            ESecurityAction::Undefined);
    }

    // Currently, we allow empty permissions with columns. They seem to be no-op.
    bool onlyReadOrEmpty = None(ace.Permissions & ~EPermission::Read);
    if (ace.Columns && !onlyReadOrEmpty) {
        return TError("ACE specifying columns may contain only %Qlv permission; found %Qlv",
            EPermission::Read,
            ace.Permissions);
    }

    if (Any(ace.Permissions & EPermission::FullRead) && ace.Action != ESecurityAction::Allow) {
        return TError("ACE with %Qlv permission may have only %Qlv action; found %Qlv",
            EPermission::FullRead,
            ESecurityAction::Allow,
            ace.Action);
    }

    bool hasRegisterQueueConsumer = Any(ace.Permissions & EPermission::RegisterQueueConsumer);
    bool onlyRegisterQueueConsumer = ace.Permissions == EPermission::RegisterQueueConsumer;

    if (hasRegisterQueueConsumer && !ace.Vital) {
        return TError("Permission %Qlv requires vitality to be specified",
            EPermission::RegisterQueueConsumer);
    }
    if (ace.Vital && !onlyRegisterQueueConsumer) {
        return TError("ACE specifying vitality must contain a single %Qlv permission; found %Qlv",
            EPermission::RegisterQueueConsumer,
            FormatPermissions(ace.Permissions));
    }

    return TError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
