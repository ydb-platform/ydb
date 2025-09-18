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

    // NB(coteeq): There is an intuitive reasoning behind all these checks:
    // Let's define 'special' ACEs. ACE is special if either is true:
    // 1. it contains full_read permission
    // 2. it specifies expression
    // 3. it specifies columns.
    //
    // For the sake of simplicity, we do not want to care how these three kinds
    // of specialness should compose together, so if any two statements
    // from the list are true, ACE is considered to be invalid.
    // Moreover, the only action any special ACE may specify is `allow`.
    //
    // Lastly, if either expression or columns are specified, the only valid
    // permission is `read`.

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

    if (Any(ace.Permissions & EPermission::FullRead) && (ace.Expression || ace.Columns)) {
        return TError(
            "ACE with %Qlv permission may not specify %Qlv or \"columns\"",
            EPermission::FullRead,
            TSerializableAccessControlEntry::ExpressionKey);
    }

    if (ace.Expression && ace.Action != ESecurityAction::Allow) {
        return TError(
            "ACE specifying %Qlv may have only %Qlv action; found %Qlv",
            TSerializableAccessControlEntry::ExpressionKey,
            ESecurityAction::Allow,
            ace.Action);
    }

    if (ace.Expression && ace.Permissions != EPermission::Read) {
        return TError(
            "ACE specifying %Qlv may contain only %Qlv permission; found %Qlv",
            TSerializableAccessControlEntry::ExpressionKey,
            EPermission::Read,
            ace.Permissions);
    }

    if (ace.InapplicableExpressionMode && !ace.Expression) {
        return TError(
            "%Qlv can only be specified if %Qlv is specified",
            TSerializableAccessControlEntry::InapplicableExpressionModeKey,
            TSerializableAccessControlEntry::ExpressionKey);
    }

    if (ace.Expression && ace.Columns) {
        return TError(
            "Single ACE must not contain both \"columns\" and %Qlv",
            TSerializableAccessControlEntry::ExpressionKey);
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

template <class TAce>
void ValidateAceCorrect(const TAce& ace)
{
    CheckAceCorrect(ace)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
