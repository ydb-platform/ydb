#pragma once

#include "node.h"

namespace NSQLTranslationV1 {

struct TRoleParameters {
protected:
    TRoleParameters() = default;

public:
    TVector<TDeferredAtom> Roles;
};

struct TUserParameters: TRoleParameters {
    TMaybe<TDeferredAtom> Password;
    bool IsPasswordNull = false;
    bool IsPasswordEncrypted = false;
    std::optional<bool> CanLogin;
    TMaybe<TDeferredAtom> Hash;
};

struct TCreateGroupParameters: TRoleParameters {};

TNodePtr BuildCreateGroup(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TMaybe<TCreateGroupParameters>& params, TScopedStatePtr scoped);
TNodePtr BuildControlUser(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name,
                          const TMaybe<TUserParameters>& params, TScopedStatePtr scoped, bool isCreateUser);
TNodePtr BuildRenameUser(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TDeferredAtom& newName, TScopedStatePtr scoped);
TNodePtr BuildAlterGroup(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TVector<TDeferredAtom>& toChange, bool isDrop,
                         TScopedStatePtr scoped);
TNodePtr BuildRenameGroup(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TDeferredAtom& newName, TScopedStatePtr scoped);
TNodePtr BuildDropRoles(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TVector<TDeferredAtom>& toDrop, bool isUser, bool missingOk, TScopedStatePtr scoped);
TNodePtr BuildGrantPermissions(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TVector<TDeferredAtom>& permissions, const TVector<TDeferredAtom>& schemaPaths, const TVector<TDeferredAtom>& roleName, TScopedStatePtr scoped);
TNodePtr BuildRevokePermissions(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TVector<TDeferredAtom>& permissions, const TVector<TDeferredAtom>& schemaPaths, const TVector<TDeferredAtom>& roleName, TScopedStatePtr scoped);

} // namespace NSQLTranslationV1
