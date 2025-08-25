#pragma once

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimrSchemeOp {
    class TModifyScheme;
}

namespace NKikimr::NSchemeShard {

struct TAuditLogFragment {
    TString Operation;
    TVector<TString> Paths;
    TString NewOwner;
    TVector<TString> ACLAdd;
    TVector<TString> ACLRemove;
    TVector<TString> UserAttrsAdd;
    TVector<TString> UserAttrsRemove;
    TString LoginUser;
    TString LoginGroup;
    TString LoginMember;
    TVector<TString> LoginUserChange;
};

TAuditLogFragment MakeAuditLogFragment(const NKikimrSchemeOp::TModifyScheme& tx);

}
