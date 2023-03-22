#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/maybe.h>

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
};

TAuditLogFragment MakeAuditLogFragment(const NKikimrSchemeOp::TModifyScheme& tx);

}
