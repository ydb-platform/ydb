#pragma once

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimrSchemeOp {
    class TModifyScheme;
}

namespace NKikimr::NSchemeShard {

struct TAuditLogFragment {
    const TString Operation;
    TMaybe<TString> Path;
    TVector<TString> SrcPaths;
    TVector<TString> DstPaths;
    TVector<TString> AddACL;
    TVector<TString> RmACL;
    TMaybe<TString> NewOwner;
    TMaybe<TString> ProtoRequest;

    TAuditLogFragment(const NKikimrSchemeOp::TModifyScheme& tx);

    void FillACL(const NKikimrSchemeOp::TModifyScheme& tx);
    void FillPathes(const NKikimrSchemeOp::TModifyScheme& tx);

    TString GetAnyPath() const;
    TString ToString() const;
};

}
