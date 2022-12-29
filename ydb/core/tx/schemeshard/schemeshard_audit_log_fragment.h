#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimrSchemeOp {
    class TModifyScheme;
}

namespace NKikimr::NSchemeShard {

struct TAuditLogFragment {
    TString Operation;
    TVector<TString> Paths;
};

TAuditLogFragment MakeAuditLogFragment(const NKikimrSchemeOp::TModifyScheme& tx);

}
