#pragma once

#include "schemeshard_identificators.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/string.h>
#include <util/generic/maybe.h>

namespace NKikimr {
namespace NSchemeShard {

struct TAuditLogFragment {
    TString Operation;
    TMaybe<TString> Path;
    TVector<TString> SrcPaths;
    TVector<TString> DstPaths;
    TVector<TString> AddACL;
    TVector<TString> RmACL;
    TMaybe<TString> NewOwner;

    TAuditLogFragment(const NKikimrSchemeOp::TModifyScheme& tx);

    void FillACL(const NKikimrSchemeOp::TModifyScheme& tx);
    void FillPathes(const NKikimrSchemeOp::TModifyScheme& tx);

    TString GetAnyPath() const;
    TString ToString() const;
};


} // NSchemeShard
} // NKikimr
