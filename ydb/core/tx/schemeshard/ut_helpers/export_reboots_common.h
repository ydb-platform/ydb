#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSchemeShardUT_Private {

class TTestWithReboots;

namespace NExportReboots {

struct TTypedScheme {
    TString Scheme;
    NKikimrSchemeOp::EPathType Type;

    explicit TTypedScheme(const TString& scheme, NKikimrSchemeOp::EPathType type = NKikimrSchemeOp::EPathTypeTable)
        : Scheme(scheme)
        , Type(type)
    {}

    TTypedScheme(const char* scheme, NKikimrSchemeOp::EPathType type = NKikimrSchemeOp::EPathTypeTable)
        : Scheme(scheme)
        , Type(type)
    {}
};

void Run(const TVector<TTypedScheme>& schemeObjects, const TString& request, TTestWithReboots& t);
void Cancel(const TVector<TTypedScheme>& schemeObjects, const TString& request, TTestWithReboots& t);
void Forget(const TVector<TTypedScheme>& schemeObjects, const TString& request, TTestWithReboots& t);

} // NExportReboots
} // NSchemeShardUT_Private
