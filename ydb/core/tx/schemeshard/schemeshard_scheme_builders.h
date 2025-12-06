#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <util/generic/fwd.h>

namespace NKikimr::NSchemeShard {

bool BuildScheme(
    const NKikimrScheme::TEvDescribeSchemeResult& describeResult, 
    TString& scheme,
    const TString& databaseRoot,
    TString& error);

NKikimrSchemeOp::EPathType GetPathType(const NKikimrScheme::TEvDescribeSchemeResult& describeResult);

} // namespace NKikimr::NSchemeShard
