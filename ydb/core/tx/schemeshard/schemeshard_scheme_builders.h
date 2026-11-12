#pragma once

#include <util/generic/fwd.h>

namespace NKikimrScheme {
    class TEvDescribeSchemeResult;
}

namespace NKikimr::NSchemeShard {

bool BuildCoordinationNodeScheme(
    const NKikimrScheme::TEvDescribeSchemeResult& describeResult,
    TString& scheme,
    TString& error);

bool BuildScheme(
    const NKikimrScheme::TEvDescribeSchemeResult& describeResult,
    TString& scheme,
    const TString& databaseRoot,
    TString& error);

} // namespace NKikimr::NSchemeShard
