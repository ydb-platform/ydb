#pragma once

#include "schemeshard_export_table_scheme.h"

#include <util/generic/fwd.h>

namespace NKikimrScheme {
    class TEvDescribeSchemeResult;
}

namespace NKikimr::NSchemeShard {

bool BuildCreateTableScheme(
    const TExportTableSchemeContext& context,
    TString& scheme,
    TString& error);

bool BuildScheme(
    const NKikimrScheme::TEvDescribeSchemeResult& describeResult,
    TString& scheme,
    const TString& databaseRoot,
    TString& error);

} // namespace NKikimr::NSchemeShard
