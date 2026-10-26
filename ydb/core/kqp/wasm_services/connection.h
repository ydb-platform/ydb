#pragma once

#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>

namespace NKikimr::NKqp {
// Public settings and schema identity must originate from the same resolution.
TString SerializeWasmServiceConnection(const NYql::TKikimrTableMetadata& metadata);
void ValidateWasmServiceConnection(const TString& settings, const NYql::TKikimrTableMetadata& metadata);
}   // namespace NKikimr::NKqp
