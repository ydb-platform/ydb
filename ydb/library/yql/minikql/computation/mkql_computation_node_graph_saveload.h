#pragma once

#include "mkql_computation_node.h"

namespace NKikimr {
namespace NMiniKQL {

void SaveGraphState(const NUdf::TUnboxedValue* roots, ui32 rootCount, ui64 hash, TString& out);

void LoadGraphState(const NUdf::TUnboxedValue* roots, ui32 rootCount, ui64 hash, const TStringBuf& in);

} // namespace NMiniKQL
} // namespace NKikimr
