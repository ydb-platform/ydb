#pragma once

#include <util/generic/flags.h>

namespace NKikimr::NCms {

enum class EService: ui8 {
    Storage = 1 /* "storage" */,
    DynamicNode = 2 /* "dynnode" */,
    Static = 4 /* "statnode" */,
};

Y_DECLARE_FLAGS(TServices, EService);
Y_DECLARE_OPERATORS_FOR_FLAGS(TServices);

bool TryFromWhiteBoardRole(const TString& role, EService& value);
void TryFromNodeId(ui32 nodeId, TServices& value);

} // namespace NKikimr::NCms
