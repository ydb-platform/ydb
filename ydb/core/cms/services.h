#pragma once

#include <util/generic/flags.h>

namespace NKikimr::NCms {

enum class EService: ui8 {
    Storage = 1 /* "storage" */,
    DynamicNode = 2 /* "dynnode" */,
};

Y_DECLARE_FLAGS(TServices, EService);
Y_DECLARE_OPERATORS_FOR_FLAGS(TServices);

bool TryFromWhiteBoardRole(const TString& role, EService& value);

} // namespace NKikimr::NCms
