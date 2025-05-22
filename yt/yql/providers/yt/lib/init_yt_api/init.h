#pragma once

#include <util/generic/string.h>
#include <util/generic/maybe.h>

namespace NYql {

void InitYtApiOnce(const TMaybe<TString>& attrs = Nothing());

} // NYql
