#pragma once

#include "column.h"

namespace NClickHouse {
    TColumnRef CreateColumnByType(const TString& type_name);
}
