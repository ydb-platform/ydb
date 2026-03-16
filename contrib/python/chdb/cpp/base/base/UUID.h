#pragma once

#include <base/strong_typedef.h>
#include <base/extended_types.h>

namespace DB_CHDB
{
    using UUID = StrongTypedef<UInt128, struct UUIDTag>;
}
