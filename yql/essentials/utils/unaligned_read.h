#pragma once

#include <util/system/unaligned_mem.h>

namespace NYql {

template <typename TArg>
inline TArg TypedReadUnaligned(const TArg* address) {
    return ::ReadUnaligned<TArg>(address);
}

} // namespace NYql
