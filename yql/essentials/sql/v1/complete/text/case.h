#pragma once

#include <util/generic/string.h>

namespace NSQLComplete {

bool NoCaseCompare(const TString& lhs, const TString& rhs);

inline auto NoCaseCompareLimit(size_t size) {
    return [size](const TString& lhs, const TString& rhs) -> bool {
        return strncasecmp(lhs.data(), rhs.data(), size) < 0;
    };
}

} // namespace NSQLComplete
