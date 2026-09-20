#include "case.h"

#include <util/string/ascii.h>

namespace NSQLComplete {

bool NoCaseCompare(const TString& lhs, const TString& rhs) {
    return AsciiCompareIgnoreCase(lhs, rhs) < 0;
}

} // namespace NSQLComplete
