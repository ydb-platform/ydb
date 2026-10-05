#include "udf_name.h"

#include <util/string/ascii.h>

#include <algorithm>
#include <limits.h>

namespace NKikimr::NUdfStore {

bool IsSafeUdfFileName(TStringBuf name) {
    // NAME_MAX bounds one directory entry, and the reader creates the
    // temporary sibling before the file itself. A name that fits while its
    // sibling does not can never finish loading, so it is not a usable name --
    // do not raise this back to NAME_MAX.
    if (name.empty()
        || name.size() + UdfTmpFileSuffix.size() > NAME_MAX
        || name == "."
        || name == "..")
    {
        return false;
    }
    return std::all_of(name.begin(), name.end(), [](char c) {
        return IsAsciiAlnum(c) || c == '.' || c == '_' || c == '-';
    });
}

} // namespace NKikimr::NUdfStore
