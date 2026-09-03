#include "udf_name.h"

#include <util/string/ascii.h>

#include <algorithm>
#include <climits>

namespace NKikimr::NUdfStore {

bool IsSafeUdfFileName(TStringBuf name) {
    if (name.empty() || name.size() > NAME_MAX || name == "." || name == "..") {
        return false;
    }
    return std::all_of(name.begin(), name.end(), [](char c) {
        return IsAsciiAlnum(c) || c == '.' || c == '_' || c == '-';
    });
}

} // namespace NKikimr::NUdfStore
