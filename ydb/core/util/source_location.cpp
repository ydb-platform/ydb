#include "source_location.h"

namespace NKikimr::NUtil {

constexpr inline auto ThisFileLocation = NCompat::TSourceLocation::current();

TString TrimSourceFileName(const char* fileName) {
    if constexpr (NCompat::HasSourceLocation) {
        constexpr static ui64 thisFileSuffix = 33; // length of "ydb/core/util/source_location.cpp"
        const static ui64 prefixLen = std::strlen(ThisFileLocation.file_name()) - thisFileSuffix;

        return TString(fileName + prefixLen);
    }

    return fileName;
}

} // namespace NKikimr::NUtil
