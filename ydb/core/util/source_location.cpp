#include "source_location.h"

#include <string_view>

namespace NKikimr::NUtil {

constexpr inline auto ThisFileLocation = NCompat::TSourceLocation::current();

TString TrimSourceFileName(const char* fileName) {
    if constexpr (NCompat::HasSourceLocation) {
        constexpr static ui64 thisFileSuffix = 33; // length of "ydb/core/util/source_location.cpp"
        constexpr static ui64 thisFileLength = std::string_view(ThisFileLocation.file_name()).length();

        if constexpr (thisFileLength < thisFileSuffix) {
            return fileName;
        }

        constexpr static ui64 prefixLength = thisFileLength - thisFileSuffix;

        if (std::strlen(fileName) < prefixLength) {
            return fileName;
        }

        return TString(fileName + prefixLength);
    }

    return fileName;
}

} // namespace NKikimr::NUtil
