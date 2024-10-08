#include "source_location.h"

#include <string_view>

namespace NKikimr::NUtil {

constexpr inline auto ThisFileLocation = NCompat::TSourceLocation::current();
constexpr inline ui64 ThisFileSuffix = 33; // length of "ydb/core/util/source_location.cpp"
constexpr inline ui64 ThisFileLength = std::string_view(ThisFileLocation.file_name()).length();
constexpr inline ui64 PrefixLength = ThisFileLength - ThisFileSuffix;

TString TrimSourceFileName(const char* fileName) {
    if constexpr (NCompat::HasSourceLocation) {
        static_assert(std::string_view(ThisFileLocation.file_name()).substr(ThisFileSuffix - 2) == std::string_view("ydb/core/util/source_location.cpp"));

        if (std::strlen(fileName) < PrefixLength) {
            return fileName;
        }

        return TString(fileName + PrefixLength);
    }

    return fileName;
}

} // namespace NKikimr::NUtil
