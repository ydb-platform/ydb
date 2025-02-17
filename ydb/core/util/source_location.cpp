#include "source_location.h"

#include <string_view>

namespace NKikimr::NUtil {

constexpr inline auto ThisFileLocation = NCompat::TSourceLocation::current();
constexpr inline ui64 ThisFileSuffixLength = 33; // length of "ydb/core/util/source_location.cpp"
constexpr inline ui64 ThisFileLength = std::string_view(ThisFileLocation.file_name()).length();
constexpr inline ui64 PrefixLength = ThisFileLength - ThisFileSuffixLength;

TString TrimSourceFileName(const char* fileName) {
    if constexpr (NCompat::HasSourceLocation) {
        static_assert(std::string_view(ThisFileLocation.file_name()).substr(PrefixLength) == std::string_view("ydb/core/util/source_location.cpp"));

        if (!std::string_view(fileName).starts_with(std::string_view(ThisFileLocation.file_name()).substr(0, PrefixLength))) {
            return fileName;
        }

        return TString(fileName + PrefixLength);
    }

    return fileName;
}

} // namespace NKikimr::NUtil
