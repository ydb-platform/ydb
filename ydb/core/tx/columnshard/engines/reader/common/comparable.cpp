#include "comparable.h"

namespace NKikimr::NOlap::NReader::NCommon {

std::partial_ordering TReplaceKeyAdapter::Compare(const TReplaceKeyAdapter& item) const {
    AFL_VERIFY(Reverse == item.Reverse);
    const std::partial_ordering result = Value.CompareNotNull(item.Value);
    if (result == std::partial_ordering::equivalent) {
        return std::partial_ordering::equivalent;
    } else if (result == std::partial_ordering::less) {
        return Reverse ? std::partial_ordering::greater : std::partial_ordering::less;
    } else if (result == std::partial_ordering::greater) {
        return Reverse ? std::partial_ordering::less : std::partial_ordering::greater;
    } else {
        AFL_VERIFY(false);
        return std::partial_ordering::less;
    }
}

}   // namespace NKikimr::NOlap::NReader::NCommon
