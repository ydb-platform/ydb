#include "comparable.h"

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>

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

TReplaceKeyAdapter TReplaceKeyAdapter::BuildStart(const TPortionInfo& portion, const TReadMetadataBase& readMetadata) {
    if (readMetadata.IsDescSorted()) {
        return TReplaceKeyAdapter(portion.IndexKeyEnd(), true);
    } else {
        return TReplaceKeyAdapter(portion.IndexKeyStart(), false);
    }
}

TReplaceKeyAdapter TReplaceKeyAdapter::BuildFinish(const TPortionInfo& portion, const TReadMetadataBase& readMetadata) {
    if (readMetadata.IsDescSorted()) {
        return TReplaceKeyAdapter(portion.IndexKeyStart(), true);
    } else {
        return TReplaceKeyAdapter(portion.IndexKeyEnd(), false);
    }
}

}   // namespace NKikimr::NOlap::NReader::NCommon
