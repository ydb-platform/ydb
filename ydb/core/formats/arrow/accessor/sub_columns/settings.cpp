#include "settings.h"

namespace NKikimr::NArrow::NAccessor::NSubColumns {

TSettings::TColumnsDistributor::EColumnType TSettings::TColumnsDistributor::TakeAndDetect(const ui64 columnSize, const ui32 columnValuesCount) {
    if (!!PredSize) {
        AFL_VERIFY(columnSize <= *PredSize)("col", columnSize)("pred", PredSize);
    }
    PredSize = columnSize;
    if (Settings.GetColumnsLimit() <= SeparatedCount) {
        return EColumnType::Other;
    }
    AFL_VERIFY(SumSize >= CurrentColumnsSize)("sum", SumSize)("columns", CurrentColumnsSize);
    if (!SumSize || 1.0 * CurrentColumnsSize / SumSize < 1 - Settings.GetOthersAllowedFraction()) {
        CurrentColumnsSize += columnSize;
        ++SeparatedCount;
        return EColumnType::Separated;
    } else if (!RecordsCount || RecordsCount < Settings.GetSparsedDetectorKff() * columnValuesCount) {
        CurrentColumnsSize += columnSize;
        ++SeparatedCount;
        return EColumnType::Separated;
    }
    return EColumnType::Other;
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
