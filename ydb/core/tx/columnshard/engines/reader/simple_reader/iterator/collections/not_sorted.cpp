#include "not_sorted.h"

namespace NKikimr::NOlap::NReader::NSimple {

std::shared_ptr<IScanCursor> TNotSortedCollection::DoBuildCursor(
    const std::shared_ptr<NCommon::IDataSource>& source, const ui32 readyRecords) const {
    if (AppDataVerified().ColumnShardConfig.GetEnableCursorV1()) {
        return std::make_shared<TNotSortedSimpleScanCursor>(source->GetSourceIdx(), readyRecords, source->GetPortionIdOptional());
    } else {
        return std::make_shared<TDeprecatedNotSortedSimpleScanCursor>(source->GetDeprecatedPortionId(), readyRecords);
    }
}

bool TNotSortedCollection::DoCheckInFlightLimits() const {
    // Check both sources and pages limits
    if (GetSourcesInFlightCount() >= InFlightLimit) {
        return false;
    }
    if (GetPagesInFlightCount() >= GetMaxPagesInFlight()) {
        return false;
    }
    return true;
}

}   // namespace NKikimr::NOlap::NReader::NSimple
