#include "not_sorted.h"

namespace NKikimr::NOlap::NReader::NSimple {

std::shared_ptr<IScanCursor> TNotSortedCollection::DoBuildCursor(
    const std::shared_ptr<NCommon::IDataSource>& source, const ui32 readyRecords) const {
    return std::make_shared<TNotSortedSimpleScanCursor>(source->GetSourceId(), readyRecords);
}

}   // namespace NKikimr::NOlap::NReader::NSimple
