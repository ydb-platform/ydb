#include "batch_iterator.h"

namespace NKikimr::NArrow::NMerger {

NJson::TJsonValue TBatchIterator::DebugJson() const {
    NJson::TJsonValue result;
    result["is_cp"] = IsControlPoint();
    result["key"] = KeyColumns.DebugJson();
    return result;
}

NKikimr::NArrow::NMerger::TSortableBatchPosition::TFoundPosition TBatchIterator::SkipToLower(const TSortableBatchPosition& pos) {
    const ui32 posStart = KeyColumns.GetPosition();
    auto result = KeyColumns.SkipToLower(pos);
    const i32 delta = IsReverse() ? (posStart - KeyColumns.GetPosition()) : (KeyColumns.GetPosition() - posStart);
    AFL_VERIFY(delta >= 0);
    AFL_VERIFY(VersionColumns.InitPosition(KeyColumns.GetPosition()))("pos", KeyColumns.GetPosition())
        ("size", VersionColumns.GetRecordsCount())("key_size", KeyColumns.GetRecordsCount());
    if (FilterIterator && delta) {
        AFL_VERIFY(FilterIterator->Next(delta));
    }
    return result;
}

bool TBatchIterator::Next() {
    const bool result = KeyColumns.NextPosition(ReverseSortKff) && VersionColumns.NextPosition(ReverseSortKff);
    if (FilterIterator) {
        Y_ABORT_UNLESS(result == FilterIterator->Next(1));
    }
    return result;
}

bool TBatchIterator::operator<(const TBatchIterator& item) const {
    const std::partial_ordering result = KeyColumns.Compare(item.KeyColumns);
    if (result == std::partial_ordering::equivalent) {
        if (IsControlPoint() && item.IsControlPoint()) {
            return false;
        } else if (IsControlPoint()) {
            return false;
        } else if (item.IsControlPoint()) {
            return true;
        }
        //don't need inverse through we need maximal version at first (reverse analytic not included in VersionColumns)
        return VersionColumns.Compare(item.VersionColumns) == std::partial_ordering::less;
    } else {
        //inverse logic through we use max heap, but need minimal element if not reverse (reverse analytic included in KeyColumns)
        return result == std::partial_ordering::greater;
    }
}

}
