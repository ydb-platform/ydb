#include "container.h"
#include <ydb/core/tx/columnshard/engines/index_info.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {
std::partial_ordering TPredicateContainer::ComparePredicatesSamePrefix(const NOlap::TPredicate& l, const NOlap::TPredicate& r) {
    Y_ABORT_UNLESS(l.Batch);
    Y_ABORT_UNLESS(r.Batch);
    Y_ABORT_UNLESS(l.Batch->num_columns());
    Y_ABORT_UNLESS(r.Batch->num_columns());
    Y_ABORT_UNLESS(l.Batch->num_rows() == r.Batch->num_rows());
    Y_ABORT_UNLESS(l.Batch->num_rows() == 1);
    std::vector<std::shared_ptr<arrow::Array>> lColumns;
    std::vector<std::shared_ptr<arrow::Array>> rColumns;
    for (ui32 i = 0; i < std::min(l.Batch->columns().size(), r.Batch->columns().size()); ++i) {
        Y_ABORT_UNLESS(l.Batch->column_name(i) == r.Batch->column_name(i));
        lColumns.emplace_back(l.Batch->column(i));
        rColumns.emplace_back(r.Batch->column(i));
    }
    return NArrow::ColumnsCompare(lColumns, 0, rColumns, 0);
}

TString TPredicateContainer::DebugString() const {
    if (!Object) {
        return IsForwardInterval() ? "+Inf" : "-Inf";
    } else {
        return TStringBuilder() << *Object;
    }
}

int TPredicateContainer::MatchScalar(const ui32 columnIdx, const std::shared_ptr<arrow::Scalar>& s) const {
    if (!Object) {
        return 1;
    }
    if (!s) {
        return 1;
    }
    if ((int)columnIdx >= Object->Batch->num_columns()) {
        return 1;
    }
    auto c = Object->Batch->column(columnIdx);
    Y_ABORT_UNLESS(c);
    auto sPredicate = c->GetScalar(0);
    Y_ABORT_UNLESS(sPredicate.ok());
    const int cmpResult = NArrow::ScalarCompare(*sPredicate, s);
    if (cmpResult == 0) {
        switch (CompareType) {
            case NArrow::ECompareType::GREATER:
            case NArrow::ECompareType::LESS:
                return -1;
            case NArrow::ECompareType::GREATER_OR_EQUAL:
            case NArrow::ECompareType::LESS_OR_EQUAL:
                return 0;
        }
    } else if (cmpResult == 1) {
        switch (CompareType) {
            case NArrow::ECompareType::GREATER:
            case NArrow::ECompareType::GREATER_OR_EQUAL:
                return -1;
            case NArrow::ECompareType::LESS:
            case NArrow::ECompareType::LESS_OR_EQUAL:
                return 1;
        }

    } else if (cmpResult == -1) {
        switch (CompareType) {
            case NArrow::ECompareType::GREATER:
            case NArrow::ECompareType::GREATER_OR_EQUAL:
                return 1;
            case NArrow::ECompareType::LESS:
            case NArrow::ECompareType::LESS_OR_EQUAL:
                return -1;
        }
    } else {
        Y_ABORT_UNLESS(false);
    }
}

const std::vector<TString>& TPredicateContainer::GetColumnNames() const {
    if (!ColumnNames) {
        if (Object) {
            ColumnNames = Object->ColumnNames();
        } else {
            ColumnNames = std::vector<TString>();
        }
    }
    return *ColumnNames;
}

bool TPredicateContainer::IsForwardInterval() const {
    return CompareType == NArrow::ECompareType::GREATER_OR_EQUAL || CompareType == NArrow::ECompareType::GREATER;
}

bool TPredicateContainer::IsInclude() const {
    return CompareType == NArrow::ECompareType::GREATER_OR_EQUAL || CompareType == NArrow::ECompareType::LESS_OR_EQUAL;
}

bool TPredicateContainer::CrossRanges(const TPredicateContainer& ext) {
    if (Object && ext.Object) {
        if (IsForwardInterval() == ext.IsForwardInterval()) {
            return true;
        }
        const std::partial_ordering result = ComparePredicatesSamePrefix(*Object, *ext.Object);
        if (result == std::partial_ordering::less) {
            return IsForwardInterval();
        } else if (result == std::partial_ordering::greater) {
            return ext.IsForwardInterval();
        } else if (Object->Batch->num_columns() == ext.Object->Batch->num_columns()) {
            return IsInclude() && ext.IsInclude();
        } else {
            return true;
        }
    } else {
        return true;
    }
}

std::optional<NKikimr::NOlap::TPredicateContainer> TPredicateContainer::BuildPredicateFrom(std::shared_ptr<NOlap::TPredicate> object, const TIndexInfo* indexInfo) {
    if (!object || object->Empty()) {
        return TPredicateContainer(NArrow::ECompareType::GREATER_OR_EQUAL);
    } else {
        if (!object->Good()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "add_range_filter")("problem", "not good 'from' predicate");
            return {};
        }
        if (!object->IsFrom()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "add_range_filter")("problem", "'from' predicate not is from");
            return {};
        }
        if (indexInfo) {
            auto cNames = object->ColumnNames();
            i32 countSortingFields = 0;
            for (i32 i = 0; i < indexInfo->GetReplaceKey()->num_fields(); ++i) {
                if (i < (int)cNames.size() && cNames[i] == indexInfo->GetReplaceKey()->field(i)->name()) {
                    ++countSortingFields;
                } else {
                    break;
                }
            }
            Y_ABORT_UNLESS(countSortingFields == object->Batch->num_columns());
        }
        return TPredicateContainer(object, ExtractKey(*object, indexInfo->GetReplaceKey()));
    }
}

std::optional<NKikimr::NOlap::TPredicateContainer> TPredicateContainer::BuildPredicateTo(std::shared_ptr<NOlap::TPredicate> object, const TIndexInfo* indexInfo) {
    if (!object || object->Empty()) {
        return TPredicateContainer(NArrow::ECompareType::LESS_OR_EQUAL);
    } else {
        if (!object->Good()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "add_range_filter")("problem", "not good 'to' predicate");
            return {};
        }
        if (!object->IsTo()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "add_range_filter")("problem", "'to' predicate not is to");
            return {};
        }
        if (indexInfo) {
            auto cNames = object->ColumnNames();
            i32 countSortingFields = 0;
            for (i32 i = 0; i < indexInfo->GetReplaceKey()->num_fields(); ++i) {
                if (i < (int)cNames.size() && cNames[i] == indexInfo->GetReplaceKey()->field(i)->name()) {
                    ++countSortingFields;
                } else {
                    break;
                }
            }
            Y_ABORT_UNLESS(countSortingFields == object->Batch->num_columns());
        }
        return TPredicateContainer(object, TPredicateContainer::ExtractKey(*object, indexInfo->GetReplaceKey()));
    }
}

}
