#include "container.h"
#include <ydb/library/formats/arrow/replace_key.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {
std::partial_ordering TPredicateContainer::ComparePredicatesSamePrefix(const NOlap::TPredicate& l, const NOlap::TPredicate& r) {
    Y_ABORT_UNLESS(l.Batch);
    Y_ABORT_UNLESS(r.Batch);
    Y_ABORT_UNLESS(l.Batch->num_columns());
    Y_ABORT_UNLESS(r.Batch->num_columns());
    Y_ABORT_UNLESS(l.Batch->num_rows() == r.Batch->num_rows());
    Y_ABORT_UNLESS(l.Batch->num_rows() == 1);
    const auto commonPrefixLength = std::min(l.Batch->columns().size(), r.Batch->columns().size());
    using NKikimr::NArrow::TRawReplaceKey;
    return TRawReplaceKey{&l.Batch->columns(), 0}.ComparePart<false>(TRawReplaceKey{&r.Batch->columns(), 0}, commonPrefixLength);
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

bool TPredicateContainer::CrossRanges(const TPredicateContainer& ext) const {
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
        } else if (Object->Batch->num_columns() < ext.Object->Batch->num_columns()) {
            return IsInclude();
        } else {
            return ext.IsInclude();
        }
    } else {
        return true;
    }
}

TConclusion<NKikimr::NOlap::TPredicateContainer> TPredicateContainer::BuildPredicateFrom(
    std::shared_ptr<NOlap::TPredicate> object, const std::shared_ptr<arrow::Schema>& pkSchema) {
    if (!object || object->Empty()) {
        return TPredicateContainer(NArrow::ECompareType::GREATER_OR_EQUAL);
    } else {
        if (!object->Good()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "add_range_filter")("problem", "not good 'from' predicate");
            return TConclusionStatus::Fail("not good 'from' predicate");
        }
        if (!object->IsFrom()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "add_range_filter")("problem", "'from' predicate not is from");
            return TConclusionStatus::Fail("'from' predicate not is from");
        }
        if (pkSchema) {
            auto cNames = object->ColumnNames();
            i32 countSortingFields = 0;
            for (i32 i = 0; i < pkSchema->num_fields(); ++i) {
                if (i < (int)cNames.size() && cNames[i] == pkSchema->field(i)->name()) {
                    ++countSortingFields;
                } else {
                    break;
                }
            }
            if (countSortingFields != object->Batch->num_columns()) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "incorrect predicate")("count", countSortingFields)(
                    "object", object->Batch->num_columns())("schema", pkSchema->ToString())(
                    "object", JoinSeq(",", cNames));
                return TConclusionStatus::Fail(
                    "incorrect predicate (not prefix for pk: " + pkSchema->ToString() + " vs " + JoinSeq(",", cNames) + ")");
            }
        }
        return TPredicateContainer(object, pkSchema ? ExtractKey(*object, pkSchema) : nullptr);
    }
}

TConclusion<TPredicateContainer> TPredicateContainer::BuildPredicateTo(
    std::shared_ptr<TPredicate> object, const std::shared_ptr<arrow::Schema>& pkSchema) {
    if (!object || object->Empty()) {
        return TPredicateContainer(NArrow::ECompareType::LESS_OR_EQUAL);
    } else {
        if (!object->Good()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "add_range_filter")("problem", "not good 'to' predicate");
            return TConclusionStatus::Fail("not good 'to' predicate");
        }
        if (!object->IsTo()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "add_range_filter")("problem", "'to' predicate not is to");
            return TConclusionStatus::Fail("'to' predicate not is to");
        }
        if (pkSchema) {
            auto cNames = object->ColumnNames();
            i32 countSortingFields = 0;
            for (i32 i = 0; i < pkSchema->num_fields(); ++i) {
                if (i < (int)cNames.size() && cNames[i] == pkSchema->field(i)->name()) {
                    ++countSortingFields;
                } else {
                    break;
                }
            }
            Y_ABORT_UNLESS(countSortingFields == object->Batch->num_columns());
        }
        return TPredicateContainer(object, pkSchema ? TPredicateContainer::ExtractKey(*object, pkSchema) : nullptr);
    }
}

NArrow::TColumnFilter TPredicateContainer::BuildFilter(const std::shared_ptr<NArrow::TGeneralContainer>& data) const {
    if (!Object) {
        auto result = NArrow::TColumnFilter::BuildAllowFilter();
        result.Add(true, data->GetRecordsCount());
        return result;
    }
    if (!data->GetRecordsCount()) {
        return NArrow::TColumnFilter::BuildAllowFilter();
    }
    auto sortingFields = Object->Batch->schema()->field_names();
    auto position = NArrow::NMerger::TRWSortableBatchPosition(data, 0, sortingFields, {}, false);
    const auto border = NArrow::NMerger::TSortableBatchPosition(Object->Batch, 0, sortingFields, {}, false);
    const bool needUppedBound = CompareType == NArrow::ECompareType::LESS_OR_EQUAL || CompareType == NArrow::ECompareType::GREATER;
    const auto findBound = position.FindBound(position, 0, data->GetRecordsCount() - 1, border, needUppedBound);
    const ui64 rowsBeforeBound = findBound ? findBound->GetPosition() : data->GetRecordsCount();

    auto filter = NArrow::TColumnFilter::BuildAllowFilter();
    switch (CompareType) {
        case NArrow::ECompareType::LESS:
        case NArrow::ECompareType::LESS_OR_EQUAL:
            filter.Add(true, rowsBeforeBound);
            filter.Add(false, data->GetRecordsCount() - rowsBeforeBound);
            break;
        case NArrow::ECompareType::GREATER:
        case NArrow::ECompareType::GREATER_OR_EQUAL:
            filter.Add(false, rowsBeforeBound);
            filter.Add(true, data->GetRecordsCount() - rowsBeforeBound);
            break;
    }
    return filter;
}

}   // namespace NKikimr::NOlap
