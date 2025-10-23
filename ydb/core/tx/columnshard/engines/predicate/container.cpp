#include "container.h"
#include <ydb/library/formats/arrow/replace_key.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {
std::partial_ordering TPredicateContainer::ComparePredicatesSamePrefix(const NOlap::TPredicate& l, const NOlap::TPredicate& r) {
    return l.Batch.ComparePartial(r.Batch);
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
    if (columnIdx >= NumColumns()) {
        return 1;
    }
    AFL_VERIFY(columnIdx < Object->Batch.GetSorting()->GetColumns().size());
    const auto& c = Object->Batch.GetSorting()->GetColumns()[columnIdx];
    auto sPredicate = c->GetScalar(0);
    const int cmpResult = NArrow::ScalarCompare(sPredicate, s);
    if (cmpResult == 0) {
        switch (GetCompareType()) {
            case NArrow::ECompareType::GREATER:
            case NArrow::ECompareType::LESS:
                return -1;
            case NArrow::ECompareType::GREATER_OR_EQUAL:
            case NArrow::ECompareType::LESS_OR_EQUAL:
                return 0;
        }
    } else if (cmpResult == 1) {
        switch (GetCompareType()) {
            case NArrow::ECompareType::GREATER:
            case NArrow::ECompareType::GREATER_OR_EQUAL:
                return -1;
            case NArrow::ECompareType::LESS:
            case NArrow::ECompareType::LESS_OR_EQUAL:
                return 1;
        }

    } else if (cmpResult == -1) {
        switch (GetCompareType()) {
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

std::vector<std::string> TPredicateContainer::GetColumnNames() const {
    if (!Object) {
        return {};
    }
    return Object->Batch.GetSorting()->GetFieldNames();
}

bool TPredicateContainer::IsForwardInterval() const {
    return IsEmpty() || GetCompareType() == NArrow::ECompareType::GREATER_OR_EQUAL || GetCompareType() == NArrow::ECompareType::GREATER;
}

bool TPredicateContainer::IsInclude() const {
    return IsEmpty() || GetCompareType() == NArrow::ECompareType::GREATER_OR_EQUAL || GetCompareType() == NArrow::ECompareType::LESS_OR_EQUAL;
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
        } else if (NumColumns() == ext.NumColumns()) {
            return IsInclude() && ext.IsInclude();
        } else if (NumColumns() < ext.NumColumns()) {
            return IsInclude();
        } else {
            return ext.IsInclude();
        }
    } else {
        return true;
    }
}

TConclusion<NKikimr::NOlap::TPredicateContainer> TPredicateContainer::BuildPredicateFrom(std::optional<TPredicate> object) {
    if (!object) {
        return TPredicateContainer();
    } else {
        if (!object->Good()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "add_range_filter")("problem", "not good 'from' predicate");
            return TConclusionStatus::Fail("not good 'from' predicate");
        }
        if (!object->IsFrom()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "add_range_filter")("problem", "'from' predicate not is from");
            return TConclusionStatus::Fail("'from' predicate not is from");
        }
        return TPredicateContainer(std::move(object));
    }
}

TConclusion<TPredicateContainer> TPredicateContainer::BuildPredicateTo(std::optional<TPredicate> object) {
    if (!object) {
        return TPredicateContainer();
    } else {
        if (!object->Good()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "add_range_filter")("problem", "not good 'to' predicate");
            return TConclusionStatus::Fail("not good 'to' predicate");
        }
        if (!object->IsTo()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "add_range_filter")("problem", "'to' predicate not is to");
            return TConclusionStatus::Fail("'to' predicate not is to");
        }
        return TPredicateContainer(object);
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
    // TODO: data -> SortableBAtchPosition
    auto sortingFields = GetColumnNames();
    auto position = NArrow::NMerger::TRWSortableBatchPosition(data, 0, sortingFields, {}, false);
    const bool needUppedBound = GetCompareType() == NArrow::ECompareType::LESS_OR_EQUAL || GetCompareType() == NArrow::ECompareType::GREATER;
    const auto findBound = position.FindBound(position, 0, data->GetRecordsCount() - 1, Object->Batch, needUppedBound);
    const ui64 rowsBeforeBound = findBound ? findBound->GetPosition() : data->GetRecordsCount();

    auto filter = NArrow::TColumnFilter::BuildAllowFilter();
    switch (GetCompareType()) {
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
