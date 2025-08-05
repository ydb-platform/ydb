#include "view.h"

#include <ydb/core/formats/arrow/reader/position.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

namespace NKikimr::NArrow {

std::partial_ordering TSimpleRow::operator<=>(const TSimpleRow& item) const {
    AFL_VERIFY_DEBUG(Schema->Equals(*item.Schema));
    return TSimpleRowViewV0(Data).Compare(TSimpleRowViewV0(item.Data), *Schema).GetResult();
}

bool TSimpleRow::operator==(const TSimpleRow& item) const {
    return (*this <=> item) == std::partial_ordering::equivalent;
}

std::shared_ptr<arrow::RecordBatch> TSimpleRow::ToBatch() const {
    auto builders = NArrow::MakeBuilders(Schema);
    AddToBuilders(builders).Validate();
    return arrow::RecordBatch::Make(Schema, 1, FinishBuilders(std::move(builders)));
}

std::partial_ordering TSimpleRow::ComparePartNotNull(const TSimpleRow& item, const ui32 columnsCount) const {
    return GetView().ComparePartNotNull(item.GetView(), columnsCount);
}

std::partial_ordering TSimpleRow::CompareNotNull(const TSimpleRow& item) const {
    return GetView().CompareNotNull(item.GetView());
}

NMerger::TSortableBatchPosition TSimpleRow::BuildSortablePosition(const bool reverse /*= false*/) const {
    return NMerger::TSortableBatchPosition(ToBatch(), 0, reverse);
}

TSimpleRow TSimpleRowContent::Build(const std::shared_ptr<arrow::Schema>& schema) const {
    return TSimpleRow(Data, schema);
}

std::partial_ordering TSimpleRowView::operator<=>(const TSimpleRowView& item) const {
    return CompareNotNull(item);
}

bool TSimpleRowView::operator==(const TSimpleRowView& item) const {
    return (*this <=> item) == std::partial_ordering::equivalent;
}

std::partial_ordering TSimpleRowView::ComparePartNotNull(const TSimpleRowView& item, const ui32 columnsCount) const {
    AFL_VERIFY(columnsCount <= GetColumnsCount());
    AFL_VERIFY(columnsCount <= item.GetColumnsCount());
#ifndef NDEBUG
    for (ui32 i = 0; i < columnsCount; ++i) {
        AFL_VERIFY(Schema->field(i)->type()->Equals(item.Schema->field(i)->type()))("self", Schema->field(i)->ToString())(
            "item", item.Schema->field(i)->ToString());
        AFL_VERIFY(Schema->field(i)->name() == item.Schema->field(i)->name())("self", Schema->field(i)->ToString())(
                                                 "item", item.Schema->field(i)->ToString());
    }
#endif
    return TSimpleRowViewV0(Data).Compare(TSimpleRowViewV0(item.Data), *Schema, columnsCount).GetResult();
}

std::partial_ordering TSimpleRowView::CompareNotNull(const TSimpleRowView& item) const {
    AFL_VERIFY_DEBUG(Schema->Equals(*item.Schema));
    AFL_VERIFY(GetColumnsCount() <= item.GetColumnsCount());
    return TSimpleRowViewV0(Data).Compare(TSimpleRowViewV0(item.Data), *Schema).GetResult();
}

}   // namespace NKikimr::NArrow
