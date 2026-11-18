#include "view.h"

#include <ydb/core/formats/arrow/reader/position.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

namespace NKikimr::NArrow {

#ifndef NDEBUG
namespace {

// SET NOT NULL changes the field metadata of existing keys. The row encoding
// stores a presence byte for every field, independent of its nullability.
bool HaveSameKeyFields(const arrow::Schema& lhs, const arrow::Schema& rhs, const ui32 count) {
    for (ui32 i = 0; i < count; ++i) {
        const auto& l = lhs.field(i);
        const auto& r = rhs.field(i);
        if (l->name() != r->name() || !l->type()->Equals(r->type())) {
            return false;
        }
    }

    return true;
}

bool HaveSameKeyFields(const arrow::Schema& lhs, const arrow::Schema& rhs) {
    return &lhs == &rhs || (lhs.num_fields() == rhs.num_fields() && HaveSameKeyFields(lhs, rhs, lhs.num_fields()));
}

} // namespace
#endif

std::partial_ordering TSimpleRowRef::operator<=>(const TSimpleRowRef& item) const {
    AFL_VERIFY_DEBUG(HaveSameKeyFields(Schema, item.Schema));
    return Row.Compare(item.Row, Schema).GetResult();
}

std::partial_ordering TSimpleRow::operator<=>(const TSimpleRow& item) const {
    AFL_VERIFY_DEBUG(HaveSameKeyFields(*Schema, *item.Schema));
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
    YDB_LOG_VERIFY(columnsCount <= GetColumnsCount());
    YDB_LOG_VERIFY(columnsCount <= item.GetColumnsCount());
    AFL_VERIFY_DEBUG(HaveSameKeyFields(*Schema, *item.Schema, columnsCount))("rowSchema", Schema->ToString(true))("itemSchema", item.Schema->ToString(true));
    return TSimpleRowViewV0(Data).Compare(TSimpleRowViewV0(item.Data), *Schema, columnsCount).GetResult();
}

std::partial_ordering TSimpleRow::CompareNotNull(const TSimpleRow& item) const {
    AFL_VERIFY_DEBUG(HaveSameKeyFields(*Schema, *item.Schema))("rowSchema", Schema->ToString(true))("itemSchema", item.Schema->ToString(true));
    YDB_LOG_VERIFY(GetColumnsCount() <= item.GetColumnsCount());
    return TSimpleRowViewV0(Data).Compare(TSimpleRowViewV0(item.Data), *Schema).GetResult();
}

NMerger::TSortableBatchPosition TSimpleRow::BuildSortablePosition(const bool reverse /*= false*/) const {
    return NMerger::TSortableBatchPosition(ToBatch(), 0, reverse);
}

TSimpleRow TSimpleRowContent::Build(const std::shared_ptr<arrow::Schema>& schema) const {
    return TSimpleRow(Data, schema);
}

}   // namespace NKikimr::NArrow
