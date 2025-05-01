#include "view.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

namespace NKikimr::NArrow {

std::partial_ordering TSimpleRow::operator<=>(const TSimpleRow& item) const {
    AFL_VERIFY_DEBUG(Schema->Equals(*item.Schema));
    return TSimpleRowViewV0(Data).Compare(TSimpleRowViewV0(item.Data), Schema).GetResult();
}

std::shared_ptr<arrow::RecordBatch> TSimpleRow::ToBatch() const {
    auto builders = NArrow::MakeBuilders(Schema);
    AddToBuilders(builders).Validate();
    return arrow::RecordBatch::Make(Schema, 1, FinishBuilders(std::move(builders)));
}

}   // namespace NKikimr::NArrow
