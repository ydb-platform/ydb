#include "columns_storage.h"

namespace NKikimr::NArrow::NAccessor::NSubColumns {
TColumnsData TColumnsData::Slice(const ui32 offset, const ui32 count) const {
    auto sliceRecords = Records->Slice(offset, count);
    TDictStats::TBuilder builder;
    ui32 idx = 0;
    for (auto&& i : Records->GetColumns()) {
        AFL_VERIFY(Stats.GetColumnName(idx) == Records->GetSchema()->field(idx)->name());
        builder.Add(Stats.GetColumnName(idx), i->GetRecordsCount() - i->GetNullsCount(), i->GetValueRawBytes());
        ++idx;
    }
    return TColumnsData(builder.Finish(), std::make_shared<TGeneralContainer>(std::move(sliceRecords)));
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
