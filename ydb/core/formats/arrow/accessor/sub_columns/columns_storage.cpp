#include "columns_storage.h"

namespace NKikimr::NArrow::NAccessor::NSubColumns {
TColumnsData TColumnsData::Slice(const ui32 offset, const ui32 count) const {
    auto sliceRecords = Records->Slice(offset, count);
    if (sliceRecords.GetRecordsCount()) {
        TDictStats::TBuilder builder;
        ui32 idx = 0;
        for (auto&& i : sliceRecords.GetColumns()) {
            AFL_VERIFY(Stats.GetColumnName(idx) == sliceRecords.GetSchema()->field(idx)->name());
            builder.Add(Stats.GetColumnName(idx), i->GetRecordsCount() - i->GetNullsCount(), i->GetValueRawBytes(), i->GetType());
            ++idx;
        }
        return TColumnsData(builder.Finish(), std::make_shared<TGeneralContainer>(std::move(sliceRecords)));

    } else {
        return TColumnsData(TDictStats::BuildEmpty(), std::make_shared<TGeneralContainer>(0));
    }
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
