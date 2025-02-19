#include "columns_storage.h"

#include <ydb/core/formats/arrow/arrow_filter.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {
TColumnsData TColumnsData::Slice(const ui32 offset, const ui32 count) const {
    auto records = Records->Slice(offset, count);
    if (records.GetRecordsCount()) {
        TDictStats::TBuilder builder;
        ui32 idx = 0;
        std::vector<ui32> indexesToRemove;
        for (auto&& i : records.GetColumns()) {
            AFL_VERIFY(Stats.GetColumnName(idx) == records.GetSchema()->field(idx)->name());
            if (i->GetRecordsCount() > i->GetNullsCount()) {
                builder.Add(Stats.GetColumnName(idx), i->GetRecordsCount() - i->GetNullsCount(), i->GetValueRawBytes(), i->GetType());
            } else {
                indexesToRemove.emplace_back(idx);
            }
            ++idx;
        }
        return TColumnsData(builder.Finish(), std::make_shared<TGeneralContainer>(std::move(records)));

    } else {
        return TColumnsData(TDictStats::BuildEmpty(), std::make_shared<TGeneralContainer>(0));
    }
}

TColumnsData TColumnsData::ApplyFilter(const TColumnFilter& filter) const {
    auto records = Records;
    AFL_VERIFY(filter.Apply(records));
    if (records->GetRecordsCount()) {
        TDictStats::TBuilder builder;
        ui32 idx = 0;
        std::vector<ui32> indexesToRemove;
        for (auto&& i : records->GetColumns()) {
            AFL_VERIFY(Stats.GetColumnName(idx) == records->GetSchema()->field(idx)->name());
            if (i->GetRecordsCount() > i->GetNullsCount()) {
                builder.Add(Stats.GetColumnName(idx), i->GetRecordsCount() - i->GetNullsCount(), i->GetValueRawBytes(), i->GetType());
            } else {
                indexesToRemove.emplace_back(idx);
            }
            ++idx;
        }
        records->DeleteFieldsByIndex(indexesToRemove);
        return TColumnsData(builder.Finish(), std::move(records));

    } else {
        return TColumnsData(TDictStats::BuildEmpty(), std::make_shared<TGeneralContainer>(0));
    }
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
