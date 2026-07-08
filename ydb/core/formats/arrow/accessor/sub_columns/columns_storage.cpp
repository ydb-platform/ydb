#include "columns_storage.h"

#include <ydb/core/formats/arrow/container/filterable/filterable.h>
#include <ydb/core/formats/arrow/filter/filter.h>

#include <yql/essentials/types/binary_json/read.h>

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
                builder.Add(Stats.GetColumnName(idx), i->GetRecordsCount() - i->GetNullsCount(), i->GetValueRawBytes(), i->GetType(),
                    Stats.GetValueType(idx));
            } else {
                indexesToRemove.emplace_back(idx);
            }
            ++idx;
        }
        records.DeleteFieldsByIndex(indexesToRemove);
        return TColumnsData(builder.Finish(), std::make_shared<TGeneralContainer>(std::move(records)));
    } else {
        return TColumnsData(TDictStats::BuildEmpty(), std::make_shared<TGeneralContainer>(0));
    }
}

TColumnsData TColumnsData::ApplyFilter(const TColumnFilter& filter) const {
    if (!Stats.GetColumnsCount()) {
        return *this;
    }
    auto records = NArrow::ApplyFilter(filter, Records);
    if (records->GetRecordsCount()) {
        TDictStats::TBuilder builder;
        ui32 idx = 0;
        std::vector<ui32> indexesToRemove;
        for (auto&& i : records->GetColumns()) {
            AFL_VERIFY(Stats.GetColumnName(idx) == records->GetSchema()->field(idx)->name());
            if (i->GetRecordsCount() > i->GetNullsCount()) {
                builder.Add(Stats.GetColumnName(idx), i->GetRecordsCount() - i->GetNullsCount(), i->GetValueRawBytes(), i->GetType(),
                    Stats.GetValueType(idx));
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

void TColumnsData::TIterator::InitArrays() {
    while (CurrentIndex < GlobalChunkedArray->GetRecordsCount()) {
        if (!FullArrayAddress || !FullArrayAddress->GetAddress().Contains(CurrentIndex)) {
            FullArrayAddress = GlobalChunkedArray->GetArray(FullArrayAddress, CurrentIndex, GlobalChunkedArray);
            ChunkAddress = std::nullopt;
        }
        const ui32 localIndex = FullArrayAddress->GetAddress().GetLocalIndex(CurrentIndex);
        ChunkAddress = FullArrayAddress->GetArray()->GetChunk(ChunkAddress, localIndex);
        // Physical type is binary (BinaryJson/String), float64 (Double) or boolean (Bool); the value
        // type in the stats tells the reader how to interpret this element.
        CurrentArray = ChunkAddress->GetArray().get();
        // Dictionary columns materialize (decode) to a dense array, so they are
        // read exactly like a plain Array here.
        if (FullArrayAddress->GetArray()->GetType() == IChunkedArray::EType::Array ||
            FullArrayAddress->GetArray()->GetType() == IChunkedArray::EType::Dictionary) {
            if (CurrentArray->IsNull(localIndex)) {
                Next();
            }
            break;
        } else if (FullArrayAddress->GetArray()->GetType() == IChunkedArray::EType::SparsedArray) {
            AFL_VERIFY(localIndex < CurrentArray->length())
                ("localIndex", localIndex)
                ("CurrentArray->length()", CurrentArray->length())
                ("CurrentArray", CurrentArray->ToString());
            if (CurrentArray->IsNull(localIndex) &&
                std::static_pointer_cast<TSparsedArray>(FullArrayAddress->GetArray())->GetDefaultValue() == nullptr) {
                CurrentIndex = ChunkAddress->GetAddress().GetGlobalFinishPosition();
            } else {
                break;
            }
        } else {
            AFL_VERIFY(false)("type", FullArrayAddress->GetArray()->GetType());
        }
    }
    AFL_VERIFY(CurrentIndex <= GlobalChunkedArray->GetRecordsCount())("index", CurrentIndex)("count", GlobalChunkedArray->GetRecordsCount());
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
