#include "accessor.h"
#include "columns_storage.h"
#include "direct_builder.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

void TColumnElements::BuildSparsedAccessor(const ui32 recordsCount) {
    AFL_VERIFY(!Accessor);
    auto recordsBuilder = TSparsedArray::MakeBuilderUtf8(RecordIndexes.size(), DataSize);
    for (ui32 idx = 0; idx < RecordIndexes.size(); ++idx) {
        recordsBuilder.AddRecord(RecordIndexes[idx], Values[idx]);
    }
    Accessor = recordsBuilder.Finish(recordsCount);
}

void TColumnElements::BuildPlainAccessor(const ui32 recordsCount) {
    AFL_VERIFY(!Accessor);
    auto builder = TTrivialArray::MakeBuilderUtf8(recordsCount, DataSize);
    for (auto it = RecordIndexes.begin(); it != RecordIndexes.end(); ++it) {
        builder.AddRecord(*it, Values[it - RecordIndexes.begin()]);
    }
    Accessor = builder.Finish(recordsCount);
}

std::shared_ptr<TSubColumnsArray> TDataBuilder::Finish() {
    std::map<ui64, std::vector<TColumnElements*>> elementsBySize;
    for (auto&& i : Elements) {
        elementsBySize[i.second.GetDataSize()].emplace_back(&i.second);
    }
    ui32 columnAccessorsCount = 0;
    std::vector<TColumnElements*> columnElements;
    std::vector<TColumnElements*> otherElements;
    for (auto rIt = elementsBySize.rbegin(); rIt != elementsBySize.rend(); ++rIt) {
        for (auto&& i : rIt->second) {
            if (columnAccessorsCount < TSettings::ColumnAccessorsCountLimit) {
                columnElements.emplace_back(i);
                if (TSettings::IsSparsed(i->GetRecordIndexes().size(), CurrentRecordIndex)) {
                    i->BuildSparsedAccessor(CurrentRecordIndex);
                } else {
                    i->BuildPlainAccessor(CurrentRecordIndex);
                }
                ++columnAccessorsCount;
            } else {
                otherElements.emplace_back(i);
            }
        }
    }
    const auto predSortElements = [](const TColumnElements* l, const TColumnElements* r) {
        return l->GetKeyName() < r->GetKeyName();
    };
    std::sort(columnElements.begin(), columnElements.end(), predSortElements);
    std::sort(otherElements.begin(), otherElements.end(), predSortElements);

    TOthersData rbOthers = MergeOthers(otherElements);

    TDictStats columnStats = BuildStats(columnElements);
    auto records = std::make_shared<TGeneralContainer>(CurrentRecordIndex);
    for (auto&& i : columnElements) {
        records->AddField(std::make_shared<arrow::Field>(std::string(i->GetKeyName()), arrow::utf8()), i->GetAccessorVerified()).Validate();
    }
    TColumnsData cData(std::move(columnStats), std::move(records));
    return std::make_shared<TSubColumnsArray>(std::move(cData), std::move(rbOthers), Type, CurrentRecordIndex);
}

TOthersData TDataBuilder::MergeOthers(const std::vector<TColumnElements*>& otherKeys) const {
    std::vector<THeapElements> heap;
    ui32 idx = 0;
    for (auto&& i : otherKeys) {
        heap.emplace_back(i, idx);
        AFL_VERIFY(heap.back().IsValid());
        ++idx;
    }
    std::make_heap(heap.begin(), heap.end());
    auto othersBuilder = TOthersData::MakeMergedBuilder();
    while (heap.size()) {
        std::pop_heap(heap.begin(), heap.end());
        othersBuilder->Add(heap.back().GetRecordIndex(), heap.back().GetKeyIndex(), heap.back().GetValue());
        if (!heap.back().Next()) {
            heap.pop_back();
        } else {
            std::push_heap(heap.begin(), heap.end());
        }
    }
    return othersBuilder->Finish(BuildStats(otherKeys));
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
