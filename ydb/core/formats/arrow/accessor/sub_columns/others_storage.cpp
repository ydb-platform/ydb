#include "others_storage.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

TOthersData::TBuilderWithStats::TBuilderWithStats() {
    Builders = NArrow::MakeBuilders(GetSchema());
    AFL_VERIFY(Builders.size() == 3);
    AFL_VERIFY(Builders[0]->type()->id() == arrow::uint32()->id());
    AFL_VERIFY(Builders[1]->type()->id() == arrow::uint32()->id());
    AFL_VERIFY(Builders[2]->type()->id() == arrow::utf8()->id());
    RecordIndex = static_cast<arrow::UInt32Builder*>(Builders[0].get());
    KeyIndex = static_cast<arrow::UInt32Builder*>(Builders[1].get());
    Values = static_cast<arrow::StringBuilder*>(Builders[2].get());
}

void TOthersData::TBuilderWithStats::Add(const ui32 recordIndex, const ui32 keyIndex, const std::string_view value) {
    AFL_VERIFY(Builders.size());
    if (StatsByKeyIndex.size() <= keyIndex) {
        StatsByKeyIndex.resize((keyIndex + 1) * 2);
    }
    StatsByKeyIndex[keyIndex].AddValue(value);
    if (!LastRecordIndex) {
        LastRecordIndex = recordIndex;
        LastKeyIndex = keyIndex;
    } else {
        AFL_VERIFY(*LastRecordIndex < recordIndex || (*LastRecordIndex == recordIndex/* && *LastKeyIndex < keyIndex*/));
    }
    TStatusValidator::Validate(RecordIndex->Append(recordIndex));
    RTKeyIndexes.emplace_back(keyIndex);
    TStatusValidator::Validate(Values->Append(value.data(), value.size()));
    ++RecordsCount;
}

TOthersData TOthersData::TBuilderWithStats::Finish(const TFinishContext& finishContext) {
    AFL_VERIFY(Builders.size());
    auto arrRecordIndex = NArrow::FinishBuilder(std::move(Builders[0]));
    auto arrValues = NArrow::FinishBuilder(std::move(Builders[2]));
    AFL_VERIFY(arrRecordIndex->type()->id() == arrow::uint32()->id());
    auto arrRecordIndexValue = std::static_pointer_cast<arrow::UInt32Array>(arrRecordIndex);
    std::optional<TDictStats> resultStats = finishContext.GetActualStats();
    if (finishContext.GetRemap()) {
        for (ui32 idx = 0; idx < RTKeyIndexes.size(); ++idx) {
            AFL_VERIFY(RTKeyIndexes[idx] < finishContext.GetRemap()->size());
            const ui32 newIndex = (*finishContext.GetRemap())[RTKeyIndexes[idx]];
            AFL_VERIFY(newIndex < finishContext.GetActualStats().GetColumnsCount());
            TStatusValidator::Validate(KeyIndex->Append(newIndex));
            if (idx) {
                const ui32 predKeyIndex = (*finishContext.GetRemap())[RTKeyIndexes[idx - 1]];
                AFL_VERIFY((arrRecordIndexValue->Value(idx - 1) < arrRecordIndexValue->Value(idx)) || 
                (arrRecordIndexValue->Value(idx - 1) == arrRecordIndexValue->Value(idx) && predKeyIndex < newIndex))(
                                                                   "r1", arrRecordIndexValue->Value(idx - 1))(
                                                                   "r2", arrRecordIndexValue->Value(idx))("k1", predKeyIndex)("k2", newIndex);
            }
        }
    } else {
        for (ui32 idx = 0; idx < RTKeyIndexes.size(); ++idx) {
            TStatusValidator::Validate(KeyIndex->Append(RTKeyIndexes[idx]));
            if (idx) {
                AFL_VERIFY((arrRecordIndexValue->Value(idx - 1) < arrRecordIndexValue->Value(idx)) || 
                (arrRecordIndexValue->Value(idx - 1) == arrRecordIndexValue->Value(idx) && RTKeyIndexes[idx - 1] < RTKeyIndexes[idx]))("r1",
                                                                   arrRecordIndexValue->Value(idx - 1))("r2", arrRecordIndexValue->Value(idx))(
                                                                   "k1", RTKeyIndexes[idx - 1])("k2", RTKeyIndexes[idx]);
            }
        }
    }
    auto arrKeyIndexes = NArrow::FinishBuilder(std::move(Builders[1]));
    std::vector<std::shared_ptr<arrow::Array>> arrays = { arrRecordIndex, arrKeyIndexes, arrValues };
    return TOthersData(*resultStats, std::make_shared<TGeneralContainer>(arrow::RecordBatch::Make(GetSchema(), RecordsCount, arrays)));
}

TOthersData TOthersData::Slice(const ui32 offset, const ui32 count, const TSettings& settings) const {
    AFL_VERIFY(Records->GetColumnsCount() == 3);
    if (!count) {
        return TOthersData::BuildEmpty();
    }
    TOthersData::TIterator itOthersData = BuildIterator();
    std::optional<ui32> startPosition = itOthersData.FindPosition(offset);
    std::optional<ui32> finishPosition = itOthersData.FindPosition(offset + count);
    if (!startPosition || startPosition == finishPosition) {
        return TOthersData(TDictStats::BuildEmpty(), std::make_shared<TGeneralContainer>(0));
    }
    std::map<ui32, TDictStats::TRTStats> usedKeys;
    {
        itOthersData.MoveToPosition(*startPosition);
        for (; itOthersData.IsValid() && itOthersData.GetRecordIndex() < offset + count; itOthersData.Next()) {
            auto itUsedKey = usedKeys.find(itOthersData.GetKeyIndex());
            if (itUsedKey == usedKeys.end()) {
                itUsedKey = usedKeys.emplace(itOthersData.GetKeyIndex(), Stats.GetColumnName(itOthersData.GetKeyIndex())).first;
            }
            itUsedKey->second.AddValue(itOthersData.GetValue());
        }
    }
    std::vector<ui32> keyIndexDecoder;
    if (usedKeys.size()) {
        keyIndexDecoder.resize(usedKeys.rbegin()->first + 1, Max<ui32>());
        ui32 idx = 0;
        for (auto&& i : usedKeys) {
            keyIndexDecoder[i.first] = idx++;
        }
    }
    TDictStats::TBuilder statBuilder;
    for (auto&& i : usedKeys) {
        statBuilder.Add(i.second.GetKeyName(), i.second.GetRecordsCount(), i.second.GetDataSize(), i.second.GetAccessorType(settings, count));
    }
    TDictStats sliceStats = statBuilder.Finish();

    {
        auto recordIndexBuilder = NArrow::MakeBuilder(arrow::uint32());
        auto keyIndexBuilder = NArrow::MakeBuilder(arrow::uint32());
        itOthersData.MoveToPosition(*startPosition);
        for (; itOthersData.IsValid() && itOthersData.GetRecordIndex() < offset + count; itOthersData.Next()) {
            NArrow::Append<arrow::UInt32Type>(*recordIndexBuilder, itOthersData.GetRecordIndex() - offset);
            AFL_VERIFY(itOthersData.GetKeyIndex() < keyIndexDecoder.size());
            const ui32 newKeyIndex = keyIndexDecoder[itOthersData.GetKeyIndex()];
            AFL_VERIFY(newKeyIndex < sliceStats.GetColumnsCount());
            NArrow::Append<arrow::UInt32Type>(*keyIndexBuilder, keyIndexDecoder[itOthersData.GetKeyIndex()]);
        }
        auto recordIndexes = NArrow::FinishBuilder(std::move(recordIndexBuilder));
        auto keyIndexes = NArrow::FinishBuilder(std::move(keyIndexBuilder));
        std::vector<std::shared_ptr<IChunkedArray>> arrays = { std::make_shared<TTrivialArray>(recordIndexes),
            std::make_shared<TTrivialArray>(keyIndexes),
            GetValuesArray()->ISlice(*startPosition, finishPosition.value_or(GetValuesArray()->GetRecordsCount()) - *startPosition) };
        auto sliceRecords = std::make_shared<TGeneralContainer>(GetSchema(), std::move(arrays));
        return TOthersData(sliceStats, sliceRecords);
    }
}

TOthersData TOthersData::BuildEmpty() {
    static TOthersData result = []() {
        auto records = std::make_shared<TGeneralContainer>(0);
        for (auto&& f : TOthersData::GetSchema()->fields()) {
            records->AddField(f, NArrow::TThreadSimpleArraysCache::GetNull(f->type(), 0)).Validate();
        }
        return TOthersData(TDictStats::BuildEmpty(), records);
    }();
    return result;
}

std::shared_ptr<IChunkedArray> TOthersData::GetPathAccessor(const std::string_view path, const ui32 recordsCount) const {
    auto idx = Stats.GetKeyIndexOptional(path);
    if (!idx) {
        return std::make_shared<TTrivialArray>(TThreadSimpleArraysCache::GetNull(arrow::utf8(), recordsCount));
    }
    TColumnFilter filter = TColumnFilter::BuildAllowFilter();
    for (TIterator it(Records); it.IsValid(); it.Next()) {
        filter.Add(it.GetKeyIndex() == *idx);
    }
    auto recordsFiltered = Records;
    AFL_VERIFY(filter.Apply(recordsFiltered));
    auto table = recordsFiltered->BuildTableVerified(std::set<std::string>({ "record_idx", "value" }));

    TSparsedArray::TBuilder builder(nullptr, arrow::utf8());
    auto batch = ToBatch(table);
    builder.AddChunk(recordsCount, batch->GetColumnByName("record_idx"), batch->GetColumnByName("value"));
    return builder.Finish();
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
