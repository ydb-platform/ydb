#include "others_storage.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <ydb/library/formats/arrow/arrow_helpers.h>

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
        AFL_VERIFY(*LastRecordIndex < recordIndex || (*LastRecordIndex == recordIndex && *LastKeyIndex < keyIndex));
    }
    TStatusValidator::Validate(RecordIndex->Append(recordIndex));
    RTKeyIndexes.emplace_back(keyIndex);
    TStatusValidator::Validate(Values->Append(value.data(), value.size()));
    ++RecordsCount;
}

TOthersData TOthersData::TBuilderWithStats::Finish(const TFinishContext& finishContext) {
    AFL_VERIFY(Builders.size());
    std::optional<TDictStats> resultStats = finishContext.GetActualStats();
    if (finishContext.GetRemap()) {
        for (ui32 idx = 0; idx < RTKeyIndexes.size(); ++idx) {
            AFL_VERIFY(RTKeyIndexes[idx] < finishContext.GetRemap()->size());
            const ui32 newIndex = (*finishContext.GetRemap())[RTKeyIndexes[idx]];
            AFL_VERIFY(newIndex < finishContext.GetActualStats().GetColumnsCount());
            TStatusValidator::Validate(KeyIndex->Append(newIndex));
        }
    } else {
        for (ui32 idx = 0; idx < RTKeyIndexes.size(); ++idx) {
            TStatusValidator::Validate(KeyIndex->Append(RTKeyIndexes[idx]));
        }
    }
    auto arrays = NArrow::Finish(std::move(Builders));
    return TOthersData(*resultStats, std::make_shared<TGeneralContainer>(arrow::RecordBatch::Make(GetSchema(), RecordsCount, arrays)));
}

TOthersData TOthersData::Slice(const ui32 offset, const ui32 count) const {
    AFL_VERIFY(Records->GetColumnsCount() == 3);
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
    std::vector<TDictStats::TRTStats> statKeys;
    TDictStats::TBuilder statBuilder;
    for (auto&& i : usedKeys) {
        statBuilder.Add(i.second.GetKeyName(), i.second.GetRecordsCount(), i.second.GetDataSize());
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
            std::make_shared<TTrivialArray>(keyIndexes), GetValuesArray()->ISlice(*startPosition, *finishPosition - *startPosition) };
        auto sliceRecords = std::make_shared<TGeneralContainer>(GetSchema(), std::move(arrays));
        return TOthersData(sliceStats, sliceRecords);
    }
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
