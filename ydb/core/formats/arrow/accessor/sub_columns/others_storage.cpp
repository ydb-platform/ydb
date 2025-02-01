#include "others_storage.h"

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
    if (RecordsCountByKeyIndex.size() <= keyIndex) {
        RecordsCountByKeyIndex.resize(RecordsCountByKeyIndex.size() * 2);
        BytesByKeyIndex.resize(BytesByKeyIndex.size() * 2);
    }
    ++RecordsCountByKeyIndex[keyIndex];
    BytesByKeyIndex[keyIndex] += value.size();
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

TOthersData TOthersData::TBuilderWithStats::Finish(const TDictStats& stats) {
    AFL_VERIFY(Builders.size());
    std::vector<ui32> toRemove;
    for (ui32 i = 0; i < stats.GetColumnsCount(); ++i) {
        if (!RecordsCountByKeyIndex[i]) {
            toRemove.emplace_back(i);
        }
    }
    std::optional<TDictStats> resultStats;
    if (toRemove.size()) {
        auto dictBuilder = TDictStats::MakeBuilder();
        std::vector<ui32> remap;
        ui32 correctIdx = 0;
        for (ui32 i = 0; i < stats.GetColumnsCount(); ++i) {
            if (!RecordsCountByKeyIndex[i]) {
                remap.emplace_back(Max<ui32>());
            } else {
                remap.emplace_back(correctIdx++);
                dictBuilder.Add(stats.GetColumnName(i), RecordsCountByKeyIndex[i], BytesByKeyIndex[i]);
            }
        }
        for (ui32 idx = 0; RTKeyIndexes.size(); ++idx) {
            AFL_VERIFY(RTKeyIndexes[idx] < remap.size());
            RTKeyIndexes[idx] = remap[RTKeyIndexes[idx]];
        }
        resultStats = dictBuilder.Finish();
    } else {
        resultStats = stats;
    }

    for (auto&& i : RTKeyIndexes) {
        TStatusValidator::Validate(KeyIndex->Append(i));
    }
    auto arrays = NArrow::Finish(std::move(Builders));
    return TOthersData(*resultStats, std::make_shared<TGeneralContainer>(arrow::RecordBatch::Make(GetSchema(), RecordsCount, arrays)));
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
