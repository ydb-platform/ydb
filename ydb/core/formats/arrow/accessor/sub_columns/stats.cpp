#include "settings.h"
#include "stats.h"

#include <ydb/core/formats/arrow/accessor/plain/constructor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/constructor.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>

#include <ydb/library/formats/arrow/arrow_helpers.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {
TSplittedColumns TDictStats::SplitByVolume(const ui32 columnsLimit) const {
    std::map<ui64, std::vector<TRTStats>> bySize;
    for (ui32 i = 0; i < GetColumnsCount(); ++i) {
        bySize[GetColumnSize(i)].emplace_back(GetRTStats(i));
    }
    std::vector<TRTStats> columnStats;
    std::vector<TRTStats> otherStats;
    for (auto it = bySize.rbegin(); it != bySize.rend(); ++it) {
        for (auto&& i : it->second) {
            if (columnStats.size() < columnsLimit) {
                columnStats.emplace_back(std::move(i));
            } else {
                otherStats.emplace_back(std::move(i));
            }
        }
    }
    std::sort(columnStats.begin(), columnStats.end());
    std::sort(otherStats.begin(), otherStats.end());
    auto columnsBuilder = MakeBuilder();
    auto othersBuilder = MakeBuilder();
    for (auto&& i : columnStats) {
        columnsBuilder.Add(i.GetKeyName(), i.GetRecordsCount(), i.GetDataSize());
    }
    for (auto&& i : otherStats) {
        othersBuilder.Add(i.GetKeyName(), i.GetRecordsCount(), i.GetDataSize());
    }
    return TSplittedColumns(columnsBuilder.Finish(), othersBuilder.Finish());
}

TDictStats TDictStats::Merge(const std::vector<const TDictStats*>& stats) {
    std::map<std::string_view, TRTStats> resultMap;
    for (auto&& i : stats) {
        for (ui32 idx = 0; idx < i->GetColumnsCount(); ++idx) {
            auto it = resultMap.find(i->GetColumnName(idx));
            if (it == resultMap.end()) {
                it = resultMap.emplace(i->GetColumnName(idx), TRTStats(i->GetColumnName(idx))).first;
            }
            it->second.Add(*i, idx);
        }
    }
    auto builder = MakeBuilder();
    for (auto&& i : resultMap) {
        builder.Add(i.second.GetKeyName(), i.second.GetRecordsCount(), i.second.GetDataSize());
    }
    return builder.Finish();
}

ui32 TDictStats::GetColumnRecordsCount(const ui32 index) const {
    AFL_VERIFY(index < DataRecordsCount->length());
    return DataRecordsCount->Value(index);
}

ui32 TDictStats::GetColumnSize(const ui32 index) const {
    AFL_VERIFY(index < DataSize->length());
    return DataSize->Value(index);
}

std::string_view TDictStats::GetColumnName(const ui32 index) const {
    AFL_VERIFY(index < DataNames->length());
    auto view = DataNames->GetView(index);
    return std::string_view(view.data(), view.size());
}

TDictStats::TDictStats(const std::shared_ptr<arrow::RecordBatch>& original)
    : Original(original) {
    AFL_VERIFY(Original->num_columns() == 3)("count", Original->num_columns());
    AFL_VERIFY(Original->column(0)->type()->id() == arrow::utf8()->id());
    AFL_VERIFY(Original->column(1)->type()->id() == arrow::uint32()->id());
    AFL_VERIFY(Original->column(2)->type()->id() == arrow::uint32()->id());
    DataNames = std::static_pointer_cast<arrow::StringArray>(Original->column(0));
    DataRecordsCount = std::static_pointer_cast<arrow::UInt32Array>(Original->column(1));
    DataSize = std::static_pointer_cast<arrow::UInt32Array>(Original->column(2));
}

bool TDictStats::IsSparsed(const ui32 columnIndex, const ui32 recordsCount) const {
    return TSettings::IsSparsed(GetColumnRecordsCount(columnIndex), recordsCount);
}

TConstructorContainer TDictStats::GetAccessorConstructor(const ui32 columnIndex, const ui32 recordsCount) const {
    if (IsSparsed(columnIndex, recordsCount)) {
        return std::make_shared<NAccessor::NSparsed::TConstructor>();
    } else {
        return std::make_shared<NAccessor::NPlain::TConstructor>();
    }
}

TDictStats TDictStats::BuildEmpty() {
    return TDictStats(MakeEmptyBatch(GetStatsSchema()));
}

TString TDictStats::SerializeAsString(const std::shared_ptr<NSerialization::ISerializer>& serializer) const {
    AFL_VERIFY(serializer);
    return serializer->SerializePayload(Original);
}

TDictStats::TBuilder::TBuilder() {
    Builders = NArrow::MakeBuilders(GetStatsSchema());
    AFL_VERIFY(Builders.size() == 3);
    AFL_VERIFY(Builders[0]->type()->id() == arrow::utf8()->id());
    AFL_VERIFY(Builders[1]->type()->id() == arrow::uint32()->id());
    AFL_VERIFY(Builders[2]->type()->id() == arrow::uint32()->id());
    Names = static_cast<arrow::StringBuilder*>(Builders[0].get());
    Records = static_cast<arrow::UInt32Builder*>(Builders[1].get());
    DataSize = static_cast<arrow::UInt32Builder*>(Builders[2].get());
}

void TDictStats::TBuilder::Add(const TString& name, const ui32 recordsCount, const ui32 dataSize) {
    AFL_VERIFY(Builders.size());
    if (!LastKeyName) {
        LastKeyName = name;
    } else {
        AFL_VERIFY(*LastKeyName < name);
    }
    AFL_VERIFY(recordsCount);
    AFL_VERIFY(dataSize);
    TStatusValidator::Validate(Names->Append(name.data(), name.size()));
    TStatusValidator::Validate(Records->Append(recordsCount));
    TStatusValidator::Validate(DataSize->Append(dataSize));
    ++RecordsCount;
}

void TDictStats::TBuilder::Add(const std::string_view name, const ui32 recordsCount, const ui32 dataSize) {
    Add(TString(name.data(), name.size()), recordsCount, dataSize);
}

TDictStats TDictStats::TBuilder::Finish() {
    AFL_VERIFY(Builders.size());
    auto arrays = NArrow::Finish(std::move(Builders));
    return TDictStats(arrow::RecordBatch::Make(GetStatsSchema(), RecordsCount, arrays));
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
