#pragma once

#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/formats/arrow/rows/view.h>
#include <ydb/core/tx/columnshard/column_fetching/cache_policy.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/range_treap/range_treap.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

struct TPortionIntervalTreeValueTraits: NRangeTreap::TDefaultValueTraits<std::shared_ptr<TPortionInfo>> {
    struct TValueHash {
        ui64 operator()(const std::shared_ptr<TPortionInfo>& value) const {
            return THash<TPortionAddress>()(value->GetAddress());
        }
    };

    static bool Less(const std::shared_ptr<TPortionInfo>& a, const std::shared_ptr<TPortionInfo>& b) noexcept {
        return a->GetAddress() < b->GetAddress();
    }

    static bool Equal(const std::shared_ptr<TPortionInfo>& a, const std::shared_ptr<TPortionInfo>& b) noexcept {
        return a->GetAddress() == b->GetAddress();
    }
};

using TPortionIntervalTree =
    NRangeTreap::TRangeTreap<NArrow::TSimpleRow, std::shared_ptr<TPortionInfo>, NArrow::TSimpleRow, TPortionIntervalTreeValueTraits>;

class TRowRange {
private:
    YDB_READONLY_DEF(ui64, Begin);
    YDB_READONLY_DEF(ui64, End);

public:
    TRowRange(const ui64 begin, const ui64 end)
        : Begin(begin)
        , End(end)
    {
        AFL_VERIFY(end >= begin);
    }

    std::partial_ordering operator<=>(const TRowRange& other) const {
        return std::tie(Begin, End) <=> std::tie(other.Begin, other.End);
    }
    bool operator==(const TRowRange& other) const {
        return (*this <=> other) == std::partial_ordering::equivalent;
    }

    ui64 NumRows() const {
        return End - Begin;
    }

    operator size_t() const {
        return CombineHashes(Begin, End);
    }

    TString DebugString() const {
        return TStringBuilder() << "[" << Begin << ";" << End << ")";
    }
};

class TDuplicateMapInfo {
private:
    TSnapshot MaxVersion;
    TRowRange Rows;
    YDB_READONLY_DEF(ui64, SourceId);

public:
    TDuplicateMapInfo(const TSnapshot& maxVersion, const TRowRange& rows, const ui64 sourceId)
        : MaxVersion(maxVersion)
        , Rows(rows)
        , SourceId(sourceId)
    {
    }

    operator size_t() const {
        size_t h = (size_t)MaxVersion;
        h = CombineHashes(h, (size_t)Rows);
        h = CombineHashes(h, SourceId);
        return h;
    }
    bool operator==(const TDuplicateMapInfo& other) const {
        return std::tie(MaxVersion, Rows, SourceId) == std::tie(other.MaxVersion, other.Rows, other.SourceId);
    }

    TString DebugString() const {
        return TStringBuilder() << "MaxVersion=" << MaxVersion.DebugString() << ";Rows=" << Rows.DebugString() << ";SourceId=" << SourceId;
    }

    const TRowRange& GetRows() const {
        return Rows;
    }
};

class TDuplicateSourceCacheResult {
private:
    std::map<ui32, std::shared_ptr<arrow::Field>> FieldByColumnId;
    using TColumnData = THashMap<NGeneralCache::TGlobalColumnAddress, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>;
    TColumnData DataByAddress;

public:
    TDuplicateSourceCacheResult(TColumnData&& data, const std::map<ui32, std::shared_ptr<arrow::Field>>& fieldByColumnId)
        : FieldByColumnId(fieldByColumnId)
        , DataByAddress(std::move(data))
    {
    }

    THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>> ExtractDataByPortion() {
        THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>> dataByPortion;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (const auto& [_, field] : FieldByColumnId) {
            fields.emplace_back(field);
        }

        THashMap<ui64, THashMap<ui32, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>> columnsByPortion;
        for (auto&& [address, data] : DataByAddress) {
            AFL_VERIFY(columnsByPortion[address.GetPortionId()].emplace(address.GetColumnId(), data).second);
        }

        for (auto& [portion, columns] : columnsByPortion) {
            std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> sortedColumns;
            for (const auto& [columnId, _] : FieldByColumnId) {
                auto column = columns.FindPtr(columnId);
                AFL_VERIFY(column);
                sortedColumns.emplace_back(*column);
            }
            std::shared_ptr<NArrow::TGeneralContainer> container = std::make_shared<NArrow::TGeneralContainer>(fields, std::move(sortedColumns));
            AFL_VERIFY(dataByPortion.emplace(portion, std::move(container)).second);
        }

        return dataByPortion;
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
