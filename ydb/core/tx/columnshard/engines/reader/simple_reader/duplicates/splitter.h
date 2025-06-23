#pragma once

#include "events.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering  {

class TDuplicateMapInfo {
private:
    TSnapshot MaxVersion;
    YDB_READONLY_DEF(ui64, Offset);
    YDB_READONLY_DEF(ui64, RowsCount);
    ui64 SourceId;

public:
    TDuplicateMapInfo(const TSnapshot& maxVersion, const ui64 offset, const ui64 rowsCount, const ui64 sourceId)
        : MaxVersion(maxVersion)
        , Offset(offset)
        , RowsCount(rowsCount)
        , SourceId(sourceId) {
    }

    operator size_t() const {
        ui64 h = 0;
        h = CombineHashes(h, (size_t)MaxVersion);
        h = CombineHashes(h, Offset);
        h = CombineHashes(h, RowsCount);
        h = CombineHashes(h, SourceId);
        return h;
    }
    bool operator==(const TDuplicateMapInfo& other) const {
        return std::tie(MaxVersion, Offset, RowsCount, SourceId) == std::tie(other.MaxVersion, other.Offset, other.RowsCount, other.SourceId);
    }

    TString DebugString() const {
        return TStringBuilder() << "MaxVersion=" << MaxVersion.DebugString() << ";Offset=" << Offset << ";RowsCount=" << RowsCount << ";SourceId"
                                << SourceId;
    }
};

class TColumnDataSplitter {
public:
    class TBorder {
    private:
        YDB_READONLY_DEF(bool, IsLast);
        NArrow::TSimpleRow Key;

        TBorder(const bool isLast, const NArrow::TSimpleRow key)
            : IsLast(isLast)
            , Key(key) {
        }

    public:
        static TBorder First(NArrow::TSimpleRow&& key) {
            return TBorder(false, std::move(key));
        }
        static TBorder Last(NArrow::TSimpleRow&& key) {
            return TBorder(true, std::move(key));
        }

        std::partial_ordering operator<=>(const TBorder& other) const {
            return std::tie(Key, IsLast) <=> std::tie(other.Key, other.IsLast);
        };
        bool operator==(const TBorder& other) const {
            return (*this <=> other) == std::partial_ordering::equivalent;
        };

        const NArrow::TSimpleRow& GetKey() const {
            return Key;
        }

        TString DebugString() const {
            return TStringBuilder() << (IsLast ? "Last:" : "First:") << Key.DebugString();
        }
    };

private:
    std::vector<TBorder> Borders;

public:
    TColumnDataSplitter(THashMap<ui64, NArrow::TFirstLastSpecialKeys> sources, const NArrow::TFirstLastSpecialKeys& bounds) {
        for (const auto& [id, specials] : sources) {
            if (specials.GetFirst() > bounds.GetFirst()) {
                Borders.emplace_back(TBorder::First(specials.GetFirst()));
            }
            if (specials.GetLast() < bounds.GetLast()) {
                Borders.emplace_back(TBorder::Last(specials.GetLast()));
            }
        }
        Borders.emplace_back(TBorder::First(bounds.GetFirst()));
        Borders.emplace_back(TBorder::Last(bounds.GetLast()));

        std::sort(Borders.begin(), Borders.end());
        Borders.erase(std::unique(Borders.begin(), Borders.end()), Borders.end());

        AFL_VERIFY(NumIntervals());
    }

    ui64 NumIntervals() const {
        AFL_VERIFY(!Borders.empty());
        return Borders.size() - 1;
    }

    const TBorder& GetIntervalFinish(const ui64 intervalIdx) const {
        AFL_VERIFY(intervalIdx < NumIntervals());
        return Borders[intervalIdx + 1];
    }

    std::vector<TDuplicateMapInfo> SplitPortion(
        const std::shared_ptr<TColumnsData>& data, const ui64 sourceId, const TSnapshot& maxVersion) const {
        AFL_VERIFY(!Borders.empty());

        std::vector<ui64> borderOffsets;
        ui64 offset = 0;

        auto sortingFields = Borders.front().GetKey().ToBatch()->schema()->field_names();
        auto position = NArrow::NMerger::TRWSortableBatchPosition(data->GetData(), 0, sortingFields, {}, false);

        for (const auto& border : Borders) {
            const auto borderPosition = NArrow::NMerger::TSortableBatchPosition(border.GetKey().ToBatch(), 0, sortingFields, {}, false);
            if (offset == data->GetData()->GetRecordsCount()) {
                borderOffsets.emplace_back(offset);
                continue;
            }
            const auto findBound = NArrow::NMerger::TSortableBatchPosition::FindBound(
                position, offset, data->GetData()->GetRecordsCount() - 1, borderPosition, border.GetIsLast());
            offset = findBound ? findBound->GetPosition() : data->GetData()->GetRecordsCount();
            borderOffsets.emplace_back(offset);
        }

        std::vector<TDuplicateMapInfo> segments;
        for (ui64 i = 1; i < borderOffsets.size(); ++i) {
            segments.emplace_back(TDuplicateMapInfo(maxVersion, borderOffsets[i - 1], borderOffsets[i] - borderOffsets[i - 1], sourceId));
        }

        return segments;
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "[";
        for (const auto& border : Borders) {
            sb << border.DebugString() << ";";
        }
        sb << "]";
        return sb;
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
