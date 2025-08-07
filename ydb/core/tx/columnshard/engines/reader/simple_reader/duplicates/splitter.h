#pragma once

#include "common.h"

#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/formats/arrow/rows/view.h>
#include <ydb/core/formats/arrow/special_keys.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering  {

class TColumnDataSplitter {
public:
    class TBorder {
    private:
        YDB_READONLY_DEF(bool, IsLast);
        NArrow::NMerger::TSortableBatchPosition Key;

        TBorder(const bool isLast, const NArrow::TSimpleRow key)
            : IsLast(isLast)
            , Key(NArrow::NMerger::TSortableBatchPosition(key.ToBatch(), 0, false))
        {
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

        const NArrow::NMerger::TSortableBatchPosition& GetKey() const {
            return Key;
        }

        TString DebugString() const {
            return TStringBuilder() << (IsLast ? "Last:" : "First:") << Key.GetData().DebugJson(0);
        }
    };

private:
    std::vector<TBorder> Borders;
    std::shared_ptr<arrow::Schema> SortingSchema;

public:
    TColumnDataSplitter(const THashMap<ui64, NArrow::TFirstLastSpecialKeys>& sources, const NArrow::TFirstLastSpecialKeys& bounds) {
        AFL_VERIFY(sources.size());
        SortingSchema = sources.begin()->second.GetSchema();

        for (const auto& [id, specials] : sources) {
            AFL_VERIFY(specials.GetSchema()->Equals(SortingSchema))("lhs", specials.GetSchema()->ToString())("rhs", SortingSchema->ToString());
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

    std::vector<TRowRange> SplitPortion(const std::shared_ptr<NArrow::TGeneralContainer>& data) const {
        AFL_VERIFY(!Borders.empty());

        std::vector<ui64> borderOffsets;
        ui64 offset = 0;

        auto position = NArrow::NMerger::TRWSortableBatchPosition(data, 0, SortingSchema->field_names(), {}, false);

        for (const auto& border : Borders) {
            if (offset == data->GetRecordsCount()) {
                borderOffsets.emplace_back(offset);
                continue;
            }
            const auto findBound = NArrow::NMerger::TSortableBatchPosition::FindBound(
                position, offset, data->GetRecordsCount() - 1, border.GetKey(), border.GetIsLast());
            offset = findBound ? findBound->GetPosition() : data->GetRecordsCount();
            borderOffsets.emplace_back(offset);
        }

        std::vector<TRowRange> segments;
        for (ui64 i = 1; i < borderOffsets.size(); ++i) {
            segments.emplace_back(TRowRange(borderOffsets[i - 1], borderOffsets[i]));
        }

        AFL_VERIFY(segments.size() == NumIntervals())("splitted", segments.size())("expected", NumIntervals())("splitter", DebugString());
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
