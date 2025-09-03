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
        YDB_READONLY_DEF(ui64, PortionId);

        TBorder(const bool isLast, const NArrow::TSimpleRow key, const ui64 portionId)
            : IsLast(isLast)
            , Key(NArrow::NMerger::TSortableBatchPosition(key.ToBatch(), 0, false))
            , PortionId(portionId)
        {
        }

    public:
        static TBorder First(NArrow::TSimpleRow&& key, const ui64 portionId) {
            return TBorder(false, std::move(key), portionId);
        }
        static TBorder Last(NArrow::TSimpleRow&& key, const ui64 portionId) {
            return TBorder(true, std::move(key), portionId);
        }
        TBorder(const TPortionBorderView& border, const TPortionInfo::TConstPtr& source)
            : TBorder(border.IsLast(), border.GetIndexKey(*source), source->GetPortionId())
        {
        }

        bool operator<(const TBorder& other) const {
            return std::tie(Key, IsLast, PortionId) < std::tie(other.Key, other.IsLast, PortionId);
        };
        bool IsEquivalent(const TBorder& other) const {
            return Key == other.Key && IsLast == other.IsLast;
        };

        const NArrow::NMerger::TSortableBatchPosition& GetKey() const {
            return Key;
        }

        TString DebugString() const {
            return TStringBuilder() << (IsLast ? "Last:" : "First:") << Key.GetSorting()->DebugJson(0);
        }

        TPortionBorderView MakeView() const {
            return IsLast ? TPortionBorderView::Last(PortionId) : TPortionBorderView::First(PortionId);
        }
    };

private:
    std::vector<TBorder> Borders;
    // std::shared_ptr<arrow::Schema> SortingSchema;

public:
    TColumnDataSplitter(const THashMap<ui64, TPortionInfo::TConstPtr>& sources) {
        AFL_VERIFY(sources.size());
        // SortingSchema = sources.begin()->second.GetSchema();
        // TBorder sortableBegin(begin, *TValidator::CheckNotNull(sources.FindPtr(begin.GetPortionId())));
        // TBorder sortableEnd(end, *TValidator::CheckNotNull(sources.FindPtr(end.GetPortionId())));

        for (const auto& [id, source] : sources) {
            // AFL_VERIFY(localBegin <= sortableEnd)("portion_start", source->GetMeta().IndexKeyStart().DebugString())(
            //     "interval_end", sortableEnd.DebugString());
            // AFL_VERIFY(localEnd >= sortableEnd)("portion_end", source->GetMeta().IndexKeyEnd().DebugString())(
            //     "interval_start", sortableBegin.DebugString());
            Borders.emplace_back(TBorder::First(source->GetMeta().IndexKeyStart(), id));
            Borders.emplace_back(TBorder::Last(source->GetMeta().IndexKeyEnd(), id));
        }
        // Borders.emplace_back(std::move(sortableBegin));
        // Borders.emplace_back(std::move(sortableEnd));

        std::sort(Borders.begin(), Borders.end());

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

    template <class Callback>
    void ForEachInterval(Callback&& callback) const {
        THashSet<ui64> currentPortions;
        const TBorder* lastBorder = &Borders.front();

        for (ui64 i = 1; i < Borders.size(); ++i) {
            const auto& currentBorder = Borders[i];
            if (!currentBorder.IsEquivalent(*lastBorder)) {
                if (!callback(TIntervalBordersView(lastBorder->MakeView(), currentBorder.MakeView()), currentPortions)) {
                    break;
                }
            }
            if (currentBorder.GetIsLast()) {
                AFL_VERIFY(currentPortions.erase(currentBorder.GetPortionId()));
            } else {
                AFL_VERIFY(currentPortions.insert(currentBorder.GetPortionId()).second);
            }
            lastBorder = &currentBorder;
        }
    }

    // std::vector<TRowRange> SplitPortion(const std::shared_ptr<NArrow::TGeneralContainer>& data) const {
    //     AFL_VERIFY(!Borders.empty());

    //     std::vector<ui64> borderOffsets;
    //     ui64 offset = 0;

    //     auto position = NArrow::NMerger::TRWSortableBatchPosition(data, 0, SortingSchema->field_names(), {}, false);

    //     for (const auto& border : Borders) {
    //         if (offset == data->GetRecordsCount()) {
    //             borderOffsets.emplace_back(offset);
    //             continue;
    //         }
    //         const auto findBound = NArrow::NMerger::TSortableBatchPosition::FindBound(
    //             position, offset, data->GetRecordsCount() - 1, border.GetKey(), border.GetIsLast());
    //         offset = findBound ? findBound->GetPosition() : data->GetRecordsCount();
    //         borderOffsets.emplace_back(offset);
    //     }

    //     std::vector<TRowRange> segments;
    //     for (ui64 i = 1; i < borderOffsets.size(); ++i) {
    //         segments.emplace_back(TRowRange(borderOffsets[i - 1], borderOffsets[i]));
    //     }

    //     AFL_VERIFY(segments.size() == NumIntervals())("splitted", segments.size())("expected", NumIntervals())("splitter", DebugString());
    //     return segments;
    // }

    // TString DebugString() const {
    //     TStringBuilder sb;
    //     sb << "[";
    //     for (const auto& border : Borders) {
    //         sb << border.DebugString() << ";";
    //     }
    //     sb << "]";
    //     return sb;
    // }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
