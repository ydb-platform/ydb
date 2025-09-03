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

public:
    TColumnDataSplitter(const THashMap<ui64, TPortionInfo::TConstPtr>& sources) {
        AFL_VERIFY(sources.size());
        for (const auto& [id, source] : sources) {
            Borders.emplace_back(TBorder::First(source->GetMeta().IndexKeyStart(), id));
            Borders.emplace_back(TBorder::Last(source->GetMeta().IndexKeyEnd(), id));
        }
        std::sort(Borders.begin(), Borders.end());
    }

    template <class Callback>
    void ForEachInterval(Callback&& callback) const {
        AFL_VERIFY(Borders.size());
        THashSet<ui64> currentPortions;
        const TBorder* lastBorder = nullptr;

        for (const auto& currentBorder : Borders) {
            if (lastBorder && !currentBorder.IsEquivalent(*lastBorder)) {
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
};

}   // namespace NKikimr::NOlap::NReader::NSimple
