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
        YDB_READONLY_DEF(std::shared_ptr<NArrow::NMerger::TSortableBatchPosition>, Key);
        YDB_READONLY_DEF(ui64, PortionId);

        TBorder(const bool isLast, const std::shared_ptr<NArrow::NMerger::TSortableBatchPosition>& key, const ui64 portionId)
            : IsLast(isLast)
            , Key(key)
            , PortionId(portionId)
        {
        }

    public:
        static TBorder First(const std::shared_ptr<NArrow::NMerger::TSortableBatchPosition>& key, const ui64 portionId) {
            return TBorder(false, key, portionId);
        }
        static TBorder Last(const std::shared_ptr<NArrow::NMerger::TSortableBatchPosition>& key, const ui64 portionId) {
            return TBorder(true, key, portionId);
        }

        bool operator<(const TBorder& other) const {
            return std::tie(*Key, IsLast, PortionId) < std::tie(*other.Key, other.IsLast, PortionId);
        };
        bool IsEquivalent(const TBorder& other) const {
            return Key == other.Key && IsLast == other.IsLast;
        };

        TString DebugString() const {
            return TStringBuilder() << "{" << (IsLast ? "Last:" : "First:") << "Portion=" << PortionId
                                    << ";Data=" << Key->GetSorting()->DebugJson(0) << "}";
        }

        TPortionBorderView MakeView() const {
            return IsLast ? TPortionBorderView::Last(PortionId) : TPortionBorderView::First(PortionId);
        }
    };

private:
    std::vector<TBorder> Borders;

public:
    TColumnDataSplitter(const THashMap<ui64, TSortableBorders>& sources) {
        AFL_VERIFY(sources.size());
        for (const auto& [id, borders] : sources) {
            Borders.emplace_back(TBorder::First(borders.GetBegin(), id));
            Borders.emplace_back(TBorder::Last(borders.GetEnd(), id));
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
                if (!callback(*lastBorder, currentBorder, currentPortions)) {
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

    TString DebugString() const {
        TStringBuilder sb;
        sb << "[";
        for (const auto& border : Borders) {
            sb << border.DebugString();
            sb << ";";
        }
        sb << "]";
        return sb;
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
