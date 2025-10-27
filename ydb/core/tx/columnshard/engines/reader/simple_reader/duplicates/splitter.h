#pragma once

#include "common.h"

#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/formats/arrow/rows/view.h>
#include <ydb/core/formats/arrow/special_keys.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering  {

class TColumnDataSplitter {
private:
    std::vector<TIntervalBorder> Borders;

public:
    TColumnDataSplitter(const THashMap<ui64, TSortableBorders>& sources) {
        AFL_VERIFY(sources.size());
        for (const auto& [id, borders] : sources) {
            Borders.emplace_back(TIntervalBorder::First(borders.GetBegin(), id));
            Borders.emplace_back(TIntervalBorder::Last(borders.GetEnd(), id));
        }
        std::sort(Borders.begin(), Borders.end());
    }

    template <class Callback>
    void ForEachIntersectingInterval(Callback&& callback, const ui64 intersectingPortionId) const {
        AFL_VERIFY(Borders.size());
        THashSet<ui64> currentPortions;
        const TIntervalBorder* prevBorder = &Borders.front();

        bool intersectionsStarted = false;
        for (ui64 i = 1; i < Borders.size(); ++i) {
            const auto& currentBorder = Borders[i];

            if (prevBorder->GetIsLast()) {
                AFL_VERIFY(currentPortions.erase(prevBorder->GetPortionId()));
            } else {
                AFL_VERIFY(currentPortions.insert(prevBorder->GetPortionId()).second);
            }

            if (prevBorder->GetPortionId() == intersectingPortionId) {
                if (prevBorder->GetIsLast()) {
                    AFL_VERIFY(intersectionsStarted);
                    break;
                } else {
                    AFL_VERIFY(!intersectionsStarted);
                    intersectionsStarted = true;
                }
            }

            if (intersectionsStarted && !currentBorder.IsEquivalent(*prevBorder)) {
                if (!callback(*prevBorder, currentBorder, currentPortions)) {
                    break;
                }
            }

            prevBorder = &currentBorder;
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
