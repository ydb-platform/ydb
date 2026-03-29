#include "common.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {
    
TBordersIterator::TBordersIterator(
    std::vector<TBorder>&& borders, const ui64 portionsCountSoftLimit)
    : Borders(std::move(borders))
    , PortionsCountSoftLimit(portionsCountSoftLimit)
{
}

TBordersBatch TBordersIterator::Next() {
    AFL_VERIFY(NextBorder < Borders.size());
    TBordersBatch batch;
    for (; NextBorder < Borders.size() && (batch.GetPortionIds().size() < PortionsCountSoftLimit || Borders[NextBorder].GetPortionIds().empty()); ++NextBorder) {
        batch.AddBorder(Borders[NextBorder]);
    }
    return batch;
}

bool TBordersIterator::IsDone() const {
    return NextBorder == Borders.size();
}

void TBordersIteratorBuilder::AppendBorder(const TBorder& border) {
    Borders.push_back(border);
}

TBordersIterator TBordersIteratorBuilder::Build() {
    return TBordersIterator(std::move(Borders), BATCH_PORTIONS_COUNT_SOFT_LIMIT);
}

ui64 TBordersIteratorBuilder::NumBorders() const {
    return Borders.size();
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
