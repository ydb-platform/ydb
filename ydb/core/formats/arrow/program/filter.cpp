#include "collection.h"
#include "filter.h"

#include <ydb/core/formats/arrow/arrow_filter.h>

#include <ydb/library/formats/arrow/validation/validation.h>

namespace NKikimr::NArrow::NSSA {

class TFilterVisitor: public arrow::ArrayVisitor {
    std::vector<bool> FiltersMerged;
    ui32 CursorIdx = 0;
    bool Started = false;

public:
    void BuildColumnFilter(NArrow::TColumnFilter& result) {
        result = NArrow::TColumnFilter(std::move(FiltersMerged));
    }

    arrow::Status Visit(const arrow::BooleanArray& array) override {
        return VisitImpl(array);
    }

    arrow::Status Visit(const arrow::Int8Array& array) override {
        return VisitImpl(array);
    }

    arrow::Status Visit(const arrow::UInt8Array& array) override {
        return VisitImpl(array);
    }

    TFilterVisitor(const ui32 rowsCount) {
        FiltersMerged.resize(rowsCount, true);
    }

    class TModificationGuard: public TNonCopyable {
    private:
        TFilterVisitor& Owner;

    public:
        TModificationGuard(TFilterVisitor& owner)
            : Owner(owner) {
            Owner.CursorIdx = 0;
            AFL_VERIFY(!Owner.Started);
            Owner.Started = true;
        }

        ~TModificationGuard() {
            AFL_VERIFY(Owner.CursorIdx == Owner.FiltersMerged.size());
            Owner.Started = false;
        }
    };

    TModificationGuard StartVisit() {
        return TModificationGuard(*this);
    }

private:
    template <class TArray>
    arrow::Status VisitImpl(const TArray& array) {
        AFL_VERIFY(Started);
        for (ui32 i = 0; i < array.length(); ++i) {
            const bool columnValue = (bool)array.Value(i);
            const ui32 currentIdx = CursorIdx++;
            FiltersMerged[currentIdx] = FiltersMerged[currentIdx] && columnValue;
        }
        AFL_VERIFY(CursorIdx <= FiltersMerged.size());
        return arrow::Status::OK();
    }
};

TConclusionStatus TFilterProcessor::DoExecute(const std::shared_ptr<TAccessorsCollection>& resources) const {
    const std::vector<std::shared_ptr<IChunkedArray>> inputColumns = resources->GetAccessors(TColumnChainInfo::ExtractColumnIds(GetInput()));
    TFilterVisitor filterVisitor(inputColumns.front()->GetRecordsCount());
    for (auto& arr : inputColumns) {
        AFL_VERIFY(arr->GetRecordsCount() == inputColumns.front()->GetRecordsCount())("arr", arr->GetRecordsCount())(
                                               "first", inputColumns.front()->GetRecordsCount());
        auto cArr = arr->GetChunkedArray();
        auto g = filterVisitor.StartVisit();
        for (auto&& i : cArr->chunks()) {
            NArrow::TStatusValidator::Validate(i->Accept(&filterVisitor));
        }
    }
    NArrow::TColumnFilter filter = NArrow::TColumnFilter::BuildAllowFilter();
    filterVisitor.BuildColumnFilter(filter);
    resources->AddFilter(filter);
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NArrow::NSSA
