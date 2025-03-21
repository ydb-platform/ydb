#include "collection.h"
#include "execution.h"
#include "filter.h"

#include <ydb/core/formats/arrow/arrow_filter.h>

#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/validation/validation.h>

namespace NKikimr::NArrow::NSSA {

class TFilterVisitor: public arrow::ArrayVisitor {
    NArrow::TColumnFilter FiltersMerged = NArrow::TColumnFilter::BuildAllowFilter();
    bool Started = false;

public:
    void Add(const bool value, const ui32 count) {
        FiltersMerged.Add(value, count);
    }

    NArrow::TColumnFilter ExtractColumnFilter() {
        return std::move(FiltersMerged);
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

    class TModificationGuard: public TNonCopyable {
    private:
        TFilterVisitor& Owner;

    public:
        TModificationGuard(TFilterVisitor& owner)
            : Owner(owner) {
            AFL_VERIFY(!Owner.Started);
            Owner.Started = true;
        }

        ~TModificationGuard() {
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
            FiltersMerged.Add(!array.IsNull(i) && (bool)array.Value(i));
        }
        return arrow::Status::OK();
    }
};

TConclusion<IResourceProcessor::EExecutionResult> TFilterProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const {
    std::vector<std::shared_ptr<IChunkedArray>> inputColumns;
    if (nodeContext.GetRemoveResourceIds().contains(GetInputColumnIdOnce())) {
        inputColumns = context.GetResources()->ExtractAccessors(TColumnChainInfo::ExtractColumnIds(GetInput()));
    } else {
        inputColumns = context.GetResources()->GetAccessors(TColumnChainInfo::ExtractColumnIds(GetInput()));
    }
    AFL_VERIFY(inputColumns.size() == 1);
    TFilterVisitor filterVisitor;
    for (auto& arr : inputColumns) {
        AFL_VERIFY(arr->GetRecordsCount() == inputColumns.front()->GetRecordsCount())("arr", arr->GetRecordsCount())(
                                               "first", inputColumns.front()->GetRecordsCount());
        std::shared_ptr<arrow::Scalar> monoValue;
        const auto isMonoValue = arr->CheckOneValueAccessor(monoValue);
        if (isMonoValue && *isMonoValue) {
            const auto isTrueConclusion = ScalarIsTrue(monoValue);
            const auto isFalseConclusion = ScalarIsFalse(monoValue);
            if (isTrueConclusion.IsFail()) {
                return isTrueConclusion;
            } else if (*isTrueConclusion) {
                filterVisitor.Add(true, arr->GetRecordsCount());
            } else if (isFalseConclusion.IsFail()) {
                return isFalseConclusion;
            } else if (*isFalseConclusion) {
                filterVisitor.Add(false, arr->GetRecordsCount());
            }
        } else {
            auto cArr = arr->GetChunkedArray();
            auto g = filterVisitor.StartVisit();
            for (auto&& i : cArr->chunks()) {
                NArrow::TStatusValidator::Validate(i->Accept(&filterVisitor));
            }
        }
    }
    NArrow::TColumnFilter filter = filterVisitor.ExtractColumnFilter();
    AFL_VERIFY(filter.GetRecordsCountVerified() == inputColumns.front()->GetRecordsCount())("filter", filter.GetRecordsCountVerified())(
                                                     "input", inputColumns.front()->GetRecordsCount());
    if (context.GetLimit()) {
        context.GetResources()->AddFilter(
            filter.Cut(context.GetResources()->GetRecordsCountActualVerified(), *context.GetLimit(), context.GetReverse()));
    } else {
        context.GetResources()->AddFilter(filter);
    }
    return EExecutionResult::Success;
}

}   // namespace NKikimr::NArrow::NSSA
