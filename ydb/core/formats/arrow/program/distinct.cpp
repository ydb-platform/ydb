#include "distinct.h"

#include "collection.h"
#include "execution.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>

namespace NKikimr::NArrow::NSSA {

static TString ScalarKey(const std::shared_ptr<arrow::Scalar>& scalar) {
    if (!scalar || !scalar->is_valid) {
        return "NULL";
    }

    return TStringBuilder() << (ui32)scalar->type->id() << ":" << scalar->ToString();
}

TConclusion<IResourceProcessor::EExecutionResult> TDistinctProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const
{
    std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> keyColumns;
    if (nodeContext.GetRemoveResourceIds().contains(KeyColumnId)) {
        keyColumns = context.MutableResources().ExtractAccessors({KeyColumnId});
    } else {
        keyColumns = context.MutableResources().GetAccessors({KeyColumnId});
    }
    AFL_VERIFY(keyColumns.size() == 1);
    auto keyAccessor = keyColumns.front();
    const ui32 recordsCount = keyAccessor->GetRecordsCount();

    NArrow::TColumnFilter distinctFilter = NArrow::TColumnFilter::BuildAllowFilter();
    if (!recordsCount) {
        context.MutableResources().AddFilter(distinctFilter);
        return EExecutionResult::Success;
    }

    std::shared_ptr<arrow::Scalar> monoValue;
    const auto isMonoValue = keyAccessor->CheckOneValueAccessor(monoValue);
    if (isMonoValue && *isMonoValue) {
        const TString key = ScalarKey(monoValue);
        if (!Seen.contains(key)) {
            Seen.emplace(key);
            distinctFilter.Add(true, 1);
            if (recordsCount > 1) {
                distinctFilter.Add(false, recordsCount - 1);
            }
        } else {
            distinctFilter.Add(false, recordsCount);
        }
    } else {
        auto chunked = keyAccessor->GetChunkedArray();
        for (const auto& chunk : chunked->chunks()) {
            for (int64_t i = 0; i < chunk->length(); ++i) {
                auto scalarRes = chunk->GetScalar(i);
                if (!scalarRes.ok()) {
                    return TConclusionStatus::Fail(scalarRes.status().message());
                }
                const TString key = ScalarKey(*scalarRes);
                const bool isNew = Seen.emplace(key).second;
                distinctFilter.Add(isNew);
            }
        }
    }

    AFL_VERIFY(distinctFilter.GetRecordsCountVerified() == recordsCount)("filter", distinctFilter.GetRecordsCountVerified())("records", recordsCount);
    context.MutableResources().AddFilter(distinctFilter);
    return EExecutionResult::Success;
}

} // namespace NKikimr::NArrow::NSSA
