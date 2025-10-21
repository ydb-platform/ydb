#include "script.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/fetch_steps.h>

namespace NKikimr::NOlap::NReader::NCommon {

TString TFetchingScript::DebugString() const {
    TStringBuilder sb;
    TStringBuilder sbBranch;
    for (auto&& i : Steps) {
        sbBranch << "{" << i->DebugString() << "};";
    }
    if (!sbBranch) {
        return "";
    }
    sb << "{branch:" << BranchName << ";steps:[" << sbBranch << "]}";
    return sb;
}

TString TFetchingScript::ProfileDebugString() const {
    TStringBuilder sb;
    TStringBuilder sbBranch;
    for (auto&& i : Steps) {
        if (i->GetSumDuration() > TDuration::MilliSeconds(10)) {
            sbBranch << "{" << i->DebugString(true) << "};";
        }
    }
    if (!sbBranch) {
        return "";
    }
    sb << "{branch:" << BranchName << ";";
    if (AtomicGet(FinishInstant) && AtomicGet(StartInstant)) {
        sb << "duration:" << AtomicGet(FinishInstant) - AtomicGet(StartInstant) << ";";
    }

    sb << "steps_10Ms:[" << sbBranch << "]}";
    return sb;
}

void TFetchingScriptBuilder::AddAllocation(
    const std::set<ui32>& entityIds, const NArrow::NSSA::IMemoryCalculationPolicy::EStage stage, const EMemType mType) {
    if (Steps.size() == 0) {
        AddStep(std::make_shared<TAllocateMemoryStep>(entityIds, mType, stage));
    } else {
        std::optional<ui32> addIndex;
        for (i32 i = Steps.size() - 1; i >= 0; --i) {
            if (auto allocation = std::dynamic_pointer_cast<TAllocateMemoryStep>(Steps[i])) {
                if (allocation->GetStage() == stage) {
                    allocation->AddAllocation(entityIds, mType);
                    return;
                } else {
                    addIndex = i + 1;
                }
                break;
            } else if (std::dynamic_pointer_cast<TAssemblerStep>(Steps[i])) {
                continue;
            } else if (std::dynamic_pointer_cast<TColumnBlobsFetchingStep>(Steps[i])) {
                continue;
            } else {
                addIndex = i + 1;
                break;
            }
        }
        AFL_VERIFY(addIndex);
        InsertStep<TAllocateMemoryStep>(*addIndex, entityIds, mType, stage);
    }
}

TString IFetchingStep::DebugString(const bool stats) const {
    TStringBuilder sb;
    sb << "name=" << Name;
    if (stats) {
        sb << ";duration=" << GetSumDuration() << ";"
           << "size=" << 1e-9 * GetSumSize();
    }
    sb << ";details={" << DoDebugString() << "};";
    return sb;
}

TFetchingScriptBuilder::TFetchingScriptBuilder(const TSpecialReadContext& context)
    : TFetchingScriptBuilder(context.GetReadMetadata()->GetResultSchema(), context.GetMergeColumns()) {
}

void TFetchingScriptBuilder::AddFetchingStep(const TColumnsSetIds& columns, const NArrow::NSSA::IMemoryCalculationPolicy::EStage stage) {
    auto actualColumns = columns - AddedFetchingColumns;
    AddedFetchingColumns += columns;
    if (actualColumns.IsEmpty()) {
        return;
    }
    if (Steps.size() && std::dynamic_pointer_cast<TColumnBlobsFetchingStep>(Steps.back())) {
        TColumnsSetIds fetchingColumns = actualColumns + std::dynamic_pointer_cast<TColumnBlobsFetchingStep>(Steps.back())->GetColumns();
        Steps.pop_back();
        AddAllocation(actualColumns.GetColumnIds(), stage, EMemType::Blob);
        AddStep(std::make_shared<TColumnBlobsFetchingStep>(fetchingColumns));
    } else {
        AddAllocation(actualColumns.GetColumnIds(), stage, EMemType::Blob);
        AddStep(std::make_shared<TColumnBlobsFetchingStep>(actualColumns));
    }
}

void TFetchingScriptBuilder::AddAssembleStep(
    const TColumnsSetIds& columns, const TString& purposeId, const NArrow::NSSA::IMemoryCalculationPolicy::EStage stage, const bool sequential) {
    auto actualColumns = columns - AddedAssembleColumns;
    AddedAssembleColumns += columns;
    if (actualColumns.IsEmpty()) {
        return;
    }
    auto actualSet = std::make_shared<TColumnsSet>(actualColumns.GetColumnIds(), FullSchema);
    if (sequential) {
        const auto notSequentialColumnIds = GuaranteeNotOptional->Intersect(*actualSet);
        if (notSequentialColumnIds.size()) {
            AddAllocation(notSequentialColumnIds, stage, EMemType::Raw);
            std::shared_ptr<TColumnsSet> cross = actualSet->BuildSamePtr(notSequentialColumnIds);
            AddStep(std::make_shared<TAssemblerStep>(cross, purposeId));
            *actualSet = *actualSet - *cross;
        }
        if (!actualSet->IsEmpty()) {
            AddAllocation(actualSet->GetColumnIds(), stage, EMemType::RawSequential);
            AddStep(std::make_shared<TOptionalAssemblerStep>(actualSet, purposeId));
        }
    } else {
        AddAllocation(actualColumns.GetColumnIds(), stage, EMemType::Raw);
        AddStep(std::make_shared<TAssemblerStep>(actualSet, purposeId));
    }
}

}   // namespace NKikimr::NOlap::NReader::NCommon
