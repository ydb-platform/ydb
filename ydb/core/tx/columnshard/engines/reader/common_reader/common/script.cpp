#include "script.h"

#include <util/string/escape.h>

#include <ydb/core/base/appdata.h>

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

TFetchingScript::TFetchingScript(const TString& branchName, std::vector<std::shared_ptr<IFetchingStep>>&& steps,
    const NColumnShard::TConcreteScanCounters* counters, const TSpecialReadContext* context)
    : BranchName(branchName)
    , Steps(std::move(steps))
{
    if (counters) {
        std::vector<NColumnShard::TStepDescription> descriptions;
        descriptions.reserve(Steps.size());
        for (const auto& step : Steps) {
            descriptions.push_back({step->GetName(), step->BuildStatisticsDetails(context)});
        }
        const auto handles = counters->RegisterSteps(descriptions);
        for (size_t i = 0; i < Steps.size(); ++i) {
            Y_ABORT_UNLESS(!Steps[i]->StepStatsHandle);
            Steps[i]->StepStatsHandle = handles[i];
        }
    }
}

std::vector<NColumnShard::TStepColumn> ResolveStatisticsColumns(const std::set<ui32>& ids, const TSpecialReadContext* context) {
    const auto resolver = context ? context->GetCommonContext()->GetResolver() : nullptr;
    std::vector<NColumnShard::TStepColumn> columns;
    columns.reserve(ids.size());
    for (const auto id : ids) {
        columns.push_back({id, resolver ? resolver->GetColumnName(id, false) : TString()});
    }
    return columns;
}

namespace {

TString FormatPKBound(const TPredicateContainer& bound, char comparison) {
    if (bound.IsAll()) {
        return {};
    }
    const auto columns = bound.GetColumnNames();
    const auto& position = bound.GetSortableBatchPosition();
    TStringBuilder values;
    for (ui32 i = 0; i < columns.size(); ++i) {
        if (i != 0) {
            values << ", ";
        }
        const auto scalar = position.GetScalar(i);
        if (!scalar->is_valid) {
            values << "NULL";
            continue;
        }
        const TString value(scalar->ToString());
        switch (scalar->type->id()) {
            case arrow::Type::STRING:
            case arrow::Type::BINARY:
            case arrow::Type::LARGE_STRING:
            case arrow::Type::LARGE_BINARY:
            case arrow::Type::FIXED_SIZE_BINARY:
                values << '"' << EscapeC(value) << '"';
                break;
            default:
                values << value;
                break;
        }
    }
    return TStringBuilder() << "KEY(" << JoinSeq(", ", columns) << ") "
                            << comparison << (bound.IsInclude() ? "=" : "") << " (" << values << ')';
}

}

NColumnShard::TStepDetails BuildPKFilterStatistics(const TSpecialReadContext* context) {
    NColumnShard::TPKFilterDetails details;
    if (context) {
        const auto& metadata = *context->GetReadMetadata();
        const auto& ranges = metadata.GetPKRangesFilter();
        if (ranges.IsEmpty()) {
            details.Predicates.emplace_back("All keys");
            return details;
        }
        details.Columns = ResolveStatisticsColumns(ranges.GetColumnIds(metadata.GetResultSchema()->GetIndexInfo()), context);
        for (const auto& range : ranges) {
            const auto lower = FormatPKBound(range.GetPredicateFrom(), '>');
            const auto upper = FormatPKBound(range.GetPredicateTo(), '<');
            if (lower && upper) {
                details.Predicates.push_back(lower + " AND " + upper);
            } else if (lower || upper) {
                details.Predicates.push_back(lower ? lower : upper);
            } else {
                details.Predicates.emplace_back("All keys");
            }
        }
    }
    return details;
}

NColumnShard::TStepDetails BuildShardingFilterStatistics(const TSpecialReadContext* context) {
    NColumnShard::TShardingFilterStepDetails details;
    if (context) {
        details.Columns = ResolveStatisticsColumns(context->GetShardingColumns()->GetColumnIds(), context);
    }
    return details;
}

TString TFetchingScript::ProfileDebugString() const {
    TStringBuilder sb;
    TStringBuilder sbBranch;
    sb << "steps:";
    for (auto&& i : Steps) {
        if (i->GetSumDuration() > TDuration::MilliSeconds(10)) {
            sbBranch << "{" << i->DebugString(true) << "};";
        }
        sb << "{" << i->DebugString(true) << "};";
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
        YDB_LOG_VERIFY(addIndex);
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
    : TFetchingScriptBuilder(context.GetReadMetadata()->GetResultSchema(), context.GetMergeColumns())
{
    if (HasAppData() && AppData()->FeatureFlags.GetEnableColumnShardStats()
        && context.GetReadMetadata()->GetStatsMode() >= NYql::NDqProto::DQ_STATS_MODE_PROFILE) {
        Counters = &context.GetCommonContext()->GetCounters();
        StatisticsContext = &context;
    }
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
