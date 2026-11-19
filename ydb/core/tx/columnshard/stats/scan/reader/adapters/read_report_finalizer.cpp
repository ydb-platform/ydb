#include "read_report_finalizer.h"

#include <ydb/core/tx/columnshard/stats/scan/metrics.h>

#include <algorithm>
#include <limits>
#include <vector>

namespace NKikimr::NOlap::NStats::NScan {

void TReadReportFinalizer::NormalizeGroupTime(NColumnShardStats::TNumericObservation& metric) {
    auto* coverage = metric.MutableCoverage();
    const bool measured = metric.HasValue() && coverage->GetApplicable() == coverage->GetMeasured() &&
                          coverage->GetMissingReason() == NColumnShardStats::MISSING_NONE;
    const auto reason = coverage->GetMissingReason();
    coverage->Clear();
    coverage->SetApplicable(1);
    coverage->SetMeasured(measured ? 1 : 0);
    if (!measured) {
        metric.ClearValue();
        coverage->SetMissingReason(reason == NColumnShardStats::MISSING_NONE ? NColumnShardStats::UNAVAILABLE : reason);
    }
}

void TReadReportFinalizer::CompletePreparation(NColumnShardStats::TOperation& operation) {
    if (operation.HasCallsCount()) {
        return;
    }
    auto summary = Definition(operation);
    bool observed = false;
    bool unfinished = false;
    const auto addTime = [&](const auto& phase) {
        AddMetric(*summary.MutableStatistics()->MutableActiveTimeUs(), phase.GetStatistics().GetActiveTimeUs());
        AddMetric(*summary.MutableStatistics()->MutableWaitTimeUs(), phase.GetStatistics().GetWaitTimeUs());
    };
    const auto addSelection = [&](const auto& metadata) {
        AddMetric(*summary.MutablePreparation()->MutableSelectedPortions(), metadata.GetPrepareReadMetadata().GetSelectedPortions());
        AddMetric(*summary.MutablePreparation()->MutableSelectedSources(), metadata.GetPrepareReadMetadata().GetSelectedSources());
    };
    for (const auto& phase : operation.GetSubOperations()) {
        if (!phase.HasPrepareRead() && !phase.HasReaderInit()) {
            continue;
        }
        if (phase.HasCallsCount()) {
            observed = true;
            addTime(phase);
        }
        if (phase.HasUnfinished()) {
            observed = true;
            unfinished = true;
            addTime(phase.GetUnfinished());
        }
        for (const auto& metadata : phase.GetSubOperations()) {
            if (metadata.HasPrepareReadMetadata()) {
                addSelection(metadata);
                if (metadata.HasUnfinished()) {
                    addSelection(metadata.GetUnfinished());
                }
            }
        }
    }
    if (!observed) {
        return;
    }
    NormalizeGroupTime(*summary.MutableStatistics()->MutableActiveTimeUs());
    NormalizeGroupTime(*summary.MutableStatistics()->MutableWaitTimeUs());
    summary.SetCallsCount(1);
    if (unfinished) {
        operation.MutableUnfinished()->Swap(&summary);
    } else {
        operation.MutableStatistics()->Swap(summary.MutableStatistics());
        operation.MutablePreparation()->Swap(summary.MutablePreparation());
        operation.SetCallsCount(1);
    }
}

void TReadReportFinalizer::CompleteOutput(NColumnShardStats::TOperation& operation) {
    if (operation.HasSend()) {
        auto* active = operation.MutableStatistics()->MutableActiveTimeUs();
        const ui64 measured = active->GetCoverage().GetMeasured();
        if (active->HasValue() && measured && measured + 1 == operation.GetCallsCount()) {
            active->SetValue(active->GetValue() + active->GetValue() / measured);
        }
    }
    for (auto& child : *operation.MutableSubOperations()) {
        CompleteOutput(child);
    }
}

std::optional<ui32> TReadReportFinalizer::FirstIndexCheck(const NColumnShardStats::TOperation& operation) {
    std::optional<ui32> result;
    if (operation.HasIndexCheck() && operation.GetIndexCheck().HasApplicationOrder()) {
        result = operation.GetIndexCheck().GetApplicationOrder();
    }
    for (const auto& child : operation.GetSubOperations()) {
        if (const auto order = FirstIndexCheck(child); order && (!result || *order < *result)) {
            result = order;
        }
    }
    return result;
}

std::pair<ui32, ui32> TReadReportFinalizer::ReadPhaseOrder(const NColumnShardStats::TOperation& operation) {
    if (operation.HasPKFilter()) {
        return { 0, 0 };
    }
    if (operation.HasIndexFilter()) {
        return { 1, operation.GetIndexFilter().HasApplicationOrder() ? operation.GetIndexFilter().GetApplicationOrder()
                                                                     : std::numeric_limits<ui32>::max() };
    }
    if (operation.HasFilter()) {
        return { 2, 0 };
    }
    if (operation.HasFetchColumns()) {
        return { 3, 0 };
    }
    if (operation.HasAggregation()) {
        return { 4, 0 };
    }
    if (operation.HasProjection()) {
        return { 5, 0 };
    }
    if (operation.HasBuildResult()) {
        return { 6, 0 };
    }
    return { std::numeric_limits<ui32>::max(), 0 };
}

void TReadReportFinalizer::OrderReader(NColumnShardStats::TOperation& reader) {
    std::vector<NColumnShardStats::TOperation*> ordered;
    for (auto& operation : *reader.MutableSubOperations()) {
        if (operation.HasIndexFilter()) {
            if (const auto order = FirstIndexCheck(operation)) {
                operation.MutableIndexFilter()->SetApplicationOrder(*order);
            }
        }
        if (ReadPhaseOrder(operation).first != std::numeric_limits<ui32>::max()) {
            ordered.push_back(&operation);
        }
    }
    std::stable_sort(ordered.begin(), ordered.end(), [](const auto* left, const auto* right) {
        return ReadPhaseOrder(*left) < ReadPhaseOrder(*right);
    });
    auto next = ordered.begin();
    auto* children = reader.MutableSubOperations();
    for (auto child = children->pointer_begin(); child != children->pointer_end(); ++child) {
        if (ReadPhaseOrder(**child).first != std::numeric_limits<ui32>::max()) {
            *child = *next++;
        }
    }
}

void TReadReportFinalizer::Finalize() && {
    for (auto& operation : *Report.MutableOperations()) {
        if (operation.HasPreparation()) {
            CompletePreparation(operation);
        } else if (operation.HasOutput()) {
            CompleteOutput(operation);
        } else if (operation.HasReader()) {
            OrderReader(operation);
        }
    }
    std::stable_sort(
        Report.MutableOperations()->pointer_begin(), Report.MutableOperations()->pointer_end(), [](const auto* left, const auto* right) {
            const auto rank = [](const auto& operation) {
                return operation.HasPreparation() ? 0 : operation.HasReader() ? 1 : operation.HasOutput() ? 2 : 3;
            };
            return rank(*left) < rank(*right);
        });
}

}   // namespace NKikimr::NOlap::NStats::NScan
