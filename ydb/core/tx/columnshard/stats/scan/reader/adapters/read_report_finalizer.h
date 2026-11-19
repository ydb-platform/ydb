#pragma once

#include <ydb/core/protos/columnshard_scan_stats.pb.h>

#include <optional>
#include <utility>

namespace NKikimr::NOlap::NStats::NScan {

class TReadReportFinalizer {
    NColumnShardStats::TReport& Report;

    static void NormalizeGroupTime(NColumnShardStats::TNumericObservation& metric);
    static void CompletePreparation(NColumnShardStats::TOperation& operation);
    static void CompleteOutput(NColumnShardStats::TOperation& operation);
    static std::optional<ui32> FirstIndexCheck(const NColumnShardStats::TOperation& operation);
    static std::pair<ui32, ui32> ReadPhaseOrder(const NColumnShardStats::TOperation& operation);
    static void OrderReader(NColumnShardStats::TOperation& reader);

public:
    explicit TReadReportFinalizer(NColumnShardStats::TReport& report)
        : Report(report)
    {
    }

    // Apply once to the completed output snapshot before publication.
    void Finalize() &&;
};

}   // namespace NKikimr::NOlap::NStats::NScan
