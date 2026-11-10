#pragma once

#include "stream_repository.h"

#include <util/generic/size_literals.h>

namespace NKikimr::NCdcAggregator {

// Bounds that keep one transaction small whatever the sender does.
constexpr size_t MaxReportsPerBatch = 10000;
constexpr size_t MaxBarrierPayloadBytes = 64_KB;

struct TDeliveredReport {
    TSourceId Source = 0;
    TBarrier Delivered;
};

struct TReportDeliveredCommand {
    TPathId StreamPathId;
    TVector<TDeliveredReport> Reports;
};

struct TReportDeliveredResult {
    enum class EStatus {
        Success,
        NotReady,
        WrongStream,
        BadRequest,
    };

    struct TSourceResult {
        enum class EStatus {
            Success,
            UnknownSource,
            RetiredSource,
            BadRequest,
        };

        TSourceId Source = 0;
        EStatus Status = EStatus::Success;
        // For Success: the progress committed with the batch, never lower
        // than the reported version.
        TRowVersion PersistedVersion = TRowVersion::Min();
        // The report raised the stored progress.
        bool Advanced = false;
    };

    EStatus Status = EStatus::Success;
    TString Error;
    // One result per report, in command order.
    TVector<TSourceResult> Results;
    // The barrier committed together with this batch, if the batch advanced it.
    std::optional<TBarrier> AdvancedReadyBarrier;
};

// Applies a batch of reports of several sources atomically.
//
// Results carry values captured right after the batch is applied: they are
// valid once this batch commits, while the shared stream state may already
// contain later uncommitted batches.
TReportDeliveredResult ReportDelivered(TStream& stream, IStreamRepository& repository, const TReportDeliveredCommand& command);

} // namespace NKikimr::NCdcAggregator
