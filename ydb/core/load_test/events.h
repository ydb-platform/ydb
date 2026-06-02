#pragma once

#include <ydb/core/base/events.h>

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/load_test.pb.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/histogram/hdr/histogram.h>
#include <library/cpp/monlib/dynamic_counters/percentile/percentile_lg.h>
#include <library/cpp/json/writer/json_value.h>

#include <google/protobuf/text_format.h>

#include <optional>
#include <variant>

namespace NKikimr {

struct TNbsDbgLikeFinishStats {
    ui64 BytesAccounted = 0;
    ui64 LsnsCompleted = 0;
    ui64 LsnsFailed = 0;
    ui64 WritesIssued = 0;
    ui64 WritesOk = 0;
    ui64 WritesErr = 0;
    ui64 WriteBytes = 0;
    ui64 ReadsPbIssued = 0;
    ui64 ReadsPbOk = 0;
    ui64 ReadsPbBytes = 0;
    ui64 ReadsDDiskIssued = 0;
    ui64 ReadsDDiskOk = 0;
    ui64 ReadsDDiskBytes = 0;
    ui64 RunningMs = 0;
    ui64 MeasuredMs = 0;
    ui32 MaxInFlight = 0;

    // Latency histograms (microseconds), samples only after MeasurementStartTime.
    static constexpr i64 kLatencyHistMaxUs = 134'000'000;
    static constexpr i32 kLatencyHistPrecision = 2;
    NHdr::THistogram WriteQuorumUs{kLatencyHistMaxUs, kLatencyHistPrecision};
    NHdr::THistogram WriteE2eUs{kLatencyHistMaxUs, kLatencyHistPrecision};
    NHdr::THistogram FlushUs{kLatencyHistMaxUs, kLatencyHistPrecision};
    NHdr::THistogram EraseUs{kLatencyHistMaxUs, kLatencyHistPrecision};
    NHdr::THistogram ReadPbUs{kLatencyHistMaxUs, kLatencyHistPrecision};
    NHdr::THistogram ReadDDiskUs{kLatencyHistMaxUs, kLatencyHistPrecision};
};

using TLoadWorkerFinishStats = std::variant<TNbsDbgLikeFinishStats>;

struct TEvLoad {
    enum EEv {
        EvLoadTestRequest = EventSpaceBegin(TKikimrEvents::ES_TEST_LOAD),
        EvLoadTestFinished,
        EvLoadTestResponse,
        EvNodeFinishResponse,
        EvYqlSingleQueryResponse,

        EvNbsLoadTabletCreate,
        EvNbsLoadTabletCreateResult,
        EvNbsLoadTabletDelete,
        EvNbsLoadTabletDeleteResult,

        EvNbsLoadTabletGetSummary,
        EvNbsLoadTabletGetSummaryResult,

        // TNbsLoadTabletListPageActor -> TLoadActor with rendered HTML.
        EvNbsTabletListPageReady,

        // In-process (load-actor <-> worker, worker <-> tablet) events.
        // See spec §12.1.
        EvNbsWrite,
        EvNbsWriteResult,
        EvNbsRead,
        EvNbsReadResult,
        EvConfigureTablet,
    };

    struct TEvLoadTestRequest : public TEventPB<TEvLoadTestRequest,
        NKikimr::TEvLoadTestRequest, EvLoadTestRequest>
    {};

    struct TEvLoadTestResponse : public TEventPB<TEvLoadTestResponse,
        NKikimr::TEvLoadTestResponse, EvLoadTestResponse>
    {};

    struct TLoadReport : public TThrRefBase {
        enum ELoadType {
            LOAD_READ,
            LOAD_WRITE,
            LOAD_LOG_WRITE,
            LOAD_ERASE,
        };

        TDuration Duration;
        ui64 Size;
        ui32 InFlight;
        TVector<ui64> RwSpeedBps;
        ELoadType LoadType;
        NMonitoring::TPercentileTrackerLg<10, 4, 1> LatencyUs; // Upper threshold of this tracker is ~134 seconds, size is 256kB

        double GetAverageSpeed() const {
            if (RwSpeedBps.size() < 1) {
                return 0;
            }
            double avg = 0;
            for (const ui64& speed : RwSpeedBps) {
                avg += speed;
            }
            avg /= RwSpeedBps.size();
            return avg;
        }

        double GetSpeedDeviation() const {
            if (RwSpeedBps.size() <= 1) {
                return 0;
            }
            i64 avg = (i64)GetAverageSpeed();
            double sd = 0;
            for (const ui64& speed : RwSpeedBps) {
                sd += ((i64)speed - avg) * ((i64)speed - avg);
            }
            sd /= RwSpeedBps.size();
            return std::sqrt(sd);
        }

        TString LoadTypeName() const {
            switch (LoadType) {
            case LOAD_READ:
                return "read";
            case LOAD_WRITE:
                return "write";
            case LOAD_LOG_WRITE:
                return "log_write";
            case LOAD_ERASE:
                return "erase";
            }
        }
    };

    struct TEvLoadTestFinished : public TEventLocal<TEvLoadTestFinished, TEvLoad::EvLoadTestFinished> {
        ui64 Tag;
        TIntrusivePtr<TLoadReport> Report; // nullptr indicates an error or an early stop
        TString ErrorReason; // human readable status, might be nonempty even in the case of success
        TString LastHtmlPage;
        NJson::TJsonValue JsonResult;

        // Optional load-worker-specific telemetry (e.g. TNbsDbgLikeFinishStats).
        std::optional<TLoadWorkerFinishStats> WorkerStats;

        TEvLoadTestFinished(ui64 tag, TIntrusivePtr<TLoadReport> report, TString errorReason = {})
            : Tag(tag)
            , Report(report)
            , ErrorReason(errorReason)
        {}
    };

    struct TEvNodeFinishResponse : public TEventPB<TEvNodeFinishResponse,
        NKikimr::TEvNodeFinishResponse, EvNodeFinishResponse>
    {
        TString ToString() const {
            TString str;
            google::protobuf::TextFormat::PrintToString(Record, &str);
            return str;
        }
    };

    struct TEvYqlSingleQueryResponse : public TEventLocal<TEvYqlSingleQueryResponse, TEvLoad::EvYqlSingleQueryResponse> {
        TString Result;  // empty in case if there is an error
        TMaybe<TString> ErrorMessage;
        TMaybe<NKikimrKqp::TQueryResponse> Response;

        TEvYqlSingleQueryResponse(TString result, TMaybe<TString> errorMessage, TMaybe<NKikimrKqp::TQueryResponse> response)
            : Result(std::move(result))
            , ErrorMessage(std::move(errorMessage))
            , Response(std::move(response))
        {}
    };

    struct TEvNbsLoadTabletAllocateGroups : public TEventPB<TEvNbsLoadTabletAllocateGroups,
        NKikimr::TEvNbsLoadTabletAllocateGroups, EvNbsLoadTabletCreate>
    {};

    struct TEvNbsLoadTabletAllocateGroupsResult : public TEventPB<TEvNbsLoadTabletAllocateGroupsResult,
        NKikimr::TEvNbsLoadTabletAllocateGroupsResult, EvNbsLoadTabletCreateResult>
    {};

    struct TEvNbsLoadTabletDelete : public TEventPB<TEvNbsLoadTabletDelete,
        NKikimr::TEvNbsLoadTabletDelete, EvNbsLoadTabletDelete>
    {};

    struct TEvNbsLoadTabletDeleteResult : public TEventPB<TEvNbsLoadTabletDeleteResult,
        NKikimr::TEvNbsLoadTabletDeleteResult, EvNbsLoadTabletDeleteResult>
    {};

    struct TEvNbsLoadTabletGetSummary : public TEventPB<TEvNbsLoadTabletGetSummary,
        NKikimr::TEvNbsLoadTabletGetSummary, EvNbsLoadTabletGetSummary>
    {};

    struct TEvNbsLoadTabletGetSummaryResult : public TEventPB<TEvNbsLoadTabletGetSummaryResult,
        NKikimr::TEvNbsLoadTabletGetSummaryResult, EvNbsLoadTabletGetSummaryResult>
    {};

    struct TEvNbsTabletListPageReady : public TEventLocal<TEvNbsTabletListPageReady, EvNbsTabletListPageReady> {
        ui32 HttpRequestId = 0;
        TString HtmlFragment;
    };

    // ---- In-process load-actor <-> worker contract (spec §12.1) -------
    //
    // These messages travel between actors registered on the same node
    // (the per-Run load actor and the long-lived worker that lives on the
    // parent tablet's mailbox). The load actor times every request itself
    // from its own Send/receive timestamps and keys its in-flight map by
    // the event cookie; the worker's reply carries only Status. The read
    // payload travels as a TRope attached to TEvNbsReadResult.

    struct TEvNbsWrite : public TEventPB<TEvNbsWrite,
        NKikimr::TEvLoadTestRequest::TNbsDbgLikeLoad::TNbsWrite, EvNbsWrite>
    {
        TEvNbsWrite() = default;
        TEvNbsWrite(ui64 address, ui32 sizeBytes) {
            Record.SetAddress(address);
            Record.SetSizeBytes(sizeBytes);
        }
    };

    struct TEvNbsWriteResult : public TEventPB<TEvNbsWriteResult,
        NKikimr::TEvLoadTestRequest::TNbsDbgLikeLoad::TNbsWriteResult, EvNbsWriteResult>
    {
        TEvNbsWriteResult() = default;
        explicit TEvNbsWriteResult(ui32 status) {
            Record.SetStatus(status);
        }
    };

    struct TEvNbsRead : public TEventPB<TEvNbsRead,
        NKikimr::TEvLoadTestRequest::TNbsDbgLikeLoad::TNbsRead, EvNbsRead>
    {
        TEvNbsRead() = default;
        TEvNbsRead(ui64 address, ui32 sizeBytes) {
            Record.SetAddress(address);
            Record.SetSizeBytes(sizeBytes);
        }
    };

    struct TEvNbsReadResult : public TEventPB<TEvNbsReadResult,
        NKikimr::TEvLoadTestRequest::TNbsDbgLikeLoad::TNbsReadResult, EvNbsReadResult>
    {
        // Read payload is attached separately via AddPayload(TRope) and
        // recovered on the receiver via GetPayload(0).
        TEvNbsReadResult() = default;
        explicit TEvNbsReadResult(ui32 status) {
            Record.SetStatus(status);
        }
    };

    struct TEvConfigureTablet : public TEventPB<TEvConfigureTablet,
        NKikimr::TEvLoadTestRequest::TNbsDbgLikeLoad::TConfigureTablet, EvConfigureTablet>
    {};
};

inline const TNbsDbgLikeFinishStats* GetNbsDbgLikeFinishStats(
    const TEvLoad::TEvLoadTestFinished& ev)
{
    if (!ev.WorkerStats) {
        return nullptr;
    }
    return std::get_if<TNbsDbgLikeFinishStats>(&*ev.WorkerStats);
}

inline void SetNbsDbgLikeFinishStats(
    TEvLoad::TEvLoadTestFinished& ev,
    TNbsDbgLikeFinishStats stats)
{
    ev.WorkerStats = TLoadWorkerFinishStats{std::move(stats)};
}

}
