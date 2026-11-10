#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/base/row_version.h>
#include <ydb/core/protos/cdc_aggregator.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/event_pb.h>

namespace NKikimr::NCdcAggregator {

namespace TEvCdcAggregator {
    enum EEv {
        // CdcAggregator tablet protocol, sent over tablet pipes.
        EvConfigure = EventSpaceBegin(TKikimrEvents::ES_CDC_AGGREGATOR),
        EvConfigureResult,
        EvActivate,
        EvActivateResult,
        EvReportDeliveredBatch,
        EvReportDeliveredBatchResult,
        EvReplaceSource,
        EvReplaceSourceResult,

        // CdcAggregatorService protocol, local to a node.
        EvReportDelivered = EvConfigure + 512,
        EvReportDeliveredResult,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_CDC_AGGREGATOR),
        "Expected EvEnd < EventSpaceEnd(TKikimrEvents::ES_CDC_AGGREGATOR)");

    struct TEvConfigure
        : public TEventPB<TEvConfigure, NKikimrCdcAggregator::TEvConfigure, EvConfigure>
    {};

    struct TEvConfigureResult
        : public TEventPB<TEvConfigureResult, NKikimrCdcAggregator::TEvConfigureResult, EvConfigureResult>
    {};

    struct TEvActivate
        : public TEventPB<TEvActivate, NKikimrCdcAggregator::TEvActivate, EvActivate>
    {};

    struct TEvActivateResult
        : public TEventPB<TEvActivateResult, NKikimrCdcAggregator::TEvActivateResult, EvActivateResult>
    {};

    struct TEvReportDeliveredBatch
        : public TEventPB<TEvReportDeliveredBatch, NKikimrCdcAggregator::TEvReportDeliveredBatch, EvReportDeliveredBatch>
    {};

    struct TEvReportDeliveredBatchResult
        : public TEventPB<TEvReportDeliveredBatchResult, NKikimrCdcAggregator::TEvReportDeliveredBatchResult, EvReportDeliveredBatchResult>
    {};

    struct TEvReplaceSource
        : public TEventPB<TEvReplaceSource, NKikimrCdcAggregator::TEvReplaceSource, EvReplaceSource>
    {};

    struct TEvReplaceSourceResult
        : public TEventPB<TEvReplaceSourceResult, NKikimrCdcAggregator::TEvReplaceSourceResult, EvReplaceSourceResult>
    {};

    // CDC sender -> local CdcAggregatorService.
    //
    // The sender may report Version only after every change of this source
    // below Version is durably written to the topic and no new change below
    // Version can appear. The sender keeps the heartbeat until it receives
    // Persisted with PersistedVersion >= Version and retries otherwise; the
    // service keeps reports in memory only. The request cookie correlates the
    // response.
    struct TEvReportDelivered
        : public TEventLocal<TEvReportDelivered, EvReportDelivered>
    {
        TPathId StreamPathId;
        ui64 AggregatorTabletId = 0;
        ui64 SourceTabletId = 0;
        TRowVersion Version = TRowVersion::Min();
        // Serialized NKikimrPQClient::TDataChunk of the heartbeat record.
        TString Payload;
    };

    struct TEvReportDeliveredResult
        : public TEventLocal<TEvReportDeliveredResult, EvReportDeliveredResult>
    {
        enum class EStatus {
            // The aggregator committed PersistedVersion for this source.
            Persisted,
            // Not configured, overloaded or unavailable: retry later.
            Retry,
            // The source was replaced by split/merge: stop reporting.
            Retired,
            // Wrong stream, unknown source or malformed report: the report
            // can never succeed, the configuration must be fixed.
            Rejected,
        };

        TPathId StreamPathId;
        ui64 SourceTabletId = 0;
        EStatus Status = EStatus::Retry;
        TRowVersion PersistedVersion = TRowVersion::Min();
        TString Error;
    };
} // namespace TEvCdcAggregator

// One CdcAggregatorService per node serves all local sources of all streams.
inline TActorId MakeCdcAggregatorServiceId(ui32 nodeId) {
    return TActorId(nodeId, TStringBuf("cdcaggr_svc"));
}

} // namespace NKikimr::NCdcAggregator
