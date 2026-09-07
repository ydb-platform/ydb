#include "kqp_scan_tracing.h"

#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr::NKqp {

NWilson::TSpan MakeScanTrace(const NWilson::TTraceId& parent, const TString& table) {
    NWilson::TSpan span(TComponentTracingLevels::TQueryProcessor::Detailed,
        NWilson::TTraceId(parent), "Scan table", NWilson::EFlags::AUTO_END);
    span.Attribute("ydb.actor.type", TString("TKqpScanFetcherActor"));
    span.Attribute("db.collection.name", table);
    return span;
}

TShardScanTrace::TShardScanTrace(const NWilson::TTraceId& parent, ui64 shardId, ui64 retries)
    : Span(TComponentTracingLevels::TQueryProcessor::Diagnostic,
        NWilson::TTraceId(parent), "Scan shard", NWilson::EFlags::AUTO_END) {
    Span.Attribute("ydb.shard_id", static_cast<i64>(shardId));
    Span.Attribute("ydb.read_retries", static_cast<i64>(retries));
    Span.Attribute("ydb.timing_boundary", TString("request_to_last_message"));
}

void TShardScanTrace::OnData(ui32 nodeId, ui64 rows, TDuration cpuTime, TDuration waitTime, bool finished) {
    if (!Span) {
        return;
    }
    Rows += rows;
    Node = nodeId;
    CpuTime = cpuTime;
    WaitTime = waitTime;
    if (finished) {
        Finish(Ydb::StatusIds::SUCCESS);
    }
}

void TShardScanTrace::Finish(Ydb::StatusIds::StatusCode status) {
    if (Span) {
        Span.Attribute("ydb.rows", static_cast<i64>(Rows));
        Span.Attribute("ydb.cpu_us", static_cast<i64>(CpuTime.MicroSeconds()));
        Span.Attribute("ydb.wait_us", static_cast<i64>(WaitTime.MicroSeconds()));
        Span.Attribute("ydb.node_id", static_cast<i64>(Node));
        Span.Attribute("ydb.finished", status == Ydb::StatusIds::SUCCESS);
        EndQueryTraceSpan(Span, status);
    }
}

}
