#include "kqp_scan_tracing.h"

#include "kqp_query_tracing.h"

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
    : Span_(TComponentTracingLevels::TQueryProcessor::Diagnostic,
        NWilson::TTraceId(parent), "Scan shard", NWilson::EFlags::AUTO_END) {
    Span_.Attribute("ydb.shard_id", static_cast<i64>(shardId));
    Span_.Attribute("ydb.read_retries", static_cast<i64>(retries));
    Span_.Attribute("ydb.timing_boundary", TString("request_to_last_message"));
}

void TShardScanTrace::OnData(ui32 nodeId, ui64 rows, TDuration cpuTime, TDuration waitTime, bool finished) {
    if (!Span_) {
        return;
    }
    Rows_ += rows;
    Node_ = nodeId;
    CpuTime_ = cpuTime;
    WaitTime_ = waitTime;
    if (finished) {
        Finish(Ydb::StatusIds::SUCCESS);
    }
}

void TShardScanTrace::Finish(Ydb::StatusIds::StatusCode status) {
    if (Span_) {
        Span_.Attribute("ydb.rows", static_cast<i64>(Rows_));
        Span_.Attribute("ydb.cpu_us", static_cast<i64>(CpuTime_.MicroSeconds()));
        Span_.Attribute("ydb.wait_us", static_cast<i64>(WaitTime_.MicroSeconds()));
        Span_.Attribute("ydb.node_id", static_cast<i64>(Node_));
        Span_.Attribute("ydb.finished", status == Ydb::StatusIds::SUCCESS);
        EndQueryTraceSpan(Span_, status);
    }
}

} // namespace NKikimr::NKqp
