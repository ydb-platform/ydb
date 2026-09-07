#pragma once

#include "kqp_query_tracing.h"

namespace NKikimr::NKqp {

NWilson::TSpan MakeScanTrace(const NWilson::TTraceId& parent, const TString& table);

class TShardScanTrace {
public:
    TShardScanTrace(const NWilson::TTraceId& parent, ui64 shardId, ui64 retries);
    void OnData(ui32 nodeId, ui64 rows, TDuration cpuTime, TDuration waitTime, bool finished);
    void Finish(Ydb::StatusIds::StatusCode status);
    NWilson::TTraceId GetTraceId() const { return Span.GetTraceId(); }

private:
    NWilson::TSpan Span;
    ui64 Rows = 0;
    TDuration CpuTime;
    TDuration WaitTime;
    ui32 Node = 0;
};

}
