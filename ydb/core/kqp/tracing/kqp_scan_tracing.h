#pragma once

#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NKqp {

NWilson::TSpan MakeScanTrace(const NWilson::TTraceId& parent, const TString& table);

class TShardScanTrace {
public:
    TShardScanTrace(const NWilson::TTraceId& parent, ui64 shardId, ui64 retries);
    void OnData(ui32 nodeId, ui64 rows, TDuration cpuTime, TDuration waitTime, bool finished);
    void Finish(Ydb::StatusIds::StatusCode status);
    NWilson::TTraceId GetTraceId() const { return Span_.GetTraceId(); }

private:
    NWilson::TSpan Span_;
    ui64 Rows_ = 0;
    TDuration CpuTime_;
    TDuration WaitTime_;
    ui32 Node_ = 0;
};

} // namespace NKikimr::NKqp
