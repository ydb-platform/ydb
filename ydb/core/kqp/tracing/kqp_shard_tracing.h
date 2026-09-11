#pragma once

#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <library/cpp/time_provider/monotonic.h>

#include <util/datetime/base.h>

#include <map>
#include <tuple>

namespace NKikimr::NKqp {

class TShardReadTrace {
public:
    NWilson::TTraceId Start(const NWilson::TSpan& parent, ui64 shardId, ui64 readId);
    void ReadResult(NWilson::TSpan& parent, ui64 shardId, ui32 nodeId, ui64 readId,
        ui64 rows, Ydb::StatusIds::StatusCode status, bool finished);
    void Retry(NWilson::TSpan& parent, ui64 shardId, ui64 readId);
    void Stop(ui64 readId);
    void Finish(NWilson::TSpan& parent);

private:
    struct TRead {
        NWilson::TSpan Span;
        TMonotonic Start;
        ui64 ShardId = 0;
        ui32 NodeId = 0;
        ui64 Rows = 0;
    };

    class TShard {
    public:
        ui64 DurationUs() const {
            return FirstRequest && LastResponse >= FirstRequest ? (LastResponse - FirstRequest).MicroSeconds() : 0;
        }
        auto Rank() const {
            return std::tuple(FailedReads > 0, Retries > 0, StoppedReads > 0, DurationUs());
        }

    public:
        TMonotonic FirstRequest;
        TMonotonic LastResponse;
        ui64 Rows = 0;
        ui64 Reads = 0;
        ui64 FailedReads = 0;
        ui64 StoppedReads = 0;
        ui64 Retries = 0;
        ui64 LastReadId = 0;
        ui32 NodeId = 0;
        bool TimingIncomplete = false;
        Ydb::StatusIds::StatusCode LastStatus = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    };

    static bool Enabled(const NWilson::TSpan& parent);
    void Complete(ui64 readId, Ydb::StatusIds::StatusCode status, bool finished);
    void RetainShards();

private:
    std::map<ui64, TRead> Reads_;
    std::map<ui64, TShard> Shards_;
    ui64 TotalReads_ = 0;
    ui64 UntracedReads_ = 0;
    ui64 EvictedShards_ = 0;
};

} // namespace NKikimr::NKqp
