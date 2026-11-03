#pragma once

#include <ydb/core/protos/kqp_stats.pb.h>
#include <util/datetime/base.h>
#include <map>
namespace NKikimr::NKqp {
TString FormatDurationAsMilliseconds(TDuration duration);

struct TPerStepScanStatistics {
    TString StepName;
    // integral counter only grows during scan, delta counter gets zeroed on another chunk
    TDuration IntegralExecutionDuration;
    TDuration IntegralWaitDuration;
    ui64 DeltaRawBytesRead = 0;
    TString DebugString() const;
};

using TScanStatistics = TMap<ui32, TPerStepScanStatistics>;

// Scan batches and task snapshots can repeat the same finalized operation.
class TColumnShardScanStatsCollection {
    struct TScan {
        NKqpProto::TColumnShardScanStats Context;
        std::map<ui64, NKqpProto::TColumnShardStepStats> Steps;
    };
    std::map<TString, TScan> Scans;

public:
    bool Empty() const { return Scans.empty(); }
    void Merge(const NKqpProto::TColumnShardScanStats& report);
    void Export(google::protobuf::RepeatedPtrField<NKqpProto::TColumnShardScanStats>* dst) const;
};

}   // namespace NKikimr::NKqp
