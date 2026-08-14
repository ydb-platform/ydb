#pragma once

#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/datetime/base.h>
#include <util/generic/utility.h>
#include <util/system/types.h>

#include <unordered_map>
#include <vector>

namespace NKikimr::NKqp {

constexpr size_t MaxShardReadDiagnostics = 8;
constexpr size_t MaxCommitShardDiagnostics = 8;

// Exported diagnostics use wall clock because Wilson span timestamps are wall-clock values.
struct TTimeWindow {
    TInstant Start;
    TInstant End;

    explicit operator bool() const {
        return Start != TInstant::Zero() && End > Start;
    }
};

class TShardReadDiagnosticsCollector {
public:
    void OnStart(ui64 shardId) {
        auto it = Indexes_.find(shardId);
        if (it == Indexes_.end()) {
            if (Reads_.size() >= MaxShardReadDiagnostics) {
                ++Dropped_;
                return;
            }
            const size_t index = Reads_.size();
            Reads_.emplace_back();
            it = Indexes_.emplace(shardId, index).first;
        }
        auto& shard = Reads_[it->second];
        shard.SetShardId(shardId);
        if (!shard.GetStartTimeMs()) {
            shard.SetStartTimeMs(TInstant::Now().MilliSeconds());
        }
    }

    void OnFinish(ui64 shardId, ui64 rows, ui32 retries, ui32 nodeId = 0,
            Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
            bool finished = true) {
        const auto it = Indexes_.find(shardId);
        if (it == Indexes_.end()) {
            return;
        }
        auto& shard = Reads_[it->second];
        shard.SetFinishTimeMs(TInstant::Now().MilliSeconds());
        shard.SetRows(shard.GetRows() + rows);
        shard.SetRetries(Max(shard.GetRetries(), retries));
        // Status is the final outcome; retries preserve transient failures separately.
        shard.SetStatus(status);
        shard.SetFinished(finished || status != Ydb::StatusIds::SUCCESS);
        if (nodeId) {
            shard.SetNodeId(nodeId);
        }
    }

    bool Empty() const {
        return Reads_.empty();
    }

    void OnError(Ydb::StatusIds::StatusCode status) {
        const ui64 nowMs = TInstant::Now().MilliSeconds();
        for (auto& shard : Reads_) {
            if (!shard.GetFinished()) {
                shard.SetFinishTimeMs(nowMs);
                shard.SetStatus(status);
                shard.SetFinished(true);
            }
        }
    }

    void Export(NKqpProto::TKqpTaskExtraStats& extraStats, ui32 totalRetries) const {
        if (totalRetries) {
            extraStats.SetReadRetriesCount(extraStats.GetReadRetriesCount() + totalRetries);
        }
        for (const auto& shard : Reads_) {
            *extraStats.AddShardReads() = shard;
        }
        if (Dropped_ > 0) {
            extraStats.SetShardReadsTruncated(extraStats.GetShardReadsTruncated() + Dropped_);
        }
    }

private:
    std::vector<NKqpProto::TKqpShardReadStats> Reads_;
    std::unordered_map<ui64, size_t> Indexes_;
    size_t Dropped_ = 0;
};

struct TShardCommitDiagnostic {
    ui64 ShardId = 0;
    TInstant PreparedAt;
    TInstant CommittedAt;
};

struct TCommitDiagnostics {
    TTimeWindow PrepareShards;
    TTimeWindow Coordinator;
    TTimeWindow ApplyShards;
    std::vector<TShardCommitDiagnostic> Shards;
    size_t ShardsTruncated = 0;
};

} // namespace NKikimr::NKqp
