#pragma once

#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/datetime/base.h>
#include <util/generic/utility.h>
#include <util/system/types.h>

#include <algorithm>
#include <unordered_map>
#include <tuple>
#include <vector>

namespace NKikimr::NKqp {

// Bounded source-side collectors retain failures, retries, and stragglers under high fan-out.
constexpr size_t MaxShardReadDiagnostics = 8;
constexpr size_t MaxActiveShardReadDiagnostics = 32;
constexpr size_t MaxCommitShardDiagnostics = 8;

// Exported diagnostics use wall clock because Wilson span timestamps are wall-clock values.
struct TTimeWindow {
    TInstant Start;
    TInstant End;

    explicit operator bool() const {
        return Start != TInstant::Zero() && End > Start;
    }
};

inline auto ShardReadDiagnosticsRank(const NKqpProto::TKqpShardReadStats& shard) {
    const bool failed = shard.GetStatus() != Ydb::StatusIds::STATUS_CODE_UNSPECIFIED
        && shard.GetStatus() != Ydb::StatusIds::SUCCESS;
    const ui64 durationMs = shard.GetStartTimeMs()
            && shard.GetFinishTimeMs() >= shard.GetStartTimeMs()
        ? shard.GetFinishTimeMs() - shard.GetStartTimeMs() : 0;
    return std::tuple(failed, shard.GetRetries() > 0, durationMs);
}

class TShardReadDiagnosticsCollector {
public:
    ui64 OnStart(ui64 shardId, TInstant now = TInstant::Now()) {
        if (const auto it = ActiveStarts_.find(shardId); it != ActiveStarts_.end()) {
            return it->second;
        }
        Indexes_.erase(shardId);
        const ui64 startTimeMs = now.MilliSeconds();
        if (ActiveStarts_.size() < MaxActiveShardReadDiagnostics) {
            ActiveStarts_.emplace(shardId, startTimeMs);
            return startTimeMs;
        }
        // Keep timings for the oldest in-flight reads. Overflow failures and retries
        // remain retainable because they rank ahead of successful samples.
        return 0;
    }

    void OnFinish(ui64 shardId, ui64 rows, ui32 retries, ui32 nodeId = 0,
            Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
            bool finished = true, TInstant now = TInstant::Now(), ui64 startTimeMs = 0) {
        auto it = Indexes_.find(shardId);
        if (!startTimeMs) {
            if (const auto active = ActiveStarts_.find(shardId); active != ActiveStarts_.end()) {
                startTimeMs = active->second;
            }
        }
        const bool terminal = finished || status != Ydb::StatusIds::SUCCESS;
        if (terminal) {
            ActiveStarts_.erase(shardId);
        }
        if (it == Indexes_.end()) {
            NKqpProto::TKqpShardReadStats candidate;
            candidate.SetShardId(shardId);
            candidate.SetStartTimeMs(startTimeMs);
            candidate.SetFinishTimeMs(now.MilliSeconds());
            candidate.SetRows(rows);
            candidate.SetRetries(retries);
            candidate.SetStatus(status);
            candidate.SetFinished(terminal);
            if (nodeId) {
                candidate.SetNodeId(nodeId);
            }
            if (Reads_.size() < MaxShardReadDiagnostics) {
                const size_t index = Reads_.size();
                Reads_.push_back(std::move(candidate));
                Indexes_.emplace(shardId, index);
                return;
            }
            const size_t replacement = FindLessInterestingThan(candidate);
            if (replacement == Reads_.size()) {
                Dropped_ += terminal;
                return;
            }
            if (const auto retained = Indexes_.find(Reads_[replacement].GetShardId());
                    retained != Indexes_.end() && retained->second == replacement) {
                Indexes_.erase(retained);
            }
            Reads_[replacement] = std::move(candidate);
            Indexes_.emplace(shardId, replacement);
            ++Dropped_;
            return;
        }
        auto& shard = Reads_[it->second];
        if (startTimeMs && (!shard.GetStartTimeMs() || startTimeMs < shard.GetStartTimeMs())) {
            shard.SetStartTimeMs(startTimeMs);
        }
        shard.SetFinishTimeMs(now.MilliSeconds());
        shard.SetRows(shard.GetRows() + rows);
        shard.SetRetries(Max(shard.GetRetries(), retries));
        // Status is the final outcome; retries preserve transient failures separately.
        shard.SetStatus(status);
        shard.SetFinished(terminal);
        if (nodeId) {
            shard.SetNodeId(nodeId);
        }
    }

    bool Empty() const {
        return Reads_.empty();
    }

    void OnError(Ydb::StatusIds::StatusCode status) {
        const ui64 nowMs = TInstant::Now().MilliSeconds();
        std::vector<std::pair<ui64, ui64>> active(ActiveStarts_.begin(), ActiveStarts_.end());
        for (const auto& [shardId, startTimeMs] : active) {
            OnFinish(shardId, 0, 0, 0, status, true, TInstant::MilliSeconds(nowMs), startTimeMs);
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
    size_t FindLessInterestingThan(const NKqpProto::TKqpShardReadStats& candidate) const {
        size_t result = Reads_.size();
        for (size_t i = 0; i < Reads_.size(); ++i) {
            if (ShardReadDiagnosticsRank(Reads_[i]) >= ShardReadDiagnosticsRank(candidate)) {
                continue;
            }
            if (result == Reads_.size()
                    || ShardReadDiagnosticsRank(Reads_[i])
                        < ShardReadDiagnosticsRank(Reads_[result])) {
                result = i;
            }
        }
        return result;
    }

    std::vector<NKqpProto::TKqpShardReadStats> Reads_;
    std::unordered_map<ui64, size_t> Indexes_;
    std::unordered_map<ui64, ui64> ActiveStarts_;
    size_t Dropped_ = 0;
};

struct TShardAckDiagnostic {
    ui64 ShardId = 0;
    TInstant AcknowledgedAt;
};

class TShardAckDiagnosticsCollector {
public:
    void OnAck(ui64 shardId, TInstant acknowledgedAt = TInstant::Now()) {
        for (auto& shard : Shards_) {
            if (shard.ShardId == shardId) {
                shard.AcknowledgedAt = Max(shard.AcknowledgedAt, acknowledgedAt);
                return;
            }
        }

        TShardAckDiagnostic candidate{shardId, acknowledgedAt};
        if (Shards_.size() < MaxCommitShardDiagnostics) {
            Shards_.push_back(candidate);
            return;
        }

        ++Dropped_;
        const auto fastest = std::min_element(Shards_.begin(), Shards_.end(),
            [](const auto& lhs, const auto& rhs) {
                return lhs.AcknowledgedAt < rhs.AcknowledgedAt;
            });
        if (fastest != Shards_.end() && fastest->AcknowledgedAt < acknowledgedAt) {
            *fastest = candidate;
        }
    }

    const std::vector<TShardAckDiagnostic>& Shards() const {
        return Shards_;
    }

    size_t Dropped() const {
        return Dropped_;
    }

private:
    std::vector<TShardAckDiagnostic> Shards_;
    size_t Dropped_ = 0;
};

struct TCommitDiagnostics {
    TTimeWindow PrepareShards;
    TTimeWindow Coordinator;
    TTimeWindow ApplyShards;
    std::vector<TShardAckDiagnostic> PreparedShards;
    std::vector<TShardAckDiagnostic> CommittedShards;
    size_t PreparedShardsTruncated = 0;
    size_t CommittedShardsTruncated = 0;
};

} // namespace NKikimr::NKqp
