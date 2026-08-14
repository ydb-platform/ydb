#pragma once

#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/datetime/base.h>
#include <util/generic/utility.h>
#include <util/system/types.h>

#include <unordered_map>
#include <tuple>
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
    void OnStart(ui64 shardId, TInstant now = TInstant::Now()) {
        auto it = Indexes_.find(shardId);
        if (it == Indexes_.end()) {
            if (Reads_.size() >= MaxShardReadDiagnostics) {
                const auto replacement = FindReplaceable();
                if (replacement == Reads_.size()) {
                    ++Dropped_;
                    return;
                }
                Indexes_.erase(Reads_[replacement].GetShardId());
                Reads_[replacement].Clear();
                it = Indexes_.emplace(shardId, replacement).first;
                ++Dropped_;
            } else {
                const size_t index = Reads_.size();
                Reads_.emplace_back();
                it = Indexes_.emplace(shardId, index).first;
            }
        }
        auto& shard = Reads_[it->second];
        shard.SetShardId(shardId);
        if (!shard.GetStartTimeMs()) {
            shard.SetStartTimeMs(now.MilliSeconds());
        }
    }

    void OnFinish(ui64 shardId, ui64 rows, ui32 retries, ui32 nodeId = 0,
            Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
            bool finished = true, TInstant now = TInstant::Now()) {
        auto it = Indexes_.find(shardId);
        if (it == Indexes_.end()) {
            NKqpProto::TKqpShardReadStats candidate;
            candidate.SetShardId(shardId);
            candidate.SetFinishTimeMs(now.MilliSeconds());
            candidate.SetRows(rows);
            candidate.SetRetries(retries);
            candidate.SetStatus(status);
            candidate.SetFinished(finished || status != Ydb::StatusIds::SUCCESS);
            if (nodeId) {
                candidate.SetNodeId(nodeId);
            }
            if (status == Ydb::StatusIds::SUCCESS && retries == 0) {
                return;
            }
            const size_t replacement = FindLessInterestingThan(candidate);
            if (replacement == Reads_.size()) {
                return;
            }
            Indexes_.erase(Reads_[replacement].GetShardId());
            Reads_[replacement] = std::move(candidate);
            Indexes_.emplace(shardId, replacement);
            return;
        }
        auto& shard = Reads_[it->second];
        shard.SetFinishTimeMs(now.MilliSeconds());
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
    static auto Rank(const NKqpProto::TKqpShardReadStats& shard) {
        const bool failed = shard.GetStatus() != Ydb::StatusIds::STATUS_CODE_UNSPECIFIED
            && shard.GetStatus() != Ydb::StatusIds::SUCCESS;
        const ui64 durationMs = shard.GetStartTimeMs() && shard.GetFinishTimeMs() >= shard.GetStartTimeMs()
            ? shard.GetFinishTimeMs() - shard.GetStartTimeMs() : 0;
        return std::tuple(failed, shard.GetRetries() > 0, durationMs);
    }

    size_t FindReplaceable() const {
        size_t result = Reads_.size();
        for (size_t i = 0; i < Reads_.size(); ++i) {
            const auto& shard = Reads_[i];
            if (!shard.GetFinished() || std::get<0>(Rank(shard)) || shard.GetRetries() > 0) {
                continue;
            }
            if (result == Reads_.size() || Rank(shard) < Rank(Reads_[result])) {
                result = i;
            }
        }
        return result;
    }

    size_t FindLessInterestingThan(const NKqpProto::TKqpShardReadStats& candidate) const {
        size_t result = Reads_.size();
        for (size_t i = 0; i < Reads_.size(); ++i) {
            if (Rank(Reads_[i]) >= Rank(candidate)) {
                continue;
            }
            if (result == Reads_.size() || Rank(Reads_[i]) < Rank(Reads_[result])) {
                result = i;
            }
        }
        return result;
    }

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
