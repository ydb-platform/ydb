#include "kqp_shard_tracing.h"

#include "kqp_query_tracing.h"
#include "kqp_trace_settings.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/wilson_ids/wilson.h>

#include <algorithm>
#include <vector>

namespace NKikimr::NKqp {

bool TShardReadTrace::Enabled(const NWilson::TSpan& parent) {
    return parent && parent.GetTraceId().GetVerbosity() >= TComponentTracingLevels::TQueryProcessor::Diagnostic;
}

NWilson::TTraceId TShardReadTrace::Start(const NWilson::TSpan& parent, ui64 shardId, ui64 readId) {
    if (!Enabled(parent)) {
        return parent.GetTraceId();
    }
    ++TotalReads_;
    if (Reads_.size() >= NQueryTraceSettings::MAX_ACTIVE_SHARD_READS) {
        ++UntracedReads_;
        return parent.GetTraceId();
    }
    auto& read = Reads_[readId];
    read.Start = parent.GetActorSystem()->Monotonic();
    read.ShardId = shardId;
    read.Span = parent.CreateChild(TComponentTracingLevels::TQueryProcessor::Diagnostic,
        "Read shard", NWilson::EFlags::AUTO_END);
    read.Span.Attribute("ydb.shard_id", static_cast<i64>(shardId));
    read.Span.Attribute("ydb.read_id", static_cast<i64>(readId));
    read.Span.Attribute("ydb.code.component", TString("KqpShardRead"));
    read.Span.Attribute("ydb.peer.actor.type", TString("DataShard"));
    return read.Span.GetTraceId();
}

void TShardReadTrace::ReadResult(NWilson::TSpan& parent, ui64 shardId, ui32 nodeId, ui64 readId,
        ui64 rows, Ydb::StatusIds::StatusCode status, bool finished) {
    if (!Enabled(parent)) {
        return;
    }
    const auto it = Reads_.find(readId);
    if (it != Reads_.end()) {
        it->second.NodeId = nodeId;
        it->second.Rows += rows;
        if (finished || status != Ydb::StatusIds::SUCCESS) {
            Complete(readId, status, finished);
        }
    } else {
        auto& shard = Shards_[shardId];
        shard.TimingIncomplete = true;
        shard.Rows += rows;
        shard.NodeId = nodeId;
        shard.LastReadId = readId;
        shard.LastResponse = parent.GetActorSystem()->Monotonic();
        shard.LastStatus = status;
        shard.Reads += finished || status != Ydb::StatusIds::SUCCESS;
        shard.FailedReads += status != Ydb::StatusIds::SUCCESS;
        RetainShards();
    }
}

void TShardReadTrace::Complete(ui64 readId, Ydb::StatusIds::StatusCode status, bool finished) {
    const auto it = Reads_.find(readId);
    if (it == Reads_.end()) {
        return;
    }
    auto& read = it->second;
    auto& shard = Shards_[read.ShardId];
    if (!shard.FirstRequest || read.Start < shard.FirstRequest) {
        shard.FirstRequest = read.Start;
    }
    shard.LastResponse = read.Span.GetActorSystem()->Monotonic();
    shard.Rows += read.Rows;
    shard.NodeId = read.NodeId;
    shard.LastReadId = readId;
    shard.LastStatus = status;
    ++shard.Reads;
    shard.FailedReads += status != Ydb::StatusIds::SUCCESS && status != Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    shard.StoppedReads += status == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    read.Span.Attribute("ydb.node_id", static_cast<i64>(read.NodeId));
    read.Span.Attribute("ydb.rows", static_cast<i64>(read.Rows));
    read.Span.Attribute("ydb.finished", finished && status == Ydb::StatusIds::SUCCESS);
    read.Span.Attribute("ydb.timing_boundary", TString(status == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED
        ? "request_to_stop" : "request_to_last_message"));
    EndQueryTraceSpan(read.Span, status);
    Reads_.erase(it);
    RetainShards();
}

void TShardReadTrace::Retry(NWilson::TSpan& parent, ui64 shardId, ui64 readId) {
    if (!Enabled(parent)) {
        return;
    }
    Stop(readId);
    auto& shard = Shards_[shardId];
    ++shard.Retries;
    shard.LastReadId = readId;
    RetainShards();
}

void TShardReadTrace::Stop(ui64 readId) {
    Complete(readId, Ydb::StatusIds::STATUS_CODE_UNSPECIFIED, false);
}

void TShardReadTrace::RetainShards() {
    if (Shards_.size() <= NQueryTraceSettings::MAX_RETAINED_READ_SHARDS) {
        return;
    }
    const auto least = std::min_element(Shards_.begin(), Shards_.end(), [](const auto& lhs, const auto& rhs) {
        return std::tuple(lhs.second.Rank(), lhs.first) < std::tuple(rhs.second.Rank(), rhs.first);
    });
    Shards_.erase(least);
    ++EvictedShards_;
}

void TShardReadTrace::Finish(NWilson::TSpan& parent) {
    while (!Reads_.empty()) {
        Stop(Reads_.begin()->first);
    }
    if (Enabled(parent) && TotalReads_) {
        std::vector<std::pair<ui64, TShard>> ranked(Shards_.begin(), Shards_.end());
        std::sort(ranked.begin(), ranked.end(), [](const auto& lhs, const auto& rhs) {
            return std::tuple(lhs.second.Rank(), lhs.first) > std::tuple(rhs.second.Rank(), rhs.first);
        });
        const size_t retained = std::min(ranked.size(), NQueryTraceSettings::MAX_INTERESTING_READ_SHARDS);
        for (size_t i = 0; i < retained; ++i) {
            const auto& [id, shard] = ranked[i];
            parent.Event("Shard read statistics", {
                {"ydb.shard_id", static_cast<i64>(id)},
                {"ydb.node_id", static_cast<i64>(shard.NodeId)},
                {"ydb.read_id", static_cast<i64>(shard.LastReadId)},
                {"ydb.rows", static_cast<i64>(shard.Rows)},
                {"ydb.reads", static_cast<i64>(shard.Reads)},
                {"ydb.failed_reads", static_cast<i64>(shard.FailedReads)},
                {"ydb.stopped_reads", static_cast<i64>(shard.StoppedReads)},
                {"ydb.read_retries", static_cast<i64>(shard.Retries)},
                {"ydb.duration_us", static_cast<i64>(shard.DurationUs())},
                {"ydb.duration.measured", bool(shard.FirstRequest) && !shard.TimingIncomplete},
                {"ydb.timing_boundary", TString("first_request_to_last_response_or_stop")},
                {"ydb.status_code", Ydb::StatusIds::StatusCode_Name(shard.LastStatus)},
            });
        }
        parent.Attribute("ydb.shard_reads", static_cast<i64>(TotalReads_));
        parent.Attribute("ydb.shard_reads_untraced", static_cast<i64>(UntracedReads_));
        parent.Attribute("ydb.shard_summaries_dropped", static_cast<i64>(EvictedShards_ + ranked.size() - retained));
        parent.Attribute("ydb.shard_stats_incomplete", bool(UntracedReads_ || EvictedShards_));
    }
    Shards_.clear();
    TotalReads_ = 0;
    UntracedReads_ = 0;
    EvictedShards_ = 0;
}

} // namespace NKikimr::NKqp
