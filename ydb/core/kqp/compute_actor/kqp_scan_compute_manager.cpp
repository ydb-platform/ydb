#include "kqp_scan_compute_manager.h"
#include <ydb/library/wilson_ids/wilson.h>
#include <util/string/builder.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_COMPUTE

namespace NKikimr::NKqp::NScanPrivate {

TShardState::TPtr TInFlightShards::Put(TShardState&& state) {
    YDB_LOG_DEBUG("Added shard to in-flight scans",
        {"event", "put_inflight"},
        {"tabletId", state.TabletId},
        {"state", state.State},
        {"gen", state.Generation});
    TScanShardsStatistics::OnScansDiff(Shards.size(), GetScansCount());
    MutableStatistics(state.TabletId).MutableStatistics(0).SetStartInstant(Now());

    TShardState::TPtr result = std::make_shared<TShardState>(std::move(state));
    YDB_LOG_ENSURE(Shards.emplace(result->TabletId, result).second,
        {"tabletId", result->TabletId});
    return result;
}

std::vector<std::unique_ptr<TComputeTaskData>> TShardScannerInfo::OnReceiveData(TEvKqpCompute::TEvScanData& data, const std::shared_ptr<TShardScannerInfo>& selfPtr) {
    if (!data.Finished) {
        YDB_LOG_ENSURE(!NeedAck);
        NeedAck = true;
    } else {
        Finished = true;
    }
    YDB_LOG_ENSURE(ActorId);
    YDB_LOG_ENSURE(!DataChunksInFlightCount,
        {"dataChunksInFlightCount", DataChunksInFlightCount});
    std::vector<std::unique_ptr<TComputeTaskData>> result;
    if (data.IsEmpty()) {
        YDB_LOG_ENSURE(data.Finished);
        result.emplace_back(std::make_unique<TComputeTaskData>(selfPtr, std::make_unique<TEvScanExchange::TEvSendData>(TabletId, data)));
    } else if (data.SplittedBatches.size() > 1) {
        YDB_LOG_ENSURE(data.ArrowBatch);
        for (auto&& i : data.SplittedBatches) {
            result.emplace_back(std::make_unique<TComputeTaskData>(selfPtr, std::make_unique<TEvScanExchange::TEvSendData>(TabletId, data, data.ArrowBatch, std::move(i))));
        }
    } else if (data.ArrowBatch) {
        result.emplace_back(std::make_unique<TComputeTaskData>(selfPtr, std::make_unique<TEvScanExchange::TEvSendData>(TabletId, data, data.ArrowBatch)));
    } else {
        result.emplace_back(std::make_unique<TComputeTaskData>(selfPtr, std::make_unique<TEvScanExchange::TEvSendData>(TabletId, data, std::move(data.Rows))));
    }
    YDB_LOG_DEBUG("Received scan data chunks from shard scanner",
        {"event", "receive_data"},
        {"actorId", ActorId},
        {"countChunks", result.size()});
    DataChunksInFlightCount = result.size();
    return result;
}

}
