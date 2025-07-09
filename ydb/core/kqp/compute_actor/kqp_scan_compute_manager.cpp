#include "kqp_scan_compute_manager.h"
#include <ydb/library/wilson_ids/wilson.h>
#include <util/string/builder.h>

namespace NKikimr::NKqp::NScanPrivate {

TShardState::TPtr TInFlightShards::Put(TShardState&& state) {
    AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "put_inflight")("tablet_id", state.TabletId)("state", state.State)("gen", state.Generation);
    TScanShardsStatistics::OnScansDiff(Shards.size(), GetScansCount());
    MutableStatistics(state.TabletId).MutableStatistics(0).SetStartInstant(Now());

    TShardState::TPtr result = std::make_shared<TShardState>(std::move(state));
    AFL_ENSURE(Shards.emplace(result->TabletId, result).second)("tablet_id", result->TabletId);
    return result;
}

std::vector<std::unique_ptr<TComputeTaskData>> TShardScannerInfo::OnReceiveData(TEvKqpCompute::TEvScanData& data, const std::shared_ptr<TShardScannerInfo>& selfPtr) {
    if (!data.Finished) {
        AFL_ENSURE(!NeedAck);
        NeedAck = true;
    } else {
        Finished = true;
    }
    AFL_ENSURE(ActorId);
    AFL_ENSURE(!DataChunksInFlightCount)("data_chunks_in_flightCount", DataChunksInFlightCount);
    std::vector<std::unique_ptr<TComputeTaskData>> result;
    if (data.IsEmpty()) {
        AFL_ENSURE(data.Finished);
        result.emplace_back(std::make_unique<TComputeTaskData>(selfPtr, std::make_unique<TEvScanExchange::TEvSendData>(TabletId, data.LocksInfo, data.Finished)));
    } else if (data.SplittedBatches.size() > 1) {
        AFL_ENSURE(data.ArrowBatch);
        for (auto&& i : data.SplittedBatches) {
            result.emplace_back(std::make_unique<TComputeTaskData>(selfPtr, std::make_unique<TEvScanExchange::TEvSendData>(data.ArrowBatch, TabletId, std::move(i), data.LocksInfo, data.Finished)));
        }
    } else if (data.ArrowBatch) {
        result.emplace_back(std::make_unique<TComputeTaskData>(selfPtr, std::make_unique<TEvScanExchange::TEvSendData>(data.ArrowBatch, TabletId, data.LocksInfo, data.Finished)));
    } else {
        result.emplace_back(std::make_unique<TComputeTaskData>(selfPtr, std::make_unique<TEvScanExchange::TEvSendData>(std::move(data.Rows), TabletId, data.LocksInfo, data.Finished)));
    }
    AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "receive_data")("actor_id", ActorId)("count_chunks", result.size());
    DataChunksInFlightCount = result.size();
    return result;
}

}
