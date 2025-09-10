#include "overload_manager_service.h"

#include <ydb/core/tx/columnshard/overload_manager/overload_manager_actor.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_events.h>
#include <ydb/core/kqp/query_data/kqp_predictor.h>

namespace NKikimr::NColumnShard::NOverload {

namespace {

std::atomic_uint64_t DEFAULT_WRITES_IN_FLY_LIMIT{0};
std::atomic_uint64_t DEFAULT_WRITES_SIZE_IN_FLY_LIMIT{0};

} // namespace

// TPositiveControlInteger TOverloadManagerServiceOperator::WritesInFlight;
// TPositiveControlInteger TOverloadManagerServiceOperator::WritesSizeInFlight;
// std::atomic_bool TOverloadManagerServiceOperator::LimitReached = false;
ui64 TOverloadManagerServiceOperator::WritesInFlight = 0;
ui64 TOverloadManagerServiceOperator::WritesSizeInFlight = 0;
bool TOverloadManagerServiceOperator::LimitReached = false;
std::mutex TOverloadManagerServiceOperator::Mutex;

ui64 TOverloadManagerServiceOperator::GetShardWritesInFlyLimit() {
    if (DEFAULT_WRITES_IN_FLY_LIMIT.load() == 0) {
        ui64 oldValue = 0;
        const ui64 newValue = std::max(NKqp::TStagePredictor::GetUsableThreads() * 200, ui32(1000));
        DEFAULT_WRITES_IN_FLY_LIMIT.compare_exchange_strong(oldValue, newValue);
    }
    return HasAppData() ? AppDataVerified().ColumnShardConfig.GetWritingInFlightRequestsCountLimit() : DEFAULT_WRITES_IN_FLY_LIMIT.load();
}

ui64 TOverloadManagerServiceOperator::GetShardWritesSizeInFlyLimit() {
    if (DEFAULT_WRITES_SIZE_IN_FLY_LIMIT.load() == 0) {
        ui64 oldValue = 0;
        const ui64 newValue = NKqp::TStagePredictor::GetUsableThreads() * 20_MB;
        DEFAULT_WRITES_SIZE_IN_FLY_LIMIT.compare_exchange_strong(oldValue, newValue);
    }
    return HasAppData() ? AppDataVerified().ColumnShardConfig.GetWritingInFlightRequestBytesLimit() : DEFAULT_WRITES_SIZE_IN_FLY_LIMIT.load();
}

NActors::TActorId TOverloadManagerServiceOperator::MakeServiceId() {
    return NActors::TActorId(0, "OverloadMng");
}

NActors::IActor* TOverloadManagerServiceOperator::CreateService() {
    return new TOverloadManager();
}

void TOverloadManagerServiceOperator::NotifyIfResourcesAvailable(bool force) {
    bool send = false;
    {
        std::lock_guard<std::mutex> guard(Mutex);
        send = (force || LimitReached) &&
            WritesInFlight <= GetShardWritesInFlyLimit() * WritesInFlightSoftLimitCoefficient &&
            WritesSizeInFlight <= GetShardWritesSizeInFlyLimit() * WritesInFlightSizeSoftLimitCoefficient;
        if (send) {
            LimitReached = false;
        }
    }
    if (send) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "Limit Ok, releasing");
        auto& context = NActors::TActorContext::AsActorContext();
        context.Send(MakeServiceId(), new NOverload::TEvOverloadResourcesReleased());
    }
}

bool TOverloadManagerServiceOperator::RequestResources(ui64 writesCount, ui64 writesSize) {
    bool already = false;
    bool limitReached = false;
    do {
        std::lock_guard<std::mutex> guard(Mutex);
        if (LimitReached) {
            already = true;
            break;
        }

        // auto resWritesInFlight = WritesInFlight.Add(writesCount);
        // auto resWriteSizeInFlight = WritesSizeInFlight.Add(writesSize);
        // if (resWritesInFlight >= GetShardWritesInFlyLimit() || resWriteSizeInFlight >= GetShardWritesSizeInFlyLimit()) {
        WritesInFlight += writesCount;
        WritesSizeInFlight += writesSize;
        if (WritesInFlight >= GetShardWritesInFlyLimit() || WritesSizeInFlight >= GetShardWritesSizeInFlyLimit()) {
            LimitReached = true;
            limitReached = true;
        }
    } while (false);

    if (already) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "Limit already reached");
    } else if (limitReached) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "Limit is reached now, locking new");
    } else {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "Requesting resources without limit");
    }

    return !already;
}

void TOverloadManagerServiceOperator::ReleaseResources(ui64 writesCount, ui64 writesSize) {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "Releasing resources");
    {
        std::lock_guard<std::mutex> guard(Mutex);

        // WritesInFlight.Sub(writesCount);
        // WritesSizeInFlight.Sub(writesSize);
        WritesInFlight -= writesCount;
        WritesSizeInFlight -= writesSize;
    }

    NotifyIfResourcesAvailable(false);
}

} // namespace NKikimr::NColumnShard::NOverload