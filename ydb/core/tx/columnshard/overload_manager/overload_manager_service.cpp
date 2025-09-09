#include "overload_manager_service.h"

#include <ydb/core/tx/columnshard/overload_manager/overload_manager_actor.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_events.h>

namespace NKikimr::NColumnShard::NOverload {

namespace {

constexpr ui64 DEFAULT_WRITES_IN_FLY_LIMIT = 1000000;
constexpr ui64 DEFAULT_WRITES_SIZE_IN_FLY_LIMIT = 2 * (((ui64)1) << 30);

} // namespace

// TPositiveControlInteger TOverloadManagerServiceOperator::WritesInFlight;
// TPositiveControlInteger TOverloadManagerServiceOperator::WritesSizeInFlight;
// std::atomic_bool TOverloadManagerServiceOperator::LimitReached = false;
ui64 TOverloadManagerServiceOperator::WritesInFlight = 0;
ui64 TOverloadManagerServiceOperator::WritesSizeInFlight = 0;
bool TOverloadManagerServiceOperator::LimitReached = false;
std::mutex TOverloadManagerServiceOperator::Mutex;

ui64 TOverloadManagerServiceOperator::GetShardWritesInFlyLimit() {
    return HasAppData() ? AppDataVerified().ColumnShardConfig.GetWritingInFlightRequestsCountLimit() : DEFAULT_WRITES_IN_FLY_LIMIT;
}

ui64 TOverloadManagerServiceOperator::GetShardWritesSizeInFlyLimit() {
    return HasAppData() ? AppDataVerified().ColumnShardConfig.GetWritingInFlightRequestBytesLimit() : DEFAULT_WRITES_SIZE_IN_FLY_LIMIT;
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
            return false;
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