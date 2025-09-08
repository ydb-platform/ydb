#include "overload_manager_service.h"

#include <ydb/core/tx/columnshard/overload_manager/overload_manager_actor.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_events.h>

namespace NKikimr::NColumnShard::NOverload {

namespace {

constexpr ui64 DEFAULT_WRITES_IN_FLY_LIMIT = 1000000;
constexpr ui64 DEFAULT_WRITES_SIZE_IN_FLY_LIMIT = 2 * (((ui64)1) << 30);

} // namespace

TPositiveControlInteger TOverloadManagerServiceOperator::WritesInFlight;
TPositiveControlInteger TOverloadManagerServiceOperator::WritesSizeInFlight;
std::atomic_bool TOverloadManagerServiceOperator::LimitReached = false;

ui64 TOverloadManagerServiceOperator::GetShardWritesInFlyLimit() {
    return HasAppData() ? AppDataVerified().ColumnShardConfig.GetWritingInFlightRequestsCountLimit() : DEFAULT_WRITES_IN_FLY_LIMIT;
}

ui64 TOverloadManagerServiceOperator::GetShardWritesSizeInFlyLimit() {
    return HasAppData() ? AppDataVerified().ColumnShardConfig.GetWritingInFlightRequestBytesLimit() : DEFAULT_WRITES_SIZE_IN_FLY_LIMIT;
}

void TOverloadManagerServiceOperator::NotifyIfResourcesAvailable() {
    if (LimitReached &&
        WritesInFlight.Val() <= GetShardWritesInFlyLimit() * WritesInFlightSoftLimitCoefficient &&
        WritesSizeInFlight.Val() <= GetShardWritesSizeInFlyLimit() * WritesInFlightSizeSoftLimitCoefficient) {
        if (LimitReached.exchange(false)) {
            auto& context = NActors::TActorContext::AsActorContext();
            context.Send(MakeServiceId(), new NOverload::TEvOverloadResourcesReleased());
        }
    }
}

NActors::TActorId TOverloadManagerServiceOperator::MakeServiceId() {
    return NActors::TActorId(0, "OverloadMng");
}

NActors::IActor* TOverloadManagerServiceOperator::CreateService() {
    return new TOverloadManager();
}

bool TOverloadManagerServiceOperator::RequestResources(ui64 writesCount, ui64 writesSize) {
    if (LimitReached) {
        return false;
    }

    auto resWritesInFlight = WritesInFlight.Add(writesCount);
    auto resWriteSizeInFlight = WritesSizeInFlight.Add(writesSize);
    if (resWritesInFlight >= GetShardWritesInFlyLimit() || resWriteSizeInFlight >= GetShardWritesSizeInFlyLimit()) {
        LimitReached = true;
    }

    return true;
}

void TOverloadManagerServiceOperator::ReleaseResources(ui64 writesCount, ui64 writesSize) {
    WritesInFlight.Sub(writesCount);
    WritesSizeInFlight.Sub(writesSize);

    NotifyIfResourcesAvailable();
}

} // namespace NKikimr::NColumnShard::NOverload