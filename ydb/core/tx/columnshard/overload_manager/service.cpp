#include "service.h"

#include "actor.h"
#include "events.h"

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
    if (resWritesInFlight < GetShardWritesInFlyLimit() && resWriteSizeInFlight < GetShardWritesSizeInFlyLimit()) {
        return true;
    }

    LimitReached = true;
    WritesInFlight.Sub(writesCount);
    WritesSizeInFlight.Sub(writesSize);

    return false;
}

void TOverloadManagerServiceOperator::ReleaseResources(ui64 writesCount, ui64 writesSize) {
    auto resWritesInFlight = WritesInFlight.Sub(writesCount);
    auto resWriteSizeInFlight = WritesSizeInFlight.Sub(writesSize);

    if (LimitReached &&
        resWritesInFlight < GetShardWritesInFlyLimit() * WritesInFlightSoftLimitCoefficient &&
        resWriteSizeInFlight < GetShardWritesSizeInFlyLimit() * WritesInFlightSizeSoftLimitCoefficient) {
        if (LimitReached.exchange(false)) {
            auto& context = NActors::TActorContext::AsActorContext();
            context.Send(MakeServiceId(), new NOverload::TEvOverloadResourcesReleased());
        }
    }
}

} // namespace NKikimr::NColumnShard::NOverload