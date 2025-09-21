#include "overload_manager_service.h"

#include <ydb/core/tx/columnshard/overload_manager/overload_manager_actor.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_events.h>
#include <ydb/core/kqp/query_data/kqp_predictor.h>

namespace NKikimr::NColumnShard::NOverload {

namespace {

std::atomic_uint64_t DEFAULT_WRITES_IN_FLY_LIMIT{0};
std::atomic_uint64_t DEFAULT_WRITES_SIZE_IN_FLY_LIMIT{0};

} // namespace

TPositiveControlInteger TOverloadManagerServiceOperator::WritesInFlight;
TPositiveControlInteger TOverloadManagerServiceOperator::WritesSizeInFlight;
std::atomic<EResourcesStatus> TOverloadManagerServiceOperator::ResourcesStatus{EResourcesStatus::Ok};

ui64 TOverloadManagerServiceOperator::GetShardWritesInFlyLimit() {
    if (DEFAULT_WRITES_IN_FLY_LIMIT.load() == 0) {
        ui64 oldValue = 0;
        const ui64 newValue = std::max(NKqp::TStagePredictor::GetUsableThreads() * 10000, ui32(100000));
        DEFAULT_WRITES_IN_FLY_LIMIT.compare_exchange_strong(oldValue, newValue);
    }
    return (HasAppData() && AppDataVerified().ColumnShardConfig.HasWritingInFlightRequestsCountLimit()) ? AppDataVerified().ColumnShardConfig.GetWritingInFlightRequestsCountLimit()
                                                                                                        : DEFAULT_WRITES_IN_FLY_LIMIT.load();
}

ui64 TOverloadManagerServiceOperator::GetShardWritesSizeInFlyLimit() {
    if (DEFAULT_WRITES_SIZE_IN_FLY_LIMIT.load() == 0) {
        ui64 oldValue = 0;
        const ui64 newValue = NKqp::TStagePredictor::GetUsableThreads() * 20_MB;
        DEFAULT_WRITES_SIZE_IN_FLY_LIMIT.compare_exchange_strong(oldValue, newValue);
    }
    return (HasAppData() && AppDataVerified().ColumnShardConfig.HasWritingInFlightRequestBytesLimit()) ?
            AppDataVerified().ColumnShardConfig.GetWritingInFlightRequestBytesLimit() :
            DEFAULT_WRITES_SIZE_IN_FLY_LIMIT.load();
}

NActors::TActorId TOverloadManagerServiceOperator::MakeServiceId() {
    return NActors::TActorId(0, "OverloadMng");
}

std::unique_ptr<NActors::IActor> TOverloadManagerServiceOperator::CreateService(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup) {
    return std::make_unique<TOverloadManager>(countersGroup);
}

void TOverloadManagerServiceOperator::NotifyIfResourcesAvailable(bool force) {
    if ((force || ResourcesStatus.load() != EResourcesStatus::Ok) && WritesInFlight.Val() <= GetShardWritesInFlyLimit() * WritesInFlightSoftLimitCoefficient &&
        WritesSizeInFlight.Val() <= GetShardWritesSizeInFlyLimit() * WritesInFlightSizeSoftLimitCoefficient) {
        ResourcesStatus = EResourcesStatus::Ok;
        auto& context = NActors::TActorContext::AsActorContext();
        context.Send(MakeServiceId(), new NOverload::TEvOverloadResourcesReleased());
    }
}

EResourcesStatus TOverloadManagerServiceOperator::RequestResources(ui64 writesCount, ui64 writesSize) {
    if (auto status = ResourcesStatus.load(); status != EResourcesStatus::Ok) {
        return status;
    }

    auto resWritesInFlight = WritesInFlight.Add(writesCount);
    auto resWriteSizeInFlight = WritesSizeInFlight.Add(writesSize);
    if (resWritesInFlight >= GetShardWritesInFlyLimit()) {
        ResourcesStatus = EResourcesStatus::WritesInFlyLimitReached;
    } else if (resWriteSizeInFlight >= GetShardWritesSizeInFlyLimit()) {
        ResourcesStatus = EResourcesStatus::WritesSizeInFlyLimitReached;
    }

    return EResourcesStatus::Ok;
}

void TOverloadManagerServiceOperator::ReleaseResources(ui64 writesCount, ui64 writesSize) {
    WritesInFlight.Sub(writesCount);
    WritesSizeInFlight.Sub(writesSize);

    NotifyIfResourcesAvailable(false);
}

} // namespace NKikimr::NColumnShard::NOverload