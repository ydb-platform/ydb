#include "overload_manager_service.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/kqp/query_data/kqp_predictor.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_service.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_actor.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_events.h>

namespace NKikimr::NColumnShard::NOverload {

namespace {

std::atomic_uint64_t DEFAULT_WRITES_IN_FLY_LIMIT{ 0 };
std::atomic_uint64_t DEFAULT_WRITES_SIZE_IN_FLY_LIMIT{ 0 };

using NFlowControl::TFlowControlManagerServiceOperator;

void SendToOverloadManager(NActors::IEventBase* event) {
    auto* actorSystem = NActors::TActivationContext::ActorSystem();
    if (!actorSystem) {
        delete event;
        return;
    }
    actorSystem->Send(new NActors::IEventHandle(TOverloadManagerServiceOperator::MakeServiceId(), NActors::TActorId(), event));
}

bool TrySendToOverloadManager(NActors::IEventBase* event) {
    auto* actorSystem = NActors::TActivationContext::ActorSystem();
    if (!actorSystem) {
        delete event;
        return false;
    }
    actorSystem->Send(new NActors::IEventHandle(TOverloadManagerServiceOperator::MakeServiceId(), NActors::TActorId(), event));
    return true;
}

}   // namespace

TPositiveControlInteger TOverloadManagerServiceOperator::WritesInFlight;
TPositiveControlInteger TOverloadManagerServiceOperator::WritesSizeInFlight;
std::atomic<EResourcesStatus> TOverloadManagerServiceOperator::ResourcesStatus{ EResourcesStatus::Ok };
std::atomic<bool> TOverloadManagerServiceOperator::CompactionOverloaded{ false };

ui64 TOverloadManagerServiceOperator::GetShardWritesInFlyLimit() {
    if (DEFAULT_WRITES_IN_FLY_LIMIT.load() == 0) {
        uint64_t oldValue = 0;
        const uint64_t newValue = std::max(NKqp::TStagePredictor::GetPossibleMaxLimitThreads() * 10000, ui32(100000));
        DEFAULT_WRITES_IN_FLY_LIMIT.compare_exchange_strong(oldValue, newValue);
    }
    return (HasAppData() && AppDataVerified().ColumnShardConfig.HasWritingInFlightRequestsCountLimit())
               ? AppDataVerified().ColumnShardConfig.GetWritingInFlightRequestsCountLimit()
               : DEFAULT_WRITES_IN_FLY_LIMIT.load();
}

ui64 TOverloadManagerServiceOperator::GetShardWritesSizeInFlyLimit() {
    if (DEFAULT_WRITES_SIZE_IN_FLY_LIMIT.load() == 0) {
        uint64_t oldValue = 0;
        const uint64_t newValue = NKqp::TStagePredictor::GetPossibleMaxLimitThreads() * 20_MB;
        DEFAULT_WRITES_SIZE_IN_FLY_LIMIT.compare_exchange_strong(oldValue, newValue);
    }
    return (HasAppData() && AppDataVerified().ColumnShardConfig.HasWritingInFlightRequestBytesLimit())
               ? AppDataVerified().ColumnShardConfig.GetWritingInFlightRequestBytesLimit()
               : DEFAULT_WRITES_SIZE_IN_FLY_LIMIT.load();
}

bool TOverloadManagerServiceOperator::AreWriteResourcesBelowSoftLimit() {
    return WritesInFlight.Val() <= GetShardWritesInFlyLimit() * WritesInFlightSoftLimitCoefficient &&
           WritesSizeInFlight.Val() <= GetShardWritesSizeInFlyLimit() * WritesInFlightSizeSoftLimitCoefficient;
}

NActors::TActorId TOverloadManagerServiceOperator::MakeServiceId() {
    return NActors::TActorId(0, "OverloadMng");
}

std::unique_ptr<NActors::IActor> TOverloadManagerServiceOperator::CreateService(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup) {
    return std::make_unique<TOverloadManager>(countersGroup);
}

void TOverloadManagerServiceOperator::SetCompactionOverloaded(bool overloaded) {
    CompactionOverloaded.store(overloaded);
}

void TOverloadManagerServiceOperator::SyncNodeOverloadPublication() {
    if (!TFlowControlManagerServiceOperator::IsEnabled()) {
        return;
    }
    SendToOverloadManager(new TEvSyncNodeOverloadPublication());
}

void TOverloadManagerServiceOperator::NotifyIfResourcesAvailable(bool force) {
    const auto previousStatus = ResourcesStatus.load();
    if ((force || previousStatus != EResourcesStatus::Ok) && AreWriteResourcesBelowSoftLimit()) {
        ResourcesStatus = EResourcesStatus::Ok;
        SendToOverloadManager(new NOverload::TEvOverloadResourcesReleased());
        if (previousStatus != EResourcesStatus::Ok) {
            SyncNodeOverloadPublication();
        }
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
        SyncNodeOverloadPublication();
    } else if (resWriteSizeInFlight >= GetShardWritesSizeInFlyLimit()) {
        ResourcesStatus = EResourcesStatus::WritesSizeInFlyLimitReached;
        SyncNodeOverloadPublication();
    }

    return EResourcesStatus::Ok;
}

void TOverloadManagerServiceOperator::ReleaseResources(ui64 writesCount, ui64 writesSize) {
    WritesInFlight.Sub(writesCount);
    WritesSizeInFlight.Sub(writesSize);

    NotifyIfResourcesAvailable(false);
}

bool TOverloadManagerServiceOperator::ReportCompactionOverload(ui64 tabletId, bool overloaded) {
    if (!TFlowControlManagerServiceOperator::IsEnabled()) {
        return false;
    }
    return TrySendToOverloadManager(new TEvCompactionOverloadState(tabletId, overloaded));
}

}   // namespace NKikimr::NColumnShard::NOverload
