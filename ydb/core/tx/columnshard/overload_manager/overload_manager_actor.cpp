#include "overload_manager_actor.h"

#include <ydb/core/tx/columnshard/overload_manager/overload_manager_service.h>

namespace NKikimr::NColumnShard::NOverload {

TOverloadManager::TOverloadManager()
    : TActor(&TThis::StateMain) {
}

void TOverloadManager::Handle(const NOverload::TEvOverloadSubscribe::TPtr& ev) {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "TEvOverloadSubscribe");

    auto record = ev->Get();
    OverloadSubscribers.AddOverloadSubscriber(record->GetColumnShardInfo(), record->GetPipeServerInfo(), record->GetOverloadSubscriberInfo());
    TOverloadManagerServiceOperator::NotifyIfResourcesAvailable(true);
}

void TOverloadManager::Handle(const NOverload::TEvOverloadUnsubscribe::TPtr& ev) {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "TEvOverloadUnsubscribe");

    auto record = ev->Get();
    OverloadSubscribers.RemoveOverloadSubscriber(record->GetColumnShardInfo(), record->GetOverloadSubscriberInfo());
}

void TOverloadManager::Handle(const NOverload::TEvOverloadPipeServerDisconnected::TPtr& ev) {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "TEvOverloadPipeServerDisconnected");

    auto record = ev->Get();
    OverloadSubscribers.RemovePipeServer(record->GetColumnShardInfo(), record->GetPipeServerInfo());
}

void TOverloadManager::Handle(const NOverload::TEvOverloadResourcesReleased::TPtr&) {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "TEvOverloadResourcesReleased");

    OverloadSubscribers.NotifyAllOverloadSubscribers();
}

void TOverloadManager::Handle(const NOverload::TEvOverloadColumnShardDied::TPtr& ev) {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "TEvOverloadColumnShardDied");

    auto record = ev->Get();
    OverloadSubscribers.NotifyColumnShardSubscribers(record->GetColumnShardInfo());
}

} // namespace NKikimr::NColumnShard::NOverload
