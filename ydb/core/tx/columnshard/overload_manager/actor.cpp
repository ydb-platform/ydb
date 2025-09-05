#include "actor.h"

namespace NKikimr::NColumnShard::NOverload {

TOverloadManager::TOverloadManager()
    : TActor(&TThis::StateMain) {
}

void TOverloadManager::Handle(const NOverload::TEvOverloadSubscribe::TPtr& ev) {
    auto record = ev->Get();
    OverloadSubscribers.AddOverloadSubscriber(record->GetColumnShardInfo(), record->GetPipeServerInfo(), record->GetOverloadSubscriberInfo());
}

void TOverloadManager::Handle(const NOverload::TEvOverloadUnsubscribe::TPtr& ev) {
    auto record = ev->Get();
    OverloadSubscribers.RemoveOverloadSubscriber(record->GetColumnShardInfo(), record->GetOverloadSubscriberInfo());
}

void TOverloadManager::Handle(const NOverload::TEvOverloadPipeServerDisconnected::TPtr& ev) {
    auto record = ev->Get();
    OverloadSubscribers.RemovePipeServer(record->GetColumnShardInfo(), record->GetPipeServerInfo());
}

void TOverloadManager::Handle(const NOverload::TEvOverloadResourcesReleased::TPtr&) {
    OverloadSubscribers.NotifyAllOverloadSubscribers();
}

void TOverloadManager::Handle(const NOverload::TEvOverloadColumnShardDied::TPtr& ev) {
    auto record = ev->Get();
    OverloadSubscribers.NotifyColumnShardSubscribers(record->GetColumnShardInfo());
}

void TOverloadManager::Handle(const NActors::TEvents::TEvPoison::TPtr&) {
    OverloadSubscribers.NotifyAllOverloadSubscribers();
    PassAway();
}

} // namespace NKikimr::NColumnShard::NOverload
