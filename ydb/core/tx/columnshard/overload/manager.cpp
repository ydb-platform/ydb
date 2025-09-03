#include "manager.h"

namespace NKikimr::NColumnShard::NOverload {

void TOverloadManager::Handle(const TEvOverloadSubscribe::TPtr& ev) {
    auto record = ev->Get();
    OverloadSubscribers.AddOverloadSubscriber(record->GetColumnShardInfo(), record->GetPipeServerInfo(), record->GetOverloadSubscriberInfo());
}

void TOverloadManager::Handle(const TEvOverloadUnsubscribe::TPtr& ev) {
    auto record = ev->Get();
    OverloadSubscribers.RemoveOverloadSubscriber(record->GetColumnShardInfo(), record->GetOverloadSubscriberInfo());
}

void TOverloadManager::Handle(const TEvOverloadPipeServerDisconnected::TPtr& ev) {
    auto record = ev->Get();
    OverloadSubscribers.RemovePipeServer(record->GetColumnShardInfo(), record->GetPipeServerInfo());
}

void TOverloadManager::Handle(const TEvOverloadResourcesReleased::TPtr&) {
    OverloadSubscribers.NotifyAllOverloadSubscribers();
}

} // namespace NKikimr::NColumnShard::NOverload
