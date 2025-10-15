#include "partition.h"

#include <ydb/core/persqueue/pqtablet/partition/mlp/mlp.h>

namespace NKikimr::NPQ {

void TPartition::HandleOnInit(TEvPersQueue::TEvMLPReadRequest::TPtr& ev) {
   MLPPendingEvents.emplace_back(ev);
}

void TPartition::HandleOnInit(TEvPersQueue::TEvMLPCommitRequest::TPtr& ev) {
   MLPPendingEvents.emplace_back(ev);
}

void TPartition::HandleOnInit(TEvPersQueue::TEvMLPReleaseRequest::TPtr& ev) {
   MLPPendingEvents.emplace_back(ev);
}

void TPartition::HandleOnInit(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest::TPtr& ev) {
   MLPPendingEvents.emplace_back(ev);
}

template<typename TEventHandle>
void TPartition::ForwardToMLPConsumer(const TString& consumer, TAutoPtr<TEventHandle>& ev) {
    auto it = MLPConsumers.find(consumer);
    if (it == MLPConsumers.end()) {
        // TODO reply error
        return;
    }

    auto& consumerInfo = it->second;
    Forward(ev, consumerInfo.ActorId);
}

void TPartition::Handle(TEvPersQueue::TEvMLPReadRequest::TPtr& ev) {
    ForwardToMLPConsumer(ev->Get()->GetConsumer(), ev);
}

void TPartition::Handle(TEvPersQueue::TEvMLPCommitRequest::TPtr& ev) {
    ForwardToMLPConsumer(ev->Get()->GetConsumer(), ev);
}

void TPartition::Handle(TEvPersQueue::TEvMLPReleaseRequest::TPtr& ev) {
    ForwardToMLPConsumer(ev->Get()->GetConsumer(), ev);
}

void TPartition::Handle(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest::TPtr& ev) {
    ForwardToMLPConsumer(ev->Get()->GetConsumer(), ev);
}

void TPartition::ProcessMLPPendingEvents() {
    LOG_D("Process MLP pending events. Count " << MLPPendingEvents.size());

    auto visitor = [this](auto& v) {
        Handle(v);
    };

    while (!MLPPendingEvents.empty()) {
        auto& ev = MLPPendingEvents.front();
        std::visit(visitor, ev);
        PendingEvents.pop_front();
    }

    PendingEvents = {};
}

void TPartition::InitializeMLPConsumers() {
    std::unordered_map<TString, NKikimrPQ::TPQTabletConfig::TConsumer> consumers;
    for (auto& consumer : Config.GetConsumers()) {
        if (consumer.GetType() == NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
            consumers.emplace(consumer.GetName(), consumer);
        }
    }

    for (auto it = MLPConsumers.begin(); it != MLPConsumers.end();) {
        auto &[name, consumerInfo] = *it;
        if (consumers.contains(name)) {
            ++it;
            continue;
        }

        Send(consumerInfo.ActorId, new TEvents::TEvPoison()); // TODO delete blobs
        it = MLPConsumers.erase(it);
    }

    for (auto& [name, consumer] : consumers) {
        if (MLPConsumers.contains(name)) {
            continue;
        }

        auto actorId = RegisterWithSameMailbox(NMLP::CreateConsumerActor(
            TabletId,
            TabletActorId,
            SelfId()
        ));
        MLPConsumers.emplace(consumer.GetName(), actorId);
    }
}

} // namespace NKikimr::NPQ
