#include "partition.h"

#include <ydb/core/persqueue/pqtablet/partition/mlp/mlp.h>

namespace NKikimr::NPQ {

void TPartition::HandleOnInit(TEvPQ::TEvMLPReadRequest::TPtr& ev) {
    LOG_D("HandleOnInit TEvPQ::TEvMLPReadRequest " << ev->Get()->Record.ShortDebugString());
    MLPPendingEvents.emplace_back(ev);
}

void TPartition::HandleOnInit(TEvPQ::TEvMLPCommitRequest::TPtr& ev) {
    LOG_D("HandleOnInit TEvPQ::TEvMLPCommitRequest " << ev->Get()->Record.ShortDebugString());
    MLPPendingEvents.emplace_back(ev);
}

void TPartition::HandleOnInit(TEvPQ::TEvMLPUnlockRequest::TPtr& ev) {
    LOG_D("HandleOnInit TEvPQ::TEvMLPUnlockRequest " << ev->Get()->Record.ShortDebugString());
    MLPPendingEvents.emplace_back(ev);
}

void TPartition::HandleOnInit(TEvPQ::TEvMLPChangeMessageDeadlineRequest::TPtr& ev) {
    LOG_D("HandleOnInit TEvPQ::TEvMLPChangeMessageDeadlineRequest " << ev->Get()->Record.ShortDebugString());
    MLPPendingEvents.emplace_back(ev);
}

template<typename TEventHandle>
void TPartition::ForwardToMLPConsumer(const TString& consumer, TAutoPtr<TEventHandle>& ev) {
    auto it = MLPConsumers.find(consumer);
    if (it == MLPConsumers.end()) {
        Send(ev->Sender, new TEvPQ::TEvMLPErrorResponse(Ydb::StatusIds::SCHEME_ERROR, "Consumer not found"), 0, ev->Cookie);
        return;
    }

    auto& consumerInfo = it->second;
    Forward(ev, consumerInfo.ActorId);
}

void TPartition::Handle(TEvPQ::TEvMLPReadRequest::TPtr& ev) {
    LOG_D("Handle TEvPQ::TEvMLPReadRequest " << ev->Get()->Record.ShortDebugString());
    ForwardToMLPConsumer(ev->Get()->GetConsumer(), ev);
}

void TPartition::Handle(TEvPQ::TEvMLPCommitRequest::TPtr& ev) {
    LOG_D("Handle TEvPQ::TEvMLPCommitRequest " << ev->Get()->Record.ShortDebugString());
    ForwardToMLPConsumer(ev->Get()->GetConsumer(), ev);
}

void TPartition::Handle(TEvPQ::TEvMLPUnlockRequest::TPtr& ev) {
    LOG_D("Handle TEvPQ::TEvMLPUnlockRequest " << ev->Get()->Record.ShortDebugString());
    ForwardToMLPConsumer(ev->Get()->GetConsumer(), ev);
}

void TPartition::Handle(TEvPQ::TEvMLPChangeMessageDeadlineRequest::TPtr& ev) {
    LOG_D("Handle TEvPQ::TEvMLPChangeMessageDeadlineRequest " << ev->Get()->Record.ShortDebugString());
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
        MLPPendingEvents.pop_front();
    }

    MLPPendingEvents = {};
}

void TPartition::InitializeMLPConsumers() {
    std::unordered_map<TString, NKikimrPQ::TPQTabletConfig::TConsumer> consumers;
    for (auto& consumer : Config.GetConsumers()) {
        if (consumer.GetType() == NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
            consumers.emplace(consumer.GetName(), consumer);
        }
    }

    LOG_D("Initializing MLP Consumers: " << consumers.size());

    for (auto it = MLPConsumers.begin(); it != MLPConsumers.end();) {
        auto &[name, consumerInfo] = *it;
        if (consumers.contains(name)) {
            ++it;
            continue;
        }

        LOG_I("Destroing MLP consumer '" << name << "'");

        Send(consumerInfo.ActorId, new TEvents::TEvPoison()); // TODO MLP delete blobs
        it = MLPConsumers.erase(it);
    }

    for (auto& [name, consumer] : consumers) {
        if (MLPConsumers.contains(name)) {
            continue;
        }

        LOG_I("Creating MLP consumer '" << name << "'");

        auto actorId = RegisterWithSameMailbox(NMLP::CreateConsumerActor(
            TabletId,
            TabletActorId,
            Partition.OriginalPartitionId,
            SelfId(),
            consumer
        ));
        MLPConsumers.emplace(consumer.GetName(), actorId);
    }
}

} // namespace NKikimr::NPQ
