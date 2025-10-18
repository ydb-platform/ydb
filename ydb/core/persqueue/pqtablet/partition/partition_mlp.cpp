#include "partition.h"

#include <ydb/core/persqueue/pqtablet/partition/mlp/mlp.h>

namespace NKikimr::NPQ {

void TPartition::HandleOnInit(TEvPersQueue::TEvMLPReadRequest::TPtr& ev) {
    LOG_D("HandleOnInit TEvPersQueue::TEvMLPReadRequest " << ev->Get()->Record.ShortDebugString());
    MLPPendingEvents.emplace_back(ev);
}

void TPartition::HandleOnInit(TEvPersQueue::TEvMLPCommitRequest::TPtr& ev) {
    LOG_D("HandleOnInit TEvPersQueue::TEvMLPCommitRequest " << ev->Get()->Record.ShortDebugString());
    MLPPendingEvents.emplace_back(ev);
}

void TPartition::HandleOnInit(TEvPersQueue::TEvMLPUnlockRequest::TPtr& ev) {
    LOG_D("HandleOnInit TEvPersQueue::TEvMLPUnlockRequest " << ev->Get()->Record.ShortDebugString());
    MLPPendingEvents.emplace_back(ev);
}

void TPartition::HandleOnInit(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest::TPtr& ev) {
    LOG_D("HandleOnInit TEvPersQueue::TEvMLPChangeMessageDeadlineRequest " << ev->Get()->Record.ShortDebugString());
    MLPPendingEvents.emplace_back(ev);
}

template<typename TEventHandle>
void TPartition::ForwardToMLPConsumer(const TString& consumer, TAutoPtr<TEventHandle>& ev) {
    auto it = MLPConsumers.find(consumer);
    if (it == MLPConsumers.end()) {
        Send(ev->Sender, new TEvPersQueue::TEvMLPErrorResponse(NPersQueue::NErrorCode::EErrorCode::SCHEMA_ERROR, "Consumer not found"), 0, ev->Cookie);
        return;
    }

    auto& consumerInfo = it->second;
    Forward(ev, consumerInfo.ActorId);
}

void TPartition::Handle(TEvPersQueue::TEvMLPReadRequest::TPtr& ev) {
    LOG_D("Handle TEvPersQueue::TEvMLPReadRequest " << ev->Get()->Record.ShortDebugString());
    ForwardToMLPConsumer(ev->Get()->GetConsumer(), ev);
}

void TPartition::Handle(TEvPersQueue::TEvMLPCommitRequest::TPtr& ev) {
    LOG_D("Handle TEvPersQueue::TEvMLPCommitRequest " << ev->Get()->Record.ShortDebugString());
    ForwardToMLPConsumer(ev->Get()->GetConsumer(), ev);
}

void TPartition::Handle(TEvPersQueue::TEvMLPUnlockRequest::TPtr& ev) {
    LOG_D("Handle TEvPersQueue::TEvMLPUnlockRequest " << ev->Get()->Record.ShortDebugString());
    ForwardToMLPConsumer(ev->Get()->GetConsumer(), ev);
}

void TPartition::Handle(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest::TPtr& ev) {
    LOG_D("Handle TEvPersQueue::TEvMLPChangeMessageDeadlineRequest " << ev->Get()->Record.ShortDebugString());
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

void TPartition::Handle(TEvPQ::TEvMLPRestartActor::TPtr& ev) {
    // TODO Add backoff of restart
    for (auto it = MLPConsumers.begin(); it != MLPConsumers.end(); ++it) {
        auto& [name, consumerInfo] = *it;
        if (consumerInfo.ActorId == ev->Sender) {
            LOG_W("Restarting MLP consumer '" << name << "'");

            MLPConsumers.erase(it);
            InitializeMLPConsumers();
            return;
        }
    }
}

} // namespace NKikimr::NPQ
