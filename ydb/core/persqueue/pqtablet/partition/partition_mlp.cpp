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

void TPartition::HandleOnInit(TEvPQ::TEvMLPPurgeRequest::TPtr& ev) {
    LOG_D("HandleOnInit TEvPQ::TEvMLPPurgeRequest " << ev->Get()->Record.ShortDebugString());
    MLPPendingEvents.emplace_back(ev);
}

void TPartition::HandleOnInit(TEvPQ::TEvGetMLPConsumerStateRequest::TPtr& ev) {
    LOG_D("HandleOnInit TEvPQ::TEvGetMLPConsumerStateRequest " << ev->Get()->Consumer << ":" << ev->Get()->PartitionId);
    MLPPendingEvents.emplace_back(ev);
}

template<typename TEventHandle>
void TPartition::ForwardToMLPConsumer(const TString& consumer, TAutoPtr<TEventHandle>& ev) {
    auto it = MLPConsumers.find(consumer);
    if (it == MLPConsumers.end()) {
        Send(ev->Sender, new TEvPQ::TEvMLPErrorResponse(Partition.OriginalPartitionId, Ydb::StatusIds::SCHEME_ERROR,
            TStringBuilder() << "Consumer '" << consumer << "' does not exist"), 0, ev->Cookie);
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

void TPartition::Handle(TEvPQ::TEvMLPPurgeRequest::TPtr& ev) {
    LOG_D("Handle TEvPQ::TEvMLPPurgeRequest " << ev->Get()->Record.ShortDebugString());
    ForwardToMLPConsumer(ev->Get()->GetConsumer(), ev);
}

void TPartition::Handle(TEvPQ::TEvGetMLPConsumerStateRequest::TPtr& ev) {
    LOG_D("Handle TEvPQ::TEvGetMLPConsumerStateRequest " << ev->Get()->Consumer << ":" << ev->Get()->PartitionId);
    ForwardToMLPConsumer(ev->Get()->Consumer, ev);
}

void TPartition::Handle(TEvPQ::TEvMLPConsumerState::TPtr& ev) {
    auto& metrics = ev->Get()->Metrics;

    LOG_D("Handle TEvPQ::TEvMLPConsumerState " << metrics.ShortDebugString());
    auto it = MLPConsumers.find(metrics.GetConsumer());
    if (it == MLPConsumers.end()) {
        return;
    }

    TabletCounters.Cumulative()[COUNTER_PQ_TABLET_CPU_USAGE] += metrics.GetCPUUsage();

    auto& consumerInfo = it->second;
    consumerInfo.Metrics = std::move(metrics);
    consumerInfo.UseForReading = ev->Get()->UseForReading;
}

void TPartition::Handle(TEvPQ::TEvMLPConsumerStatus::TPtr& ev) {
    auto& record = ev->Get()->Record;
    LOG_D("Handle TEvPQ::TEvMLPConsumerStatus " << record.ShortDebugString());

    auto it = MLPConsumers.find(record.GetConsumer());
    if (it == MLPConsumers.end()) {
        return;
    }

    auto& consumerInfo = it->second;
    consumerInfo.UseForReading = record.GetUseForReading();

    record.SetGeneration(TabletGeneration);
    record.SetCookie(++PQRBCookie);
    Forward(ev, TabletActorId);
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
    auto retentionPeriod = [&](const auto& consumer) -> std::optional<TDuration> {
        if (consumer.GetImportant()) {
            return std::nullopt;
        }
        if (consumer.HasAvailabilityPeriodMs()) {
            return TDuration::MilliSeconds(consumer.GetAvailabilityPeriodMs());
        } else if (Config.GetPartitionConfig().GetStorageLimitBytes() > 0) {
            // retention by storage is not supported yet
            return std::nullopt;
        } else {
            return TDuration::Seconds(Config.GetPartitionConfig().GetLifetimeSeconds());
        }
    };

    std::unordered_map<TString, NKikimrPQ::TPQTabletConfig::TConsumer> consumers;
    for (auto& consumer : Config.GetConsumers()) {
        if (consumer.GetType() == NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
            consumers.emplace(consumer.GetName(), consumer);
        }
    }

    LOG_D("Initializing MLP Consumers: " << consumers.size());

    for (auto it = MLPConsumers.begin(); it != MLPConsumers.end();) {
        auto &[name, consumerInfo] = *it;
        if (auto cit = consumers.find(name); cit != consumers.end()) {
            LOG_I("Updating MLP consumer '" << name << "' config");
            auto& config = cit->second;
            Send(consumerInfo.ActorId, new TEvPQ::TEvMLPConsumerUpdateConfig(Config, config,
                retentionPeriod(config), GetPerPartitionCounterSubgroup()));

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
            DbPath,
            TabletId,
            TabletActorId,
            Partition.OriginalPartitionId,
            SelfId(),
            Config,
            consumer,
            retentionPeriod(consumer),
            GetEndOffset(),
            GetPerPartitionCounterSubgroup()
        ));
        MLPConsumers.emplace(consumer.GetName(), actorId);
    }
}

void TPartition::DropDataOfMLPConsumer(NKikimrClient::TKeyValueRequest& request, const TString& consumer) {
    auto snapshotKey = NMLP::MakeSnapshotKey(Partition.OriginalPartitionId, consumer);
    auto snapshot = request.AddCmdDeleteRange()->MutableRange();
    snapshot->SetFrom(snapshotKey);
    snapshot->SetIncludeFrom(true);
    snapshot->SetTo(std::move(snapshotKey));
    snapshot->SetIncludeTo(true);

    auto wal = request.AddCmdDeleteRange()->MutableRange();
    wal->SetFrom(NMLP::MinWALKey(Partition.OriginalPartitionId, consumer));
    wal->SetIncludeFrom(true);
    wal->SetTo(NMLP::MaxWALKey(Partition.OriginalPartitionId, consumer));
    wal->SetIncludeTo(true);
}

void TPartition::NotifyEndOffsetChanged() {
    if (LastNotifiedEndOffset == GetEndOffset()) {
        return;
    }
    LastNotifiedEndOffset = GetEndOffset();
    for (auto [_, info] : MLPConsumers) {
        Send(info.ActorId, new TEvPQ::TEvEndOffsetChanged(GetEndOffset()));
    }
}

} // namespace NKikimr::NPQ
