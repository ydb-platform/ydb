#include "partition.h"

#include <ydb/core/persqueue/pqtablet/partition/mlp/mlp.h>

#define YDB_LOG_THIS_FILE_COMPONENT Service

namespace NKikimr::NPQ {

void TPartition::HandleOnInit(TEvPQ::TEvMLPReadRequest::TPtr& ev) {
    YDB_LOG_DEBUG("HandleOnInit TEvPQ::TEvMLPReadRequest",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record});
    MLPPendingEvents.emplace_back(ev);
}

void TPartition::HandleOnInit(TEvPQ::TEvMLPCommitRequest::TPtr& ev) {
    YDB_LOG_DEBUG("HandleOnInit TEvPQ::TEvMLPCommitRequest",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record});
    MLPPendingEvents.emplace_back(ev);
}

void TPartition::HandleOnInit(TEvPQ::TEvMLPUnlockRequest::TPtr& ev) {
    YDB_LOG_DEBUG("HandleOnInit TEvPQ::TEvMLPUnlockRequest",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record});
    MLPPendingEvents.emplace_back(ev);
}

void TPartition::HandleOnInit(TEvPQ::TEvMLPChangeMessageDeadlineRequest::TPtr& ev) {
    YDB_LOG_DEBUG("HandleOnInit TEvPQ::TEvMLPChangeMessageDeadlineRequest",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record});
    MLPPendingEvents.emplace_back(ev);
}

void TPartition::HandleOnInit(TEvPQ::TEvMLPPurgeRequest::TPtr& ev) {
    YDB_LOG_DEBUG("HandleOnInit TEvPQ::TEvMLPPurgeRequest",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record});
    MLPPendingEvents.emplace_back(ev);
}

void TPartition::HandleOnInit(TEvPQ::TEvGetMLPConsumerStateRequest::TPtr& ev) {
    YDB_LOG_DEBUG("HandleOnInit TEvPQ::TEvGetMLPConsumerStateRequest",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"Consumer", ev->Get()->Consumer},
        {"PartitionId", ev->Get()->PartitionId});
    MLPPendingEvents.emplace_back(ev);
}

void TPartition::HandleOnInit(TEvPQ::TEvMLPUpdateExternalLockedMessageGroupsId::TPtr& ev)  {
    YDB_LOG_DEBUG("HandleOnInit TEvPQ::TEvMLPUpdateExternalLockedMessageGroupsId",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"Record.GetConsumer", ev->Get()->Record.GetConsumer()},
        {"GetPartitionId", ev->Get()->GetPartitionId()});
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
    YDB_LOG_DEBUG("Handle TEvPQ::TEvMLPReadRequest",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record});
    ForwardToMLPConsumer(ev->Get()->GetConsumer(), ev);
}

void TPartition::Handle(TEvPQ::TEvMLPCommitRequest::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvPQ::TEvMLPCommitRequest",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record});
    ForwardToMLPConsumer(ev->Get()->GetConsumer(), ev);
}

void TPartition::Handle(TEvPQ::TEvMLPUnlockRequest::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvPQ::TEvMLPUnlockRequest",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record});
    ForwardToMLPConsumer(ev->Get()->GetConsumer(), ev);
}

void TPartition::Handle(TEvPQ::TEvMLPChangeMessageDeadlineRequest::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvPQ::TEvMLPChangeMessageDeadlineRequest",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record});
    ForwardToMLPConsumer(ev->Get()->GetConsumer(), ev);
}

void TPartition::Handle(TEvPQ::TEvMLPPurgeRequest::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvPQ::TEvMLPPurgeRequest",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record});
    ForwardToMLPConsumer(ev->Get()->GetConsumer(), ev);
}

void TPartition::Handle(TEvPQ::TEvGetMLPConsumerStateRequest::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvPQ::TEvGetMLPConsumerStateRequest",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"Consumer", ev->Get()->Consumer},
        {"PartitionId", ev->Get()->PartitionId});
    ForwardToMLPConsumer(ev->Get()->Consumer, ev);
}

void TPartition::Handle(TEvPQ::TEvMLPUpdateExternalLockedMessageGroupsId::TPtr& ev)  {
    YDB_LOG_DEBUG("Handle TEvPQ::TEvMLPUpdateExternalLockedMessageGroupsId",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"Record.GetConsumer", ev->Get()->Record.GetConsumer()},
        {"GetPartitionId", ev->Get()->GetPartitionId()});
    ForwardToMLPConsumer(ev->Get()->GetConsumer(), ev);
}

void TPartition::Handle(TEvPQ::TEvMLPConsumerState::TPtr& ev) {
    auto& metrics = ev->Get()->Metrics;

    YDB_LOG_DEBUG("Handle TEvPQ::TEvMLPConsumerState",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"#_metrics", metrics});
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
    YDB_LOG_DEBUG("Handle TEvPQ::TEvMLPConsumerStatus",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", record});

    auto it = MLPConsumers.find(record.GetConsumer());
    if (it == MLPConsumers.end()) {
        return;
    }

    auto& consumerInfo = it->second;
    consumerInfo.UseForReading = record.GetUseForReading();
    consumerInfo.LockedMessageCount = record.GetLockedMessageCount();
    consumerInfo.DelayedMessageCount = record.GetDelayedMessageCount();
    consumerInfo.MessageCount = record.GetMessageCount();

    record.SetGeneration(TabletGeneration);
    record.SetCookie(++PQRBCookie);
    Forward(ev, TabletActorId);
}

void TPartition::ProcessMLPPendingEvents() {
    YDB_LOG_DEBUG("Process MLP pending events. Count",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"#_MLPPendingEvents.size", MLPPendingEvents.size()});

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

    YDB_LOG_DEBUG("Initializing MLP",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"consumers", consumers.size()});

    for (auto it = MLPConsumers.begin(); it != MLPConsumers.end();) {
        auto &[name, consumerInfo] = *it;
        if (auto cit = consumers.find(name); cit != consumers.end()) {
            YDB_LOG_INFO("Updating MLP consumer config",
                {"logPrefix", NPQ_LOG_PREFIX},
                {"name", name});
            auto& config = cit->second;
            Send(consumerInfo.ActorId, new TEvPQ::TEvMLPConsumerUpdateConfig(Config, config,
                retentionPeriod(config), GetPerPartitionCounterSubgroup()));

            ++it;
            continue;
        }

        YDB_LOG_INFO("Destroing MLP consumer",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"name", name});

        Send(consumerInfo.ActorId, new TEvents::TEvPoison()); // TODO MLP delete blobs
        it = MLPConsumers.erase(it);
    }

    for (auto& [name, consumer] : consumers) {
        if (MLPConsumers.contains(name)) {
            continue;
        }

        YDB_LOG_INFO("Creating MLP consumer",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"name", name});
        auto actorId = RegisterWithSameMailbox(NMLP::CreateConsumerActor(
            DbPath,
            TabletId,
            TabletActorId,
            Partition.OriginalPartitionId,
            SelfId(),
            TabletGeneration,
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
