#include "mlp_consumer.h"

#include <ydb/core/persqueue/common/key.h>

namespace NKikimr::NPQ::NMLP {

TString MakeSnapshotKey(ui32 partitionId, ui32 consumerId) {
    return TStringBuilder() << TKeyPrefix(TKeyPrefix::EType::TypeConsumerData, TPartitionId(partitionId))
        << "_" << Sprintf("%.10" PRIu32, consumerId);
}

TConsumerActor::TConsumerActor(ui64 tabletId, const TActorId& tabletActorId, ui32 partitionId, const TActorId& partitionActorId, const NKikimrPQ::TPQTabletConfig_TConsumer& config)
    : TBaseActor(tabletId, tabletActorId, NKikimrServices::EServiceKikimr::PQ_MLP_CONSUMER)
    , PartitionId(partitionId)
    , PartitionActorId(partitionActorId)
    , Config(config) {
}

void TConsumerActor::Bootstrap() {
    Become(&TConsumerActor::StateInit);

    auto request = std::make_unique<TEvKeyValue::TEvRequest>();
    request->Record.AddCmdRead()->SetKey(MakeSnapshotKey(PartitionId, Config.GetId()));

    Send(TabletActorId, std::move(request));
}

void TConsumerActor::PassAway() {
    Batch.Rollback();

    // TODO reply error for all mesages from queues

    TBase::PassAway();
}

TString TConsumerActor::BuildLogPrefix() const {
    return TStringBuilder() << "[" << PartitionId << "][MLP][" << Config.GetName() << "]";
}

void TConsumerActor::Queue(TEvPersQueue::TEvMLPReadRequest::TPtr& ev) {
    ReadRequestsQueue.push_back(std::move(ev));
}

void TConsumerActor::Queue(TEvPersQueue::TEvMLPCommitRequest::TPtr& ev) {
    CommitRequestsQueue.push_back(std::move(ev));
}

void TConsumerActor::Queue(TEvPersQueue::TEvMLPReleaseRequest::TPtr& ev) {
    ReleaseRequestsQueue.push_back(std::move(ev));
}

void TConsumerActor::Queue(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest::TPtr& ev) {
    ChangeMessageDeadlineRequestsQueue.push_back(std::move(ev));
}

void TConsumerActor::Handle(TEvPersQueue::TEvMLPReadRequest::TPtr& ev) {
    Queue(ev);
    ProcessEventQueue();
}

void TConsumerActor::Handle(TEvPersQueue::TEvMLPCommitRequest::TPtr& ev) {
    Queue(ev);
    ProcessEventQueue();
}

void TConsumerActor::Handle(TEvPersQueue::TEvMLPReleaseRequest::TPtr& ev) {
    Queue(ev);
    ProcessEventQueue();
}

void TConsumerActor::Handle(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest::TPtr& ev) {
    Queue(ev);
    ProcessEventQueue();
}

void TConsumerActor::HandleOnInit(TEvKeyValue::TEvResponse::TPtr& ev) {
    auto& record = ev->Get()->Record;

    if (record.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
        return Restart(TStringBuilder() << "Received KV error on initialization: " << record.GetStatus());
    }
    if (record.ReadResultSize() != 1) {
        return Restart(TStringBuilder() << "Unexpected KV response on initialization: " << record.ReadResultSize());
    }

    auto& readResult = record.GetReadResult(0);
    if (readResult.GetStatus() != NKikimrProto::OK) {
        return Restart(TStringBuilder() << "Received KV response error on initialization: " << readResult.GetStatus());
    }
    AFL_ENSURE(readResult.HasValue() && readResult.GetValue().size());

    NKikimrPQ::TMLPStorageSnapshot snapshot;
    if (!snapshot.ParseFromString(readResult.GetValue())) {
        return Restart(TStringBuilder() << "Parse snapshot error");
    }

    if (Config.GetId() != snapshot.GetConfiguration().GetConsumerId()) {
        return Restart(TStringBuilder() << "Snapshot consumer id mismatch: " << Config.GetId() << " vs " << snapshot.GetConfiguration().GetConsumerId());
    }

    if (Config.GetGeneration() != snapshot.GetConfiguration().GetGeneration()) {
        LOG_W("Received snapshot from old consumer generation: " << Config.GetGeneration() << " vs " << snapshot.GetConfiguration().GetGeneration());
    } else {
        Storage->InitializeFromSnapshot(snapshot);
        Storage->ProccessDeadlines();
    }

    LOG_D("Initialized");
    Become(&TConsumerActor::StateWork);

    ProcessEventQueue();
}

STFUNC(TConsumerActor::StateInit) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPersQueue::TEvMLPReadRequest, Queue);
        hFunc(TEvPersQueue::TEvMLPCommitRequest, Queue);
        hFunc(TEvPersQueue::TEvMLPReleaseRequest, Queue);
        hFunc(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest, Queue);
        hFunc(TEvKeyValue::TEvResponse, HandleOnInit);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

STFUNC(TConsumerActor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPersQueue::TEvMLPReadRequest, Handle);
        hFunc(TEvPersQueue::TEvMLPCommitRequest, Handle);
        hFunc(TEvPersQueue::TEvMLPReleaseRequest, Handle);
        hFunc(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

STFUNC(TConsumerActor::StateWrite) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPersQueue::TEvMLPReadRequest, Queue);
        hFunc(TEvPersQueue::TEvMLPCommitRequest, Queue);
        hFunc(TEvPersQueue::TEvMLPReleaseRequest, Queue);
        hFunc(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest, Queue);
        hFunc(TEvKeyValue::TEvResponse, HandleOnWrite);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

void TConsumerActor::Restart(TString&& error) {
    LOG_E(error);

    // TODO Send restart command to partition

    PassAway();
}

void TConsumerActor::ProcessEventQueue() {
    Batch.Clear();

    for (auto& ev : CommitRequestsQueue) {
        for (auto offset : ev->Get()->Record.GetOffset()) {
            Storage->Commit({
                .Offset = offset
            });
        }

        Batch.Add(ev);
    }
    CommitRequestsQueue.clear();

    for (auto& ev : ReleaseRequestsQueue) {
        for (auto offset : ev->Get()->Record.GetOffset()) {
            Storage->Unlock({
                .Offset = offset
            });
        }

        Batch.Add(ev);
    }
    ReleaseRequestsQueue.clear();

    for (auto& ev : ChangeMessageDeadlineRequestsQueue) {
        auto deadlineTimestamp = ev->Get()->GetDeadlineTimestamp();
        for (auto offset : ev->Get()->Record.GetOffset()) {
            Storage->ChangeMessageDeadline({
                .Offset = offset
            }, deadlineTimestamp);
        }

        Batch.Add(ev);
    }
    ChangeMessageDeadlineRequestsQueue.clear();

    ui64 fromOffset = 0;
    while (!ReadRequestsQueue.empty()) {
        auto& ev = ReadRequestsQueue.front();

        size_t count = ev->Get()->GetMaxNumberOfMessages();
        const auto deadline = ev->Get()->GetVisibilityTimeout().ToDeadLine();

        std::vector<TMessageId> messages;
        messages.reserve(count);
        for (; count; --count) {
            auto result = Storage->Next(deadline, fromOffset);
            if (!result) {
                break;
            }

            messages.push_back(result->Message);
            fromOffset = result->FromOffset;
        }

        if (messages.empty() && ev->Get()->GetWaitTime() != TDuration::Zero()) {
            break;
        }

        Batch.Add(ev, std::move(messages));
        ReadRequestsQueue.pop_front();
    }

    if (Batch.Empty()) {
        return;
    }

    PersistSnapshot();
}

void TConsumerActor::PersistSnapshot() {
    Become(&TConsumerActor::StateWrite);

    Storage->Compact();

    NKikimrPQ::TMLPStorageSnapshot snapshot;

    auto* config = snapshot.MutableConfiguration();
    config->SetConsumerId(Config.GetId());
    config->SetGeneration(Config.GetGeneration());

    Storage->CreateSnapshot(snapshot);
}


NActors::IActor* CreateConsumerActor(
    ui64 tabletId,
    const NActors::TActorId& tabletActorId,
    ui32 partitionId,
    const NActors::TActorId& partitionActorId,
    const NKikimrPQ::TPQTabletConfig_TConsumer& config) {
    return new TConsumerActor(tabletId, tabletActorId, partitionId, partitionActorId, config);
}

}
