#include "mlp_batch.h"
#include "mlp_consumer.h"
#include "mlp_storage.h"

#include <ydb/core/persqueue/common/key.h>
#include <ydb/core/persqueue/events/internal.h>

namespace NKikimr::NPQ::NMLP {

TString MakeSnapshotKey(ui32 partitionId, ui32 consumerId) {
    return TStringBuilder() << TKeyPrefix(TKeyPrefix::EType::TypeConsumerData, TPartitionId(partitionId))
        << "_" << Sprintf("%.10" PRIu32, consumerId);
}

TConsumerActor::TConsumerActor(ui64 tabletId, const TActorId& tabletActorId, ui32 partitionId, const TActorId& partitionActorId, const NKikimrPQ::TPQTabletConfig_TConsumer& config)
    : TBaseActor(tabletId, tabletActorId, NKikimrServices::EServiceKikimr::PQ_MLP_CONSUMER)
    , PartitionId(partitionId)
    , PartitionActorId(partitionActorId)
    , Config(config)
    , Storage(std::make_unique<TStorage>()) {
}

void TConsumerActor::Bootstrap() {
    Become(&TConsumerActor::StateInit);

    auto request = std::make_unique<TEvKeyValue::TEvRequest>();
    request->Record.AddCmdRead()->SetKey(MakeSnapshotKey(PartitionId, Config.GetId()));

    Send(TabletActorId, std::move(request));
}

void TConsumerActor::PassAway() {
    if (Batch) {
        Batch->Rollback();
    }

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

void TConsumerActor::Queue(TEvPersQueue::TEvMLPUnlockRequest::TPtr& ev) {
    UnlockRequestsQueue.push_back(std::move(ev));
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

void TConsumerActor::Handle(TEvPersQueue::TEvMLPUnlockRequest::TPtr& ev) {
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
        hFunc(TEvPersQueue::TEvMLPUnlockRequest, Queue);
        hFunc(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest, Queue);
        hFunc(TEvKeyValue::TEvResponse, HandleOnInit);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

void TConsumerActor::HandleOnWrite(TEvKeyValue::TEvResponse::TPtr& ev) {
    auto& record = ev->Get()->Record;

    if (record.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
        return Restart(TStringBuilder() << "Received KV error on write: " << record.GetStatus());
    }
    if (record.ReadResultSize() != 1) {
        return Restart(TStringBuilder() << "Unexpected KV response on write: " << record.ReadResultSize());
    }

    auto& readResult = record.GetReadResult(0);
    if (readResult.GetStatus() != NKikimrProto::OK) {
        return Restart(TStringBuilder() << "Received KV response error on write: " << readResult.GetStatus());
    }
    AFL_ENSURE(readResult.HasValue() && readResult.GetValue().size());

    LOG_D("Snapshot persisted");
    Become(&TConsumerActor::StateWork);

    ProcessEventQueue();
}

STFUNC(TConsumerActor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPersQueue::TEvMLPReadRequest, Handle);
        hFunc(TEvPersQueue::TEvMLPCommitRequest, Handle);
        hFunc(TEvPersQueue::TEvMLPUnlockRequest, Handle);
        hFunc(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

STFUNC(TConsumerActor::StateWrite) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPersQueue::TEvMLPReadRequest, Queue);
        hFunc(TEvPersQueue::TEvMLPCommitRequest, Queue);
        hFunc(TEvPersQueue::TEvMLPUnlockRequest, Queue);
        hFunc(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest, Queue);
        hFunc(TEvKeyValue::TEvResponse, HandleOnWrite);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

namespace {

template<typename T>
void ReplyErrorAll(const TActorIdentity selfActorId, std::deque<T>& queue) {
    for (auto& ev : queue) {
        selfActorId.Send(ev->Sender, new TEvPersQueue::TEvMLPErrorResponse(NPersQueue::NErrorCode::EErrorCode::ERROR, "Actor destroyed"), 0, ev->Cookie);
    }
    queue.clear();
}

}

void TConsumerActor::Restart(TString&& error) {
    LOG_E(error);

    Send(PartitionActorId, new TEvPQ::TEvMLPRestartActor());

    ReplyErrorAll(SelfId(), ReadRequestsQueue);
    ReplyErrorAll(SelfId(), CommitRequestsQueue);
    ReplyErrorAll(SelfId(), UnlockRequestsQueue);
    ReplyErrorAll(SelfId(), ChangeMessageDeadlineRequestsQueue);

    PassAway();
}

void TConsumerActor::ProcessEventQueue() {
    AFL_ENSURE(!Batch);
    Batch = std::make_unique<TBatch>(SelfId(), PartitionActorId);

    for (auto& ev : CommitRequestsQueue) {
        for (auto offset : ev->Get()->Record.GetOffset()) {
            Storage->Commit(offset);
        }

        Batch->Add(ev);
    }
    CommitRequestsQueue.clear();

    for (auto& ev : UnlockRequestsQueue) {
        for (auto offset : ev->Get()->Record.GetOffset()) {
            Storage->Unlock(offset);
        }

        Batch->Add(ev);
    }
    UnlockRequestsQueue.clear();

    for (auto& ev : ChangeMessageDeadlineRequestsQueue) {
        auto deadlineTimestamp = ev->Get()->GetDeadlineTimestamp();
        for (auto offset : ev->Get()->Record.GetOffset()) {
            Storage->ChangeMessageDeadline(offset, deadlineTimestamp);
        }

        Batch->Add(ev);
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

        Batch->Add(ev, std::move(messages));
        ReadRequestsQueue.pop_front();
    }

    if (Batch->Empty()) {
        Batch.reset();
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

    auto request = std::make_unique<TEvKeyValue::TEvRequest>();
    request->Record.AddCmdWrite()->SetKey(MakeSnapshotKey(PartitionId, Config.GetId()));
    request->Record.AddCmdWrite()->SetValue(snapshot.SerializeAsString());

    Send(TabletActorId, std::move(request));
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
