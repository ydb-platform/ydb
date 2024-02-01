#include "schemeshard_impl.h"

#include <ydb/core/tx/tx_proxy/proxy.h>

namespace NKikimr::NSchemeShard {

NOperationQueue::EStartStatus TSchemeShard::StartBackgroundCleaning(const TPathId& pathId) {
    auto info = ResolveTempTableInfo(pathId);
    if (!info) {
        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    auto& tempTablesByOwner = TempTablesState.TempTablesByOwner;

    auto it = tempTablesByOwner.find(info->OwnerActorId);
    if (it == tempTablesByOwner.end()) {
        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    auto tempTableIt = it->second.find(pathId);
    if (tempTableIt == it->second.end()) {
        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    auto ctx = ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "RunBackgroundCleaning "
        "for temp table# " << JoinPath({info->WorkingDir, info->Name})
        << ", ownerId# " << info->OwnerActorId
        << ", next wakeup# " << BackgroundCleaningQueue->GetWakeupDelta()
        << ", rate# " << BackgroundCleaningQueue->GetRate()
        << ", in queue# " << BackgroundCleaningQueue->Size() << " cleaning events"
        << ", running# " << BackgroundCleaningQueue->RunningSize() << " cleaning events"
        << " at schemeshard " << TabletID());

    auto txId = GetCachedTxId(ctx);

    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), TabletID());
    auto& record = propose->Record;

    auto& modifyScheme = *record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropTable);
    modifyScheme.SetWorkingDir(info->WorkingDir);
    modifyScheme.SetInternal(true);

    auto& drop = *modifyScheme.MutableDrop();
    drop.SetName(info->Name);

    BackgroundCleaningTxs[txId] = pathId;

    Send(SelfId(), std::move(propose));

    return NOperationQueue::EStartStatus::EOperationRunning;
}

void TSchemeShard::HandleBackgroundCleaningCompletionResult(const TTxId& txId) {
    const auto& pathId = BackgroundCleaningTxs.at(txId);

    auto ctx = ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Get BackgroundCleaning CompletionResult "
        "for txId# " << txId
        << ", next wakeup# " << BackgroundCleaningQueue->GetWakeupDelta()
        << ", in queue# " << BackgroundCleaningQueue->GetRate() << " cleaning events"
        << ", running# " << BackgroundCleaningQueue->RunningSize() << " cleaning events"
        << " at schemeshard " << TabletID());

    BackgroundCleaningQueue->OnDone(pathId);
}

void TSchemeShard::OnBackgroundCleaningTimeout(const TPathId& pathId) {
    auto info = ResolveTempTableInfo(pathId);

    auto ctx = ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "BackgroundCleaning timeout "
        "for temp table# " << JoinPath({info->WorkingDir, info->Name})
        << ", ownerId# " << info->OwnerActorId
        << ", next wakeup# " << BackgroundCleaningQueue->GetWakeupDelta()
        << ", in queue# " << BackgroundCleaningQueue->GetRate() << " cleaning events"
        << ", running# " << BackgroundCleaningQueue->RunningSize() << " cleaning events"
        << " at schemeshard " << TabletID());
}

void TSchemeShard::Handle(TEvPrivate::TEvRetryNodeSubscribe::TPtr& ev, const TActorContext&) {
    auto& nodeStates = TempTablesState.NodeStates;
    auto nodeId = ev->Get()->NodeId;

    auto it = nodeStates.find(nodeId);
    if (it == nodeStates.end()) {
        return;
    }

    auto& nodeState = it->second;
    auto& retryState = nodeState.RetryState;
    retryState.IsScheduled = false;
    RetryNodeSubscribe(nodeId);
}

void TSchemeShard::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev, const TActorContext&) {
    RetryNodeSubscribe(ev->Get()->NodeId);
}

const TDuration& GetCurrentDelay(
        const NKikimrConfig::TBackgroundCleaningConfig::TRetrySettings& backgroundCleaningRetrySettings,
        TTempTablesState::TRetryState& state) {
    if (state.CurrentDelay == TDuration::Zero()) {
        state.CurrentDelay =
            TDuration::MilliSeconds(backgroundCleaningRetrySettings.GetStartDelayMs());
    }
    return state.CurrentDelay;
}

TDuration GetDelay(
    const NKikimrConfig::TBackgroundCleaningConfig::TRetrySettings& backgroundCleaningRetrySettings,
    TTempTablesState::TRetryState& state
) {
    auto newDelay = state.CurrentDelay;
    newDelay *= 2;
    auto maxDelay =
        TDuration::MilliSeconds(backgroundCleaningRetrySettings.GetMaxDelayMs());
    if (newDelay > maxDelay) {
        newDelay = maxDelay;
    }
    newDelay *= AppData()->RandomProvider->Uniform(50, 200);
    newDelay /= 100;
    state.CurrentDelay = newDelay;
    return state.CurrentDelay;
}

void TSchemeShard::RetryNodeSubscribe(ui32 nodeId) {
    auto& nodeStates = TempTablesState.NodeStates;
    auto it = nodeStates.find(nodeId);
    if (it == nodeStates.end()) {
        return;
    }

    auto& nodeState = it->second;
    auto& retryState = nodeState.RetryState;

    if (retryState.IsScheduled) {
        return;
    }

    retryState.RetryNumber++;

    if (retryState.RetryNumber > BackgroundCleaningRetrySettings.GetMaxRetryNumber()) {
        for (const auto& ownerActorId: nodeState.Owners) {
            auto& tempTablesByOwner = TempTablesState.TempTablesByOwner;

            auto itTempTables = tempTablesByOwner.find(ownerActorId);
            if (itTempTables == tempTablesByOwner.end()) {
                continue;
            }

            auto& currentTempTables = itTempTables->second;
            for (auto& pathId: currentTempTables) {
                EnqueueBackgroundCleaning(pathId);
            }
            tempTablesByOwner.erase(itTempTables);
        }
        nodeStates.erase(it);

        Send(new IEventHandle(TActivationContext::InterconnectProxy(nodeId), SelfId(),
            new TEvents::TEvUnsubscribe, 0));
        return;
    }

    auto now = TlsActivationContext->Monotonic();
    if (now - retryState.LastRetryAt < GetCurrentDelay(BackgroundCleaningRetrySettings, retryState)) {
        auto at = retryState.LastRetryAt + GetDelay(BackgroundCleaningRetrySettings, retryState);
        retryState.IsScheduled = true;
        Schedule(at - now, new TEvPrivate::TEvRetryNodeSubscribe(nodeId));
        return;
    }

    for (const auto& ownerActorId: nodeState.Owners) {
        Send(new IEventHandle(ownerActorId, SelfId(),
            new TEvSchemeShard::TEvOwnerActorAck(),
            IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession));
    }
    retryState.LastRetryAt = now;
    return;
}

bool TSchemeShard::CheckOwnerUndelivered(TEvents::TEvUndelivered::TPtr& ev) {
    auto& tempTablesByOwner = TempTablesState.TempTablesByOwner;

    auto ownerActorId = ev->Sender;
    auto it = tempTablesByOwner.find(ownerActorId);
    if (it == tempTablesByOwner.end()) {
        return false;
    }

    auto& currentTempTables = it->second;

    for (auto& pathId: currentTempTables) {
        EnqueueBackgroundCleaning(pathId);
    }
    tempTablesByOwner.erase(it);

    auto& nodeStates = TempTablesState.NodeStates;
    auto itNodeStates = nodeStates.find(ownerActorId.NodeId());
    if (itNodeStates == nodeStates.end()) {
        return true;
    }
    auto itOwner = itNodeStates->second.Owners.find(ownerActorId);
    if (itOwner == itNodeStates->second.Owners.end()) {
        return true;
    }
    itNodeStates->second.Owners.erase(itOwner);
    if (itNodeStates->second.Owners.empty()) {
        nodeStates.erase(itNodeStates);
        Send(new IEventHandle(TActivationContext::InterconnectProxy(ownerActorId.NodeId()), SelfId(),
            new TEvents::TEvUnsubscribe, 0));
    }

    return true;
}

void TSchemeShard::EnqueueBackgroundCleaning(const TPathId& pathId) {
    if (BackgroundCleaningQueue) {
        BackgroundCleaningQueue->Enqueue(std::move(pathId));
    }
}

void TSchemeShard::RemoveBackgroundCleaning(const TPathId& pathId) {
    if (BackgroundCleaningQueue) {
        BackgroundCleaningQueue->Remove(std::move(pathId));
    }
}

void TSchemeShard::HandleBackgroundCleaningTransactionResult(
        TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& result) {
    const auto txId = TTxId(result->Get()->Record.GetTxId());

    auto ctx = ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Get BackgroundCleaning TransactionResult "
        "for txId# " << txId
        << ", next wakeup# " << BackgroundCleaningQueue->GetWakeupDelta()
        << ", in queue# " << BackgroundCleaningQueue->GetRate() << " cleaning events"
        << ", running# " << BackgroundCleaningQueue->RunningSize() << " cleaning events"
        << " at schemeshard " << TabletID());

    const auto& pathId = BackgroundCleaningTxs.at(txId);

    const NKikimrScheme::TEvModifySchemeTransactionResult &record = result->Get()->Record;

    switch (record.GetStatus()) {
    case NKikimrScheme::EStatus::StatusPathDoesNotExist:
    case NKikimrScheme::EStatus::StatusSuccess: {
        BackgroundCleaningQueue->OnDone(pathId);
        break;
    }
    case NKikimrScheme::EStatus::StatusAccepted:
        Send(SelfId(), new TEvSchemeShard::TEvNotifyTxCompletion(record.GetTxId()));
        break;
    default: {
        BackgroundCleaningQueue->OnDone(pathId);
        EnqueueBackgroundCleaning(pathId);
        break;
    }
    }
}

void TSchemeShard::ClearTempTablesState() {
    auto ctx = ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "Clear TempTablesState with owners number: "
        << TempTablesState.TempTablesByOwner.size());

    if (BackgroundCleaningQueue) {
        auto& nodeStates = TempTablesState.NodeStates;
        for (const auto& [nodeId, nodeState] : nodeStates) {
            Send(new IEventHandle(TActivationContext::InterconnectProxy(nodeId), SelfId(),
                new TEvents::TEvUnsubscribe, 0));
        }
        BackgroundCleaningQueue->Clear();
    }
    TempTablesState.TempTablesByOwner.clear();
    TempTablesState.NodeStates.clear();
}

} // NKikimr::NSchemeShard
