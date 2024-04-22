#include "schemeshard_impl.h"

#include <ydb/core/tx/tx_proxy/proxy.h>

namespace NKikimr::NSchemeShard {

namespace {
    struct TTraverseResult {
        TVector<NKikimr::TPathId> Dirs;
        TVector<NKikimr::TPathId> Tables;
    };

    TTraverseResult Traverse(const TString& workingDir, const TString& name, TSchemeShard* schemeShard) {
        TTraverseResult result;
        const auto resolvedPath = TPath::Resolve(workingDir, schemeShard).Dive(name);
        if (!resolvedPath.IsResolved()) {
            return result;
        }

        TVector<NKikimr::TPathId> toVisit;
        toVisit.push_back(resolvedPath.Base()->PathId);

        while (!toVisit.empty()) {
            const auto pathId = toVisit.back();
            toVisit.pop_back();

            const auto path = TPath::Init(pathId, schemeShard);
            const auto pathElement = path.Base();

            if (pathElement->IsDirectory()) {
                result.Dirs.emplace_back(pathId);
                for (const auto& [_, child] : pathElement->GetChildren()) {
                    toVisit.push_back(child);
                }
            } else if (pathElement->IsTable()) {
                result.Tables.emplace_back(pathId);
            }
        }

        return result;
    }
}

NOperationQueue::EStartStatus TSchemeShard::StartBackgroundCleaning(const TPathId& pathId) {
    auto info = ResolveTempDirInfo(pathId);
    if (!info) {
        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    auto& TempDirsByOwner = TempDirsState.TempDirsByOwner;

    auto it = TempDirsByOwner.find(info->TempDirOwnerActorId);
    if (it == TempDirsByOwner.end()) {
        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    auto tempTableIt = it->second.find(pathId);
    if (tempTableIt == it->second.end()) {
        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    auto ctx = ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "RunBackgroundCleaning "
        "for temp dir# " << JoinPath({info->WorkingDir, info->Name})
        << ", ownerId# " << info->TempDirOwnerActorId
        << ", next wakeup# " << BackgroundCleaningQueue->GetWakeupDelta()
        << ", rate# " << BackgroundCleaningQueue->GetRate()
        << ", in queue# " << BackgroundCleaningQueue->Size() << " cleaning events"
        << ", running# " << BackgroundCleaningQueue->RunningSize() << " cleaning events"
        << " at schemeshard " << TabletID());

    auto traverseResult = Traverse(info->WorkingDir, info->Name, this);

    auto [stateIter, _] = BackgroundCleaningState.emplace(
        pathId,
        TBackgroundCleaningState {
            {},
            std::move(traverseResult.Dirs),
            std::move(traverseResult.Tables)
        });
    auto& state = stateIter->second;

    for (const auto& tablePathId : state.TablesToDrop) {
        const auto txId = GetCachedTxId(ctx);
        BackgroundCleaningTxToDirPathId[txId] = pathId;
        state.TxIds.insert(txId);

        auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), TabletID());
        auto& record = propose->Record;

        auto tablePath = TPath::Init(tablePathId, this);

        auto& modifyScheme = *record.AddTransaction();
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropTable);
        modifyScheme.SetWorkingDir(tablePath.Parent().PathString());
        modifyScheme.SetInternal(true);

        auto& drop = *modifyScheme.MutableDrop();
        drop.SetName(tablePath.LeafName());

        Send(SelfId(), std::move(propose));
    }

    if (state.TablesToDrop.empty()) {
        ContinueBackgroundCleaning(pathId);
    }

    return NOperationQueue::EStartStatus::EOperationRunning;
}

bool TSchemeShard::ContinueBackgroundCleaning(const TPathId& pathId) {
    auto& state = BackgroundCleaningState.at(pathId);

    auto processNextDir = [&]() {
        auto ctx = ActorContext();

        const auto txId = GetCachedTxId(ctx);
        BackgroundCleaningTxToDirPathId[txId] = pathId;
        state.TxIds.insert(txId);

        const auto nextDirPathId = state.DirsToRemove.back();
        state.DirsToRemove.pop_back();

        auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), TabletID());
        auto& record = propose->Record;

        auto dirPath = TPath::Init(nextDirPathId, this);

        auto& modifyScheme = *record.AddTransaction();
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpRmDir);
        modifyScheme.SetWorkingDir(dirPath.Parent().PathString());
        modifyScheme.SetInternal(true);

        auto& drop = *modifyScheme.MutableDrop();
        drop.SetName(dirPath.LeafName());

        Send(SelfId(), std::move(propose));
    };

    if (state.TablesToDrop.empty() && state.DirsToRemove.empty()) {
        CleanBackgroundCleaningState(pathId);
        BackgroundCleaningQueue->OnDone(pathId);
        return false;
    } else if (state.TablesToDrop.empty()) {
        processNextDir();
    } else {
        state.TablesToDrop.pop_back();
        if (state.TablesToDrop.empty()) {
            processNextDir();
        }
    }

    return true;
}

void TSchemeShard::HandleBackgroundCleaningCompletionResult(const TTxId& txId) {
    const auto pathId = BackgroundCleaningTxToDirPathId.at(txId);
    Y_ABORT_UNLESS(BackgroundCleaningState.at(pathId).TxIds.contains(txId));

    auto ctx = ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Get BackgroundCleaning CompletionResult "
        "for txId# " << txId
        << ", next wakeup# " << BackgroundCleaningQueue->GetWakeupDelta()
        << ", in queue# " << BackgroundCleaningQueue->GetRate() << " cleaning events"
        << ", running# " << BackgroundCleaningQueue->RunningSize() << " cleaning events"
        << " at schemeshard " << TabletID());

    ContinueBackgroundCleaning(pathId);
}

void TSchemeShard::OnBackgroundCleaningTimeout(const TPathId& pathId) {
    auto info = ResolveTempDirInfo(pathId);

    auto ctx = ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "BackgroundCleaning timeout "
        "for temp dir# " << JoinPath({info->WorkingDir, info->Name})
        << ", ownerId# " << info->TempDirOwnerActorId
        << ", next wakeup# " << BackgroundCleaningQueue->GetWakeupDelta()
        << ", in queue# " << BackgroundCleaningQueue->GetRate() << " cleaning events"
        << ", running# " << BackgroundCleaningQueue->RunningSize() << " cleaning events"
        << " at schemeshard " << TabletID());
}

void TSchemeShard::Handle(TEvPrivate::TEvRetryNodeSubscribe::TPtr& ev, const TActorContext&) {
    auto& nodeStates = TempDirsState.NodeStates;
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
        TTempDirsState::TRetryState& state) {
    if (state.CurrentDelay == TDuration::Zero()) {
        state.CurrentDelay =
            TDuration::MilliSeconds(backgroundCleaningRetrySettings.GetStartDelayMs());
    }
    return state.CurrentDelay;
}

TDuration GetDelay(
    const NKikimrConfig::TBackgroundCleaningConfig::TRetrySettings& backgroundCleaningRetrySettings,
    TTempDirsState::TRetryState& state
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
    auto& nodeStates = TempDirsState.NodeStates;
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
            auto& tempDirsByOwner = TempDirsState.TempDirsByOwner;

            auto itTempTables = tempDirsByOwner.find(ownerActorId);
            if (itTempTables == tempDirsByOwner.end()) {
                continue;
            }

            auto& currentTempTables = itTempTables->second;
            for (auto& pathId: currentTempTables) {
                EnqueueBackgroundCleaning(pathId);
            }
            tempDirsByOwner.erase(itTempTables);
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
    auto& tempDirsByOwner = TempDirsState.TempDirsByOwner;

    auto ownerActorId = ev->Sender;
    auto it = tempDirsByOwner.find(ownerActorId);
    if (it == tempDirsByOwner.end()) {
        return false;
    }

    auto& currentTempTables = it->second;

    for (auto& pathId: currentTempTables) {
        EnqueueBackgroundCleaning(pathId);
    }
    tempDirsByOwner.erase(it);

    auto& nodeStates = TempDirsState.NodeStates;
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
    const auto pathId = BackgroundCleaningTxToDirPathId.at(txId);
    Y_ABORT_UNLESS(BackgroundCleaningState.at(pathId).TxIds.contains(txId));

    auto ctx = ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Get BackgroundCleaning TransactionResult "
        "for txId# " << txId
        << ", next wakeup# " << BackgroundCleaningQueue->GetWakeupDelta()
        << ", in queue# " << BackgroundCleaningQueue->GetRate() << " cleaning events"
        << ", running# " << BackgroundCleaningQueue->RunningSize() << " cleaning events"
        << " at schemeshard " << TabletID());

    const NKikimrScheme::TEvModifySchemeTransactionResult &record = result->Get()->Record;

    switch (record.GetStatus()) {
    case NKikimrScheme::EStatus::StatusPathDoesNotExist:
    case NKikimrScheme::EStatus::StatusSuccess: {
        ContinueBackgroundCleaning(pathId);
        break;
    }
    case NKikimrScheme::EStatus::StatusAccepted:
        Send(SelfId(), new TEvSchemeShard::TEvNotifyTxCompletion(record.GetTxId()));
        break;
    default: {
        CleanBackgroundCleaningState(pathId);
        BackgroundCleaningQueue->OnDone(pathId);
        EnqueueBackgroundCleaning(pathId);
        break;
    }
    }
}

void TSchemeShard::CleanBackgroundCleaningState(const TPathId& pathId) {
    const auto& state = BackgroundCleaningState.at(pathId);
    for (const auto& txId : state.TxIds) {
        BackgroundCleaningTxToDirPathId.erase(txId);
    }
    BackgroundCleaningState.erase(pathId);
}

void TSchemeShard::ClearTempDirsState() {
    auto ctx = ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "Clear TempDirsState with owners number: "
        << TempDirsState.TempDirsByOwner.size());

    if (BackgroundCleaningQueue) {
        auto& nodeStates = TempDirsState.NodeStates;
        for (const auto& [nodeId, nodeState] : nodeStates) {
            Send(new IEventHandle(TActivationContext::InterconnectProxy(nodeId), SelfId(),
                new TEvents::TEvUnsubscribe, 0));
        }
        BackgroundCleaningQueue->Clear();
    }
    TempDirsState.TempDirsByOwner.clear();
    TempDirsState.NodeStates.clear();

    BackgroundCleaningTxToDirPathId.clear();
    BackgroundCleaningState.clear();
}

} // NKikimr::NSchemeShard
