#include "schemeshard_impl.h"

#include <ydb/core/tx/tx_proxy/proxy.h>

namespace NKikimr::NSchemeShard {

class TTempTableDropStarter: public TActorBootstrapped<TTempTableDropStarter> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SCHEMESHARD_TEMP_TABLE_DROP_STARTER;
    }

    explicit TTempTableDropStarter(const TActorId& ssActorId, THolder<TEvPrivate::TEvDropTempTable>&& req)
        : SSActorId(ssActorId)
        , Request(std::move(req)) // without txId
    {
    }

    void Bootstrap() {
        AllocateTxId();
        Become(&TTempTableDropStarter::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, Handle)
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    void AllocateTxId() {
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId);
    }

    void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        Request->TxId = ev->Get()->TxId;
        Send(SSActorId, Request.Release());
        PassAway();
    }

private:
    const TActorId SSActorId;
    THolder<TEvPrivate::TEvDropTempTable> Request;
};

NOperationQueue::EStartStatus TSchemeShard::StartBackgroundCleaning(const TBackgroundCleaningInfo& info) {
    auto& tempTablesByOwner = TempTablesState.TempTablesByOwner;

    auto it = tempTablesByOwner.find(info.second);
    if (it == tempTablesByOwner.end()) {
        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    auto tempTableIt = it->second.find(info.GetPathId());
    if (tempTableIt == it->second.end()) {
        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    auto ctx = ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "RunBackgroundCleaning "
        "for temp table# " << JoinPath({info.first.first, info.first.second})
        << ", ownerId# " << info.second
        << ", next wakeup# " << BackgroundCleaningQueue->GetWakeupDelta()
        << ", rate# " << BackgroundCleaningQueue->GetRate()
        << ", in queue# " << BackgroundCleaningQueue->Size() << " cleaning events"
        << ", running# " << BackgroundCleaningQueue->RunningSize() << " cleaning events"
        << " at schemeshard " << TabletID());

    auto ev = MakeHolder<TEvPrivate::TEvDropTempTable>();
    ev->WorkingDir = info.first.first;
    ev->Name = info.first.second;
    ev->PathId = info.GetPathId();

    ctx.Register(new TTempTableDropStarter(ctx.SelfID, std::move(ev)));

    return NOperationQueue::EStartStatus::EOperationRunning;
}

void TSchemeShard::HandleBackgroundCleaningCompletionResult(ui64 txId) {
    auto txsIt = BackgroundCleaningTxs.find(txId);
    if (txsIt == BackgroundCleaningTxs.end()) {
        return;
    }

    auto tempTablePath = TPath::Init(txsIt->second, this);
    auto tempTableName = tempTablePath.LeafName();
    auto tempTableWorkingDir = tempTablePath.Parent().PathString();

    BackgroundCleaningQueue->OnDone(TBackgroundCleaningInfo(
        std::move(tempTableWorkingDir),
        std::move(tempTableName),
        TActorId(),
        txsIt->second)
    );
}

void TSchemeShard::Handle(TEvPrivate::TEvDropTempTable::TPtr& ev, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "Start to drop temp table: " << JoinPath({ev->Get()->WorkingDir, ev->Get()->Name}));
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(ev->Get()->TxId), TabletID());
    auto& record = propose->Record;

    auto& modifyScheme = *record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropTable);
    modifyScheme.SetWorkingDir(ev->Get()->WorkingDir);

    auto& drop = *modifyScheme.MutableDrop();
    drop.SetName(ev->Get()->Name);

    ui64 txId = ev->Get()->TxId;
    BackgroundCleaningTxs[txId] = ev->Get()->PathId;

    Send(SelfId(), std::move(propose));
}

void TSchemeShard::OnBackgroundCleaningTimeout(const TBackgroundCleaningInfo& info) {
    auto ctx = ActorContext();

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Background cleaning timeout "
        "for temp table# " << JoinPath({info.first.first, info.first.second})
        << ", ownerId# " << info.second
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
                auto tempTablePath = TPath::Init(pathId, this);
                auto tempTableName = tempTablePath.LeafName();
                auto tempTableWorkingDir = tempTablePath.Parent().PathString();


                EnqueueBackgroundCleaning(
                    TBackgroundCleaningInfo(
                        std::move(tempTableWorkingDir),
                        std::move(tempTableName),
                        ownerActorId,
                        pathId));
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
        auto tempTablePath = TPath::Init(pathId, this);
        auto tempTableName = tempTablePath.LeafName();
        auto tempTableWorkingDir = tempTablePath.Parent().PathString();

        EnqueueBackgroundCleaning(
            TBackgroundCleaningInfo(
                std::move(tempTableWorkingDir),
                std::move(tempTableName),
                ownerActorId,
                pathId));
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

void TSchemeShard::EnqueueBackgroundCleaning(const TBackgroundCleaningInfo& info) {
    if (BackgroundCleaningQueue) {
        BackgroundCleaningQueue->Enqueue(std::move(info));
    }
}

void TSchemeShard::RemoveBackgroundCleaning(const TBackgroundCleaningInfo& info) {
    if (BackgroundCleaningQueue) {
        BackgroundCleaningQueue->Remove(std::move(info));
    }
}

void TSchemeShard::HandleBackgroundCleaningTransactionResult(
        TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& result) {
    const auto txId = result->Get()->Record.GetTxId();

    auto ctx = ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "Get TransactionResult of temp table drop with txId: " << txId);

    auto txsIt = BackgroundCleaningTxs.find(txId);
    if (txsIt == BackgroundCleaningTxs.end()) {
        return;
    }

    const NKikimrScheme::TEvModifySchemeTransactionResult &record = result->Get()->Record;

    switch (record.GetStatus()) {
    case NKikimrScheme::EStatus::StatusAlreadyExists:
    case NKikimrScheme::EStatus::StatusSuccess: {
        auto tempTablePath = TPath::Init(txsIt->second, this);
        auto tempTableName = tempTablePath.LeafName();
        auto tempTableWorkingDir = tempTablePath.Parent().PathString();

        BackgroundCleaningQueue->OnDone(TBackgroundCleaningInfo(
            std::move(tempTableWorkingDir),
            std::move(tempTableName),
            TActorId(),
            txsIt->second)
        );
        break;
    }
    case NKikimrScheme::EStatus::StatusAccepted:
        Send(SelfId(), new TEvSchemeShard::TEvNotifyTxCompletion(txId));
        break;
    default: {
        auto tempTablePath = TPath::Init(txsIt->second, this);
        auto tempTableName = tempTablePath.LeafName();
        auto tempTableWorkingDir = tempTablePath.Parent().PathString();

        BackgroundCleaningQueue->OnDone(TBackgroundCleaningInfo(
            std::move(tempTableWorkingDir),
            std::move(tempTableName),
            TActorId(),
            txsIt->second)
        );
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
