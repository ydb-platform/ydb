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
    auto& tempTablesBySession = TempTablesState.TempTablesBySession;

    auto it = tempTablesBySession.find(info.second);
    if (it == tempTablesBySession.end()) {
        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    auto tempTableIt = it->second.find(TTempTableId{info.first.first, info.first.second});
    if (tempTableIt == it->second.end()) {
        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    auto ctx = ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "RunBackgroundCleaning "
        "for temp table# " << JoinPath({info.first.first, info.first.second})
        << ", sessionId# " << info.second
        << ", next wakeup# " << BackgroundCleaningQueue->GetWakeupDelta()
        << ", rate# " << BackgroundCleaningQueue->GetRate()
        << ", in queue# " << BackgroundCleaningQueue->Size() << " cleaning events"
        << ", running# " << BackgroundCleaningQueue->RunningSize() << " cleaning events"
        << " at schemeshard " << TabletID());

    auto ev = MakeHolder<TEvPrivate::TEvDropTempTable>();
    ev->WorkingDir = info.first.first;
    ev->Name = info.first.second;

    ctx.Register(new TTempTableDropStarter(ctx.SelfID, std::move(ev)));

    return NOperationQueue::EStartStatus::EOperationRunning;
}

void TSchemeShard::HandleBackgroundCleaningCompletionResult(ui64 txId) {
    auto txsIt = BackgroundCleaningTxs.find(txId);
    if (txsIt == BackgroundCleaningTxs.end()) {
        return;
    }
    BackgroundCleaningQueue->OnDone(TBackgroundCleaningInfo(
        std::move(txsIt->second.WorkingDir),
        std::move(txsIt->second.Name),
        TActorId())
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
    BackgroundCleaningTxs[txId] = TTempTableId{ev->Get()->WorkingDir, ev->Get()->Name};

    Send(SelfId(), std::move(propose));
}

void TSchemeShard::OnBackgroundCleaningTimeout(const TBackgroundCleaningInfo& info) {
    auto ctx = ActorContext();

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Background cleaning timeout "
        "for temp table# " << JoinPath({info.first.first, info.first.second})
        << ", sessionId# " << info.second
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
        for (const auto& sessionActorId: nodeState.Sessions) {
            auto& tempTablesBySession = TempTablesState.TempTablesBySession;

            auto itTempTables = tempTablesBySession.find(sessionActorId);
            if (itTempTables == tempTablesBySession.end()) {
                continue;
            }

            auto& currentTempTables = itTempTables->second;
            for (auto& tempTableId: currentTempTables) {
                EnqueueBackgroundCleaning(
                    TBackgroundCleaningInfo(
                        std::move(tempTableId.WorkingDir),
                        std::move(tempTableId.Name),
                        sessionActorId));
            }
            tempTablesBySession.erase(itTempTables);
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

    for (const auto& sessionActorId: nodeState.Sessions) {
        Send(new IEventHandle(sessionActorId, SelfId(),
            new TEvSchemeShard::TEvSessionActorAck(),
            IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession));
    }
    retryState.LastRetryAt = now;
    return;
}

bool TSchemeShard::CheckSessionUndelivered(TEvents::TEvUndelivered::TPtr& ev) {
    auto& tempTablesBySession = TempTablesState.TempTablesBySession;

    auto sessionActorId = ev->Sender;
    auto it = tempTablesBySession.find(sessionActorId);
    if (it == tempTablesBySession.end()) {
        return false;
    }

    auto& currentTempTables = it->second;

    for (auto& tempTableId: currentTempTables) {
        EnqueueBackgroundCleaning(
            TBackgroundCleaningInfo(
                std::move(tempTableId.WorkingDir),
                std::move(tempTableId.Name),
                sessionActorId));
    }
    tempTablesBySession.erase(it);

    auto& nodeStates = TempTablesState.NodeStates;
    auto itNodeStates = nodeStates.find(sessionActorId.NodeId());
    if (itNodeStates == nodeStates.end()) {
        return true;
    }
    auto itSession = itNodeStates->second.Sessions.find(sessionActorId);
    if (itSession == itNodeStates->second.Sessions.end()) {
        return true;
    }
    itNodeStates->second.Sessions.erase(itSession);
    if (itNodeStates->second.Sessions.empty()) {
        nodeStates.erase(itNodeStates);
        Send(new IEventHandle(TActivationContext::InterconnectProxy(sessionActorId.NodeId()), SelfId(),
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
    case NKikimrScheme::EStatus::StatusSuccess:
        BackgroundCleaningQueue->OnDone(TBackgroundCleaningInfo(
            std::move(txsIt->second.WorkingDir),
            std::move(txsIt->second.Name),
            TActorId())
        );
        break;
    case NKikimrScheme::EStatus::StatusAccepted:
        Send(SelfId(), new TEvSchemeShard::TEvNotifyTxCompletion(txId));
        break;
    default:
        BackgroundCleaningQueue->OnDone(TBackgroundCleaningInfo(
            std::move(txsIt->second.WorkingDir),
            std::move(txsIt->second.Name),
            TActorId())
        );
        break;
    }
}

void TSchemeShard::ClearTempTablesState() {
    auto ctx = ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "Clear TempTablesState with sessions number: "
        << TempTablesState.TempTablesBySession.size());

    if (BackgroundCleaningQueue) {
        auto& tempTablesBySession = TempTablesState.TempTablesBySession;
        for (const auto& [sessionActorId, tempTables] : tempTablesBySession) {
            for (const auto& tempTableId : tempTables) {
                EnqueueBackgroundCleaning(
                    TBackgroundCleaningInfo(
                        std::move(tempTableId.WorkingDir),
                        std::move(tempTableId.Name),
                        sessionActorId));
            }
        }

        auto& nodeStates = TempTablesState.NodeStates;
        for (const auto& [nodeId, nodeState] : nodeStates) {
            Send(new IEventHandle(TActivationContext::InterconnectProxy(nodeId), SelfId(),
                new TEvents::TEvUnsubscribe, 0));
        }
        BackgroundCleaningQueue->Clear();
    }
    TempTablesState.TempTablesBySession.clear();
    TempTablesState.NodeStates.clear();
}

} // NKikimr::NSchemeShard
