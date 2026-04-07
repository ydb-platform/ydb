#include "datashard_impl.h"
#include "datashard_locks_db.h"
#include "setup_sys_locks.h"

#include <ydb/library/actors/async/continuation.h>

namespace NKikimr::NDataShard {

using namespace NLongTxService;

enum class ETxLockRows {
    Restart,
    Rollback,
    CommitSync,
    CommitAsync,
};

class TDataShard::TTxLockRows
    : public NTabletFlatExecutor::ITransaction
{
public:
    TTxLockRows(TAsyncContinuation<void> continuation, std::function<ETxLockRows(TTransactionContext&)> lambda)
        : Continuation(std::move(continuation))
        , Lambda(std::move(lambda))
    {}

    TTxType GetTxType() const override { return TXTYPE_LOCK_ROWS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        if (!Continuation) {
            return true;
        }

        switch (Lambda(txc)) {
            case ETxLockRows::Restart:
                return false;

            case ETxLockRows::Rollback:
                txc.DB.RollbackChanges();
                Continuation.Resume();
                return true;

            case ETxLockRows::CommitSync:
                return true;

            case ETxLockRows::CommitAsync:
                Continuation.Resume();
                return true;
        }
    }

    void Complete(const TActorContext&) override {
        if (Continuation) {
            Continuation.Resume();
        }
    }

    template<class TCallback>
    static async<void> Run(TDataShard* self, const TCallback& callback) {
        co_await WithAsyncContinuation<void>([&](TAsyncContinuation<void> continuation) {
            self->Execute(new TTxLockRows(std::move(continuation), [&callback](TTransactionContext& txc) {
                return callback(txc);
            }));
        });
    }

private:
    TAsyncContinuation<void> Continuation;
    std::function<ETxLockRows(TTransactionContext&)> Lambda;
};

class TDataShard::TLockRowsTxObserver : public NTable::ITransactionObserver {
public:
    TRowVersion VolatileVersion = TRowVersion::Min();

    TLockRowsTxObserver(TDataShard& self)
        : Self(self)
    {}

    void OnSkipUncommitted(ui64 txId) override {
        if (auto* info = Self.GetVolatileTxManager().FindByCommitTxId(txId)) {
            if (info->State != EVolatileTxState::Aborting) {
                VolatileVersion = Max(VolatileVersion, info->Version);
                Self.SysLocksTable().AddVolatileDependency(txId);
            }
        } else {
            Self.SysLocksTable().AddReadConflict(txId);
        }
    }

    void OnSkipCommitted(const TRowVersion&) override {
        // We don't read from snapshot and should never skip committed deltas
        Y_ENSURE(false, "unreachable");
    }

    void OnSkipCommitted(const TRowVersion&, ui64) override {
        // We don't read from snapshot and should never skip committed deltas
        Y_ENSURE(false, "unreachable");
    }

    void OnApplyCommitted(const TRowVersion&) override {
        // We don't conflict with committed rows
        Y_ENSURE(false, "unreachable");
    }

    void OnApplyCommitted(const TRowVersion&, ui64) override {
        // We don't conflict with committed rows
        Y_ENSURE(false, "unreachable");
    }

private:
    TDataShard& Self;
};

class TDataShard::TLockRowsNotifyWaitGuard {
public:
    TLockRowsNotifyWaitGuard(TDataShard& self, TLockRowsRequestState& state, TLockInfo::TPtr lock, TLockInfo::TPtr otherLock)
        : Self(self)
        , Lock(std::move(lock))
        , OtherLock(std::move(otherLock))
    {
        if (Lock->GetLockId() != OtherLock->GetLockId()) {
            RequestId = Self.NextTieBreakerIndex++;
            Self.LockRowsWaitRequests[RequestId] = &state;
            Self.Send(
                MakeLongTxServiceID(Self.SelfId().NodeId()),
                new TEvLongTxService::TEvWaitingLockAdd(
                    RequestId,
                    { Lock->GetLockId(), Lock->GetLockNodeId() },
                    { OtherLock->GetLockId(), OtherLock->GetLockNodeId() }));
            Sent = true;
        }
    }

    ~TLockRowsNotifyWaitGuard() {
        if (Sent && TlsActivationContext) {
            Self.Send(
                MakeLongTxServiceID(Self.SelfId().NodeId()),
                new TEvLongTxService::TEvWaitingLockRemove(RequestId));
        }
        // Note: we may fail to send the request with an exception, after
        // inserting request into LockRowsWaitRequests. Make sure the request
        // is removed when we exit the scope.
        if (RequestId) {
            Self.LockRowsWaitRequests.erase(RequestId);
        }
    }

private:
    TDataShard& Self;
    const TLockInfo::TPtr Lock;
    const TLockInfo::TPtr OtherLock;
    ui64 RequestId = 0;
    bool Sent = false;
};

void TDataShard::CheckLockRowsRejectAll() {
    bool cancelled = false;
    for (auto it = LockRowsRequests.begin(); it != LockRowsRequests.end(); ++it) {
        auto& state = it->second;

        if (state.Result) {
            // This request has some result already
            continue;
        }

        if (CheckLockRowsReject(state)) {
            // Asynchronously cancel the request
            state.Scope.Cancel();
            cancelled = true;
        }
    }

    if (cancelled) {
        // Waiting queue may have been updated
        UpdateProposeQueueSize();
    }
}

bool TDataShard::CheckLockRowsReject(TLockRowsRequestState& state) {
    if (state.Result) {
        return true;
    }

    auto makeError = [&](NKikimrDataEvents::TEvLockRowsResult::EStatus status, TString errorMsg) {
        return std::make_unique<NEvents::TDataEvents::TEvLockRowsResult>(
            TabletID(), state.RequestId.RequestId, status, std::move(errorMsg));
    };

    switch (GetState()) {
        case TShardState::Ready:
            break;

        case TShardState::SplitSrcWaitForNoTxInFlight:
        case TShardState::SplitSrcMakeSnapshot:
        case TShardState::SplitSrcSendingSnapshot:
        case TShardState::SplitSrcWaitForPartitioningChanged:
            state.Result = makeError(NKikimrDataEvents::TEvLockRowsResult::STATUS_OVERLOADED, TStringBuilder()
                << "Shard " << TabletID() << " has moved to shard state " << GetState());
            return true;

        default:
            state.Result = makeError(NKikimrDataEvents::TEvLockRowsResult::STATUS_INTERNAL_ERROR, TStringBuilder()
                << "Shard " << TabletID() << " has an unexpected state " << GetState());
            return true;
    }

    if (IsStopping()) {
        state.Result = makeError(NKikimrDataEvents::TEvLockRowsResult::STATUS_OVERLOADED, TStringBuilder()
            << "Shard " << TabletID() << " is stopping");
        return true;
    }

    if (IsReplicated()) {
        state.Result = makeError(NKikimrDataEvents::TEvLockRowsResult::STATUS_BAD_REQUEST, TStringBuilder()
            << "Shard " << TabletID() << " is a replicated table and cannot be modified by transactions");
        return true;
    }

    return false;
}

TLockInfo::TPtr TDataShard::FindValidLockOwner(ui64 lockId) {
    auto lock = SysLocksTable().GetRawLock(lockId);
    if (!lock) {
        // When lock is removed it implicitly unlocks all locks
        return nullptr;
    }

    if (lock->IsBroken()) {
        // When lock is broken it implicitly unlocks all locks
        return nullptr;
    }

    auto* info = GetVolatileTxManager().FindByCommitTxId(lockId);
    if (info) {
        // When volatile transaction starts to commit it implicitly unlocks all locks
        return nullptr;
    }

    return lock;
}

void TDataShard::HandleLockRowsRequest(NEvents::TDataEvents::TEvLockRows::TPtr ev) {
    auto* msg = ev->Get();

    TLockRowsRequestId requestId(ev->Sender, msg->Record.GetRequestId());

    auto makeError = [&](NKikimrDataEvents::TEvLockRowsResult::EStatus status, TString errorMsg) {
        return std::make_unique<NEvents::TDataEvents::TEvLockRowsResult>(
            TabletID(), requestId.RequestId, status, std::move(errorMsg));
    };

    auto sendError = [&](NKikimrDataEvents::TEvLockRowsResult::EStatus status, TString errorMsg) {
        Send(ev->Sender, makeError(status, std::move(errorMsg)).release(), 0, ev->Cookie);
    };

    if (auto it = LockRowsRequests.find(requestId); it != LockRowsRequests.end()) {
        // Duplicate request id is a hard error. Cancel the older request.
        it->second.Scope.Cancel();

        // Cancellation is not recursive, wait until it's unregistered.
        co_await it->second.Finished.Wait();

        sendError(NKikimrDataEvents::TEvLockRowsResult::STATUS_BAD_REQUEST, TStringBuilder()
            << "Duplicate request id " << requestId.RequestId << " from actor " << requestId.Sender);
        co_return;
    }

    if (msg->Record.GetPayloadFormat() != NKikimrDataEvents::FORMAT_CELLVEC) {
        sendError(NKikimrDataEvents::TEvLockRowsResult::STATUS_BAD_REQUEST, TStringBuilder()
            << "Unsupported payload format " << msg->Record.GetPayloadFormat());
        co_return;
    }

    NTable::ELockMode lockMode;
    switch (msg->Record.GetLockMode()) {
        case NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE:
            lockMode = NTable::ELockMode::Exclusive;
            break;

        case NKikimrDataEvents::PESSIMISTIC_SHARED:
            lockMode = NTable::ELockMode::Shared;
            break;

        case NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE_NO_KEY:
            lockMode = NTable::ELockMode::NoKeyExclusive;
            break;

        case NKikimrDataEvents::PESSIMISTIC_SHARED_KEY:
            lockMode = NTable::ELockMode::KeyShared;
            break;

        default:
            sendError(NKikimrDataEvents::TEvLockRowsResult::STATUS_BAD_REQUEST, TStringBuilder()
                << "Unsupported lock mode " << NKikimrDataEvents::ELockMode_Name(msg->Record.GetLockMode()));
            co_return;
    }

    const ui64 lockId = msg->Record.GetLockId();
    const ui32 lockNodeId = msg->Record.GetLockNodeId();
    const TTableId tableId = msg->GetTableId();
    const bool skipLocked = msg->Record.GetSkipLocked();

    {
        NKikimrTxDataShard::TEvProposeTransactionResult::EStatus rejectStatus;
        ERejectReasons rejectReasons;
        TString rejectDescription;
        bool reject = CheckDataTxReject("LockRows", ActorContext(), rejectStatus, rejectReasons, rejectDescription);
        if (reject) {
            NKikimrDataEvents::TEvLockRowsResult::EStatus status;
            switch (rejectStatus) {
                case NKikimrTxDataShard::TEvProposeTransactionResult::OVERLOADED:
                    status = NKikimrDataEvents::TEvLockRowsResult::STATUS_OVERLOADED;
                    break;
                case NKikimrTxDataShard::TEvProposeTransactionResult::ERROR:
                    if ((rejectReasons & ERejectReasons::WrongState) != ERejectReasons::None) {
                        status = NKikimrDataEvents::TEvLockRowsResult::STATUS_WRONG_SHARD_STATE;
                    } else {
                        status = NKikimrDataEvents::TEvLockRowsResult::STATUS_INTERNAL_ERROR;
                    }
                    break;
                default:
                    Y_ENSURE(false, "Unexpected rejectStatus " << rejectStatus);
            }

            sendError(status, rejectDescription);
            co_return;
        }
    }

    auto initRequest = [&]() -> TLockRowsRequestState& {
        auto res = LockRowsRequests.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(requestId),
            std::forward_as_tuple(requestId));
        Y_ENSURE(res.second);
        SetCounter(COUNTER_LOCK_ROWS_INFLIGHT, LockRowsRequests.size());
        return res.first->second;
    };

    TLockRowsRequestState& state = initRequest();
    Y_DEFER {
        auto it = LockRowsRequests.find(requestId);
        Y_ABORT_UNLESS(it != LockRowsRequests.end());
        Y_ABORT_UNLESS(&it->second == &state);
        state.Scope.Cancel();
        state.Finished.NotifyAll();
        LockRowsRequests.erase(it);
        // Note: update counters unless actor system is shutting down
        if (TlsActivationContext) {
            // TODO: is this actually safe? Counters are owned by executor,
            // which may be destroyed before the datashard actor.
            SetCounter(COUNTER_LOCK_ROWS_INFLIGHT, LockRowsRequests.size());
        }
    };

    // Make sure we send any pending result before going away
    Y_DEFER {
        if (state.Result && TlsActivationContext) {
            Send(ev->Sender, state.Result.release(), 0, ev->Cookie);
        }
    };

    // Establish a cancellation scope for TEvLockRowsCancel
    state.Scope = co_await TAsyncCancellationScope::WithCurrentHandler();

    // Register this request in a pipe server for cancellation
    if (auto* pipeServer = PipeServers.FindPtr(ev->Recipient)) {
        pipeServer->LockRowsRequests.PushBack(&state);
    }

    auto setWaiting = [&](bool waiting) {
        if (waiting != state.Waiting && TlsActivationContext) {
            if (waiting) {
                IncCounter(COUNTER_LOCK_ROWS_WAITING);
            } else {
                DecCounter(COUNTER_LOCK_ROWS_WAITING);
            }
            state.Waiting = waiting;
        }
    };
    Y_DEFER {
        setWaiting(false);
    };

    if (MediatorStateWaiting) {
        // Wait until mediator state is restored
        co_await MediatorStateWaitingCoroutines.Wait([&]{
            UpdateProposeQueueSize();
            setWaiting(true);
        });
    }

    if (Pipeline.HasProposeDelayers()) {
        // Wait until dependency graph is restored
        co_await DelayedProposeCoroutines.Wait([&]{
            UpdateProposeQueueSize();
            setWaiting(true);
        });
    }

    TRowVersion snapshot = TRowVersion::Max();
    if (msg->Record.HasSnapshot()) {
        setWaiting(true);
        const auto& protoSnapshot = msg->Record.GetSnapshot();
        snapshot = TRowVersion(protoSnapshot.GetStep(), protoSnapshot.GetTxId());
        bool success = co_await Pipeline.WaitForSnapshot(snapshot);
        if (!success) {
            sendError(NKikimrDataEvents::TEvLockRowsResult::STATUS_OVERLOADED, TStringBuilder()
                << "Shard " << TabletID() << " has too many concurrent requests");
            co_return;
        }
    }

    setWaiting(false);

    // Cycle in the low-priority queue before doing anything expensive
    co_await LowPriorityQueue.Next();

    std::vector<ui32> columnIds;
    columnIds.reserve(msg->Record.ColumnIdsSize());
    for (size_t i = 0; i < msg->Record.ColumnIdsSize(); ++i) {
        columnIds.push_back(msg->Record.GetColumnIds(i));
    }

    TSerializedCellMatrix matrix;
    try {
        matrix = msg->GetCellMatrix();
    } catch (const std::exception& e) {
        sendError(NKikimrDataEvents::TEvLockRowsResult::STATUS_BAD_REQUEST, TStringBuilder() << e.what());
        co_return;
    }

    // Don't keep long-term references to paylods which are no longer needed
    msg->StripPayload();

    if (matrix.GetColCount() != columnIds.size()) {
        sendError(NKikimrDataEvents::TEvLockRowsResult::STATUS_BAD_REQUEST, TStringBuilder()
            << "TSerializedCellVec payload has " << matrix.GetColCount() << " columns, expected " << columnIds.size() << " columns");
        co_return;
    }

    auto checkSchema = [&]() -> ui32 {
        auto tableInfo = FindUserTable(tableId.PathId);
        if (!tableInfo) {
            state.Result = makeError(NKikimrDataEvents::TEvLockRowsResult::STATUS_SCHEME_ERROR, TStringBuilder()
                << "Table '" << tableId << "' doesn't exist at shard " << TabletID());
            return 0;
        }

        if (tableInfo->GetTableSchemaVersion() != 0 && tableId.SchemaVersion != tableInfo->GetTableSchemaVersion()) {
            state.Result = makeError(NKikimrDataEvents::TEvLockRowsResult::STATUS_SCHEME_CHANGED, TStringBuilder()
                << "Table '" << tableInfo->Path << "': schema changed at shard " << TabletID());
            return 0;
        }

        if (columnIds.size() != tableInfo->KeyColumnIds.size()) {
            state.Result = makeError(NKikimrDataEvents::TEvLockRowsResult::STATUS_SCHEME_ERROR, TStringBuilder()
                << "Table '" << tableInfo->Path << "' has " << tableInfo->KeyColumnIds.size() << " key columns, which doesn't match " << columnIds.size() << " in the request");
            return 0;
        }

        for (size_t i = 0; i < tableInfo->KeyColumnIds.size(); ++i) {
            if (columnIds[i] != tableInfo->KeyColumnIds[i]) {
                state.Result = makeError(NKikimrDataEvents::TEvLockRowsResult::STATUS_SCHEME_ERROR, TStringBuilder()
                    << "Column id mismatch (" << columnIds[i] << " != " << tableInfo->KeyColumnIds[i] << ") at position " << i);
                return 0;
            }
        }

        return tableInfo->LocalTid;
    };

    TVector<TSysLocks::TLock> takenLocks;
    auto applyLocks = [&]() {
        bool ok = true;
        auto res = SysLocksTable().ApplyLocks();
        if (!res.first.empty()) {
            Y_ENSURE(res.first.size() == 1);
            if (!takenLocks.empty()) {
                Y_ENSURE(takenLocks.size() == 1);
                // Perform a a very simplistic lock merge
                ok = (
                    takenLocks[0].Generation == res.first[0].Generation &&
                    takenLocks[0].Counter == res.first[0].Counter);
            }
            takenLocks = std::move(res.first);
            if (!ok) {
                takenLocks[0].Counter = TSysTables::TLocksTable::TLock::ErrorBroken;
            }
        }
        return ok;
    };

    TLockInfo::TPtr lock;
    for (const auto& protoLock : msg->Record.GetExistingLocks()) {
        auto existingLockId = protoLock.GetLockId();
        auto existingLock = SysLocksTable().GetRawLock(existingLockId);

        bool isBroken = (
            !existingLock ||
            existingLock->IsBroken() ||
            existingLock->GetGeneration() != protoLock.GetGeneration() ||
            existingLock->GetCounter() != protoLock.GetCounter());

        if (isBroken) {
            state.Result = makeError(NKikimrDataEvents::TEvLockRowsResult::STATUS_LOCKS_BROKEN, TStringBuilder()
                << "Lock " << existingLockId << " is broken at shard " << TabletID());
            state.Result->AddLock(
                existingLockId,
                TabletID(),
                existingLock ? existingLock->GetGeneration() : Generation(),
                existingLock ? existingLock->GetCounter() : Max<ui64>(),
                tableId.PathId.OwnerId,
                tableId.PathId.LocalPathId,
                existingLock ? existingLock->IsWriteLock() : false);
            co_return;
        }

        if (lockId == existingLockId) {
            lock = existingLock;
        }
    }

    TRuntimeLockHolder runtimeLock;
    TVector<TRawTypeValue> typedKey;
    size_t processedKeys = 0;

    auto success = std::make_unique<NEvents::TDataEvents::TEvLockRowsResult>(
            TabletID(), requestId.RequestId, NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS);

    ui64 globalTxId = 0;
    Y_DEFER {
        if (globalTxId) {
            // We may allocate some globalTxId, but find out later we no longer
            // need it, because someone else created the same MultiTxId chain.
            // Avoid wasting this resource and give it to someone else.
            RecycleGlobalTxId(globalTxId);
        }
    };

    while (!state.Result) {
        bool reschedule = false;
        bool needGlobalTxId = false;
        TLockInfo::TPtr waitForLock;

        // We need to run each iteration in a transaction
        co_await TTxLockRows::Run(this, [&](TTransactionContext& txc) {
            if (lock && lock->IsBroken()) {
                state.Result = makeError(NKikimrDataEvents::TEvLockRowsResult::STATUS_LOCKS_BROKEN, TStringBuilder()
                    << "Lock " << lockId << " is broken at shard " << TabletID());
                state.Result->AddLock(
                    lockId,
                    TabletID(),
                    lock->GetGeneration(),
                    lock->GetCounter(),
                    tableId.PathId.OwnerId,
                    tableId.PathId.LocalPathId,
                    lock->IsWriteLock());
                return ETxLockRows::Rollback;
            }

            if (CheckLockRowsReject(state)) {
                Y_ENSURE(state.Result);
                return ETxLockRows::Rollback;
            }

            // Schema may have changed, must recheck on every iteration
            ui32 localTid = checkSchema();
            if (!localTid) {
                Y_ENSURE(state.Result);
                return ETxLockRows::Rollback;
            }

            auto& tableInfo = txc.DB.GetScheme().Tables.at(localTid);

            NIceDb::TNiceDb db(txc.DB);
            TDataShardLocksDb locksDb(*this, txc);
            TSetupSysLocks guardLocks(lockId, lockNodeId, *this, &locksDb);

            // Ensure lock exists or is allowed to be created
            // We need to do that even when we already have a valid lock pointer to establish a lock update
            switch (SysLocksTable().EnsureCurrentLock()) {
                case EEnsureCurrentLock::Success:
                    // Lock is valid, we may continue with reads and side-effects
                    if (lock) {
                        Y_ENSURE(lock == guardLocks.Lock);
                    } else {
                        lock = guardLocks.Lock;
                        lock->SetPessimistic();
                        StartLockRowsBrokenWatcher(requestId, tableId, lock);
                    }
                    SubscribeNewLocks();
                    break;

                case EEnsureCurrentLock::Broken:
                case EEnsureCurrentLock::TooMany:
                case EEnsureCurrentLock::Abort:
                    state.Result = makeError(NKikimrDataEvents::TEvLockRowsResult::STATUS_LOCKS_BROKEN, TStringBuilder()
                        << "Lock " << lockId << " is broken or cannot be created at shard " << TabletID());
                    return ETxLockRows::Rollback;

                case EEnsureCurrentLock::Missing:
                        Y_ENSURE(false, "unreachable");
            }

            bool advanced = false;

            while (processedKeys < matrix.GetRowCount()) {
                TConstArrayRef<TCell> key = matrix.GetRow(processedKeys);

                // TODO: Check if there are operations writing to the current
                // key below our snapshot, and wait for them before trying to
                // lock. This is not strictly necessary, but may help us from
                // breaking the lock immediately due to concurrent serializable
                // transactions.

                // This observer will detect conflicts with uncommitted
                // changes, including undecided volatile transactions.
                auto observer = MakeIntrusive<TLockRowsTxObserver>(*this);

                // Probe the key until the first persistently committed version
                auto row = txc.DB.SelectRowVersion(localTid, key, /* readFlags */ 0, /* tx map */ nullptr, observer);

                // Handle page fault by restarting or rescheduling
                if (row.Ready == NTable::EReady::Page) {
                    if (advanced) {
                        applyLocks();

                        // Commit current changes and retry immediately
                        reschedule = true;
                        return ETxLockRows::CommitAsync;
                    }

                    return ETxLockRows::Restart;
                }

                // Undecided volatile transactions will have non-zero VolatileVersion
                // We don't wait until they are decided and return a modified flag
                // instead. A subsequent re-read will wait for the decision.
                const bool modified = row.RowVersion > snapshot || observer->VolatileVersion > snapshot;

                auto setLockMode = [&](NTable::ELockMode newLockMode, ui64 txId) {
                    typedKey.clear();
                    for (size_t i = 0; i < columnIds.size(); ++i) {
                        if (key[i].IsNull()) {
                            typedKey.emplace_back();
                        } else {
                            auto typeId = tableInfo.Columns.at(columnIds[i]).PType.GetTypeId();
                            typedKey.emplace_back(key[i].Data(), key[i].Size(), typeId);
                        }
                    }

                    GetConflictsCache().GetTableCache(localTid).AddUncommittedWrite(key, txId, txc.DB);
                    txc.DB.LockRowTx(localTid, newLockMode, typedKey, txId);
                    SysLocksTable().SetWriteLock(tableId, key);
                    advanced = true;
                };

                auto finishLocked = [&]() {
                    success->Record.AddLockedKeys(processedKeys);
                    if (modified) {
                        success->Record.AddModifiedKeys(processedKeys);
                    }
                    runtimeLock.Reset();
                    ++processedKeys;
                };

                auto finishSkipped = [&]() {
                    success->Record.AddSkippedKeys(processedKeys);
                    runtimeLock.Reset();
                    ++processedKeys;
                };

                // Special case when this single lock is already the key owner
                // We must not add ourselves to the wait queue in that case
                // Note: we don't try to lock the key out-of-order however
                if (row.LockTxId == lockId) {
                    Y_ENSURE(row.LockMode != NTable::ELockMode::None);
                    Y_ENSURE(row.LockMode != NTable::ELockMode::Multi);
                    if (lockMode > row.LockMode) {
                        NTable::ELockMode combinedLockMode = CombinedRowLockMode(row.LockMode, lockMode);
                        setLockMode(combinedLockMode, lockId);
                    }
                    finishLocked();
                    continue;
                }

                // The first conflicting owner we need to wait for
                TLockInfo::TPtr currentOwner;
                NTable::ELockMode currentLockMode = NTable::ELockMode::None;

                bool multiLockEmpty = false;
                const TMultiTxId* multiLock = nullptr;
                if (row.LockMode == NTable::ELockMode::Multi) {
                    multiLock = MultiTxIdManager.FindMultiTxId(row.LockTxId);
                    if (multiLock) {
                        // Find the first conflicting lock we would need to wait
                        auto enumerator = MultiTxIdManager.EnumerateLocks(multiLock, lockMode);
                        while (auto result = enumerator.Next()) {
                            if (result.LockId != lockId) {
                                // Note: we are not supposed to have removed or broken locks in the owners list
                                currentOwner = FindValidLockOwner(result.LockId);
                                if (currentOwner) {
                                    currentLockMode = result.LockMode;
                                    break;
                                } else {
                                    // TODO: cleanup removed or broken leaf locks from MultiTxId
                                    // Y_DEBUG_ABORT_UNLESS(false, "Unexpected invalid lock contained in MultiTxId");
                                }
                            }
                        }
                        // When we don't have any conflicting locks we might be the only valid owner
                        if (!currentOwner) {
                            multiLockEmpty = true;
                            bool haveOtherLocks = false;
                            enumerator.Reset(multiLock, NTable::ELockMode::None);
                            while (auto result = enumerator.Next()) {
                                if (result.LockId != lockId) {
                                    // We have found some other lock, so we cannot upgrade without waiting
                                    if (FindValidLockOwner(result.LockId)) {
                                        multiLockEmpty = false;
                                        haveOtherLocks = true;
                                    } else {
                                        // TODO: cleanup removed or broken leaf locks from MultiTxId
                                        // Y_DEBUG_ABORT_UNLESS(false, "Unexpected invalid lock contained in MultiTxId");
                                    }
                                } else {
                                    currentLockMode = Max(currentLockMode, result.LockMode);
                                    multiLockEmpty = false;
                                }
                            }
                            // We don't need to lock the row when current lock mode is the same or stronger
                            if (currentLockMode >= lockMode) {
                                finishLocked();
                                continue;
                            }
                            // We can use fast path upgrade when current lock is the only owner
                            if (currentLockMode != NTable::ELockMode::None && !haveOtherLocks) {
                                NTable::ELockMode combinedLockMode = CombinedRowLockMode(currentLockMode, lockMode);
                                MultiTxIdManager.DecrementLockedRowsCount(db, row.LockTxId);
                                setLockMode(combinedLockMode, lockId);
                                finishLocked();
                                continue;
                            }
                        }
                    }
                } else if (row.LockMode != NTable::ELockMode::None) {
                    currentOwner = FindValidLockOwner(row.LockTxId);
                    if (currentOwner) {
                        currentLockMode = row.LockMode;
                    }
                }

                // Don't bother waiting in skipLocked mode when current owner conflicts with us
                if (skipLocked && currentOwner && !IsCompatibleRowLockMode(currentLockMode, lockMode)) {
                    finishSkipped();
                    continue;
                }

                // Key is either not locked, or locked by someone else
                // We need to establish a runtime lock for fair locking
                if (!runtimeLock.IsValid()) {
                    // Note: AddRuntimeLock will group multiple runtime locks
                    // with the same key and lock together, however it will not
                    // do that for different (related) locks. When implementing
                    // safepoints we would need to group all locks from the same
                    // transaction, not just having the same LockId.
                    runtimeLock = SysLocksTable().AddRuntimeLock(tableId, key);
                    Y_ENSURE(runtimeLock.IsValid());
                    if (!runtimeLock.IsOwner()) {
                        if (skipLocked) {
                            finishSkipped();
                            continue;
                        }

                        applyLocks();

                        // Wait until the runtime lock is acquired
                        return ETxLockRows::CommitAsync;
                    }
                }

                Y_ENSURE(runtimeLock.IsOwner());

                // Wait for current conflicting owner to release the lock
                if (currentOwner && !IsCompatibleRowLockMode(currentLockMode, lockMode)) {
                    waitForLock = currentOwner;
                    applyLocks();
                    return ETxLockRows::CommitAsync;
                }

                if (multiLock && !multiLock->HasFlag(EMultiTxIdFlag::Broken) && !multiLockEmpty) {
                    // We don't conflict with current locks, create a new MultiTxId which includes the new lock and mode
                    auto singleLockMode = multiLock->GetLockMode();
                    Y_ENSURE(singleLockMode, "TODO: support extending multiple row lock modes");

                    ui64 newMultiTxId = MultiTxIdManager.CombineRowLocks(
                        db,
                        row.LockTxId, *singleLockMode,
                        lockId, lockMode,
                        globalTxId);

                    if (!newMultiTxId) {
                        Y_ENSURE(!globalTxId);
                        needGlobalTxId = true;
                        applyLocks();
                        return ETxLockRows::CommitAsync;
                    }

                    MultiTxIdManager.DecrementLockedRowsCount(db, row.LockTxId);
                    setLockMode(NTable::ELockMode::Multi, newMultiTxId);
                    finishLocked();
                    continue;
                }

                if (currentOwner) {
                    // For multiLock currentOwner points to the first conflict (which we are not supposed to have)
                    Y_ENSURE(!multiLock);
                    Y_ENSURE(currentOwner->GetLockId() != lockId);
                    Y_ENSURE(IsCompatibleRowLockMode(currentLockMode, lockMode));

                    ui64 newMultiTxId = MultiTxIdManager.CombineRowLocks(
                        db,
                        currentOwner->GetLockId(), currentLockMode,
                        lockId, lockMode,
                        globalTxId);

                    if (!newMultiTxId) {
                        Y_ENSURE(!globalTxId);
                        needGlobalTxId = true;
                        applyLocks();
                        return ETxLockRows::CommitAsync;
                    }

                    setLockMode(NTable::ELockMode::Multi, newMultiTxId);
                    finishLocked();
                    continue;
                }

                if (multiLock) {
                    // We are going to overwrite this row lock below
                    MultiTxIdManager.DecrementLockedRowsCount(db, row.LockTxId);
                }

                setLockMode(lockMode, lockId);
                finishLocked();
            }

            applyLocks();

            for (const auto& lock : takenLocks) {
                success->AddLock(lock.LockId, lock.DataShard, lock.Generation, lock.Counter,
                                lock.SchemeShard, lock.PathId, lock.HasWrites);
                if (lock.IsError()) {
                    success->Record.SetStatus(NKikimrDataEvents::TEvLockRowsResult::STATUS_LOCKS_BROKEN);
                }
            }
            state.Result = std::move(success);
            return ETxLockRows::CommitSync;
        });

        if (state.Result) {
            break;
        }

        if (reschedule) {
            // Note: reschedule transaction without waiting for anything
            continue;
        }

        if (needGlobalTxId) {
            Y_ENSURE(!globalTxId);
            globalTxId = co_await AllocateGlobalTxId();
            Y_ENSURE(globalTxId);
            needGlobalTxId = false;
        }

        if (runtimeLock.IsValid()) {
            while (!runtimeLock.IsOwner()) {
                // Notify long tx service about waiting for the predecessor
                TLockRowsNotifyWaitGuard waitGuard(*this, state, lock, runtimeLock.Predecessor().GetLock());
                // Wait until lock predecessor changes
                co_await runtimeLock.OnChangedEvent.Wait([&]{
                    setWaiting(true);
                });
            }

            if (waitForLock) {
                // Wait until current row's persistent owner is broken or removed
                if (!waitForLock->IsBroken()) {
                    // Notify long tx service abour waiting for the predecessor
                    TLockRowsNotifyWaitGuard waitGuard(*this, state, lock, waitForLock);
                    // Wake up when current row owner is broken (removing it also breaks)
                    co_await waitForLock->OnBrokenEvent.Wait([&]{
                        setWaiting(true);
                    });
                }
                waitForLock.Reset();
            }
        }

        setWaiting(false);

        // Cycle in the low-priority queue before doing anything expensive
        co_await LowPriorityQueue.Next();
    }

    Send(ev->Sender, state.Result.release(), 0, ev->Cookie);
}

void TDataShard::HandleLockRowsCancel(NEvents::TDataEvents::TEvLockRowsCancel::TPtr& ev) {
    auto* msg = ev->Get();

    TLockRowsRequestId requestId(ev->Sender, msg->Record.GetRequestId());
    auto it = LockRowsRequests.find(requestId);
    if (it != LockRowsRequests.end()) {
        it->second.Scope.Cancel();
        // Note: awaiter queue size may change when Cancel is called
        UpdateProposeQueueSize();
    }
}

void TDataShard::HandleLockRowsDeadlock(TEvLongTxService::TEvWaitingLockDeadlock::TPtr& ev) {
    auto* msg = ev->Get();

    auto it = LockRowsWaitRequests.find(msg->RequestId);
    if (it != LockRowsWaitRequests.end()) {
        TLockRowsRequestState& state = *it->second;
        if (!state.Result) {
            state.Result = std::make_unique<NEvents::TDataEvents::TEvLockRowsResult>(
                TabletID(), state.RequestId.RequestId,
                NKikimrDataEvents::TEvLockRowsResult::STATUS_DEADLOCK,
                TStringBuilder() << "Deadlock with another transaction detected at shard " << TabletID());
            state.Scope.Cancel();
            // Note: awaiter queue size may change when Cancel is called
            UpdateProposeQueueSize();
        }
    }
}

void TDataShard::StartLockRowsBrokenWatcher(TLockRowsRequestId requestId, TTableId tableId, TLockInfo::TPtr lock) {
    // Note: this watcher uses unstructured concurrency to watch when an
    // acquired lock breaks while the request is waiting. This function starts
    // recursively while inside the transaction, so we can be sure the request
    // is still valid and lock is not broken yet. However the request may exit
    // at any time, invalidating the state.
    TLockRowsRequestState* state = LockRowsRequests.FindPtr(requestId);
    Y_ENSURE(state, "TEvLockRows request state must be wait in StartLockRowsBrokenWatcher");
    Y_ENSURE(!lock->IsBroken(), "Unexpected broken lock in StartLockRowsBrokenWatcher");
    // We attach to the same scope, which will cancel the wait along with the
    // request. The return value will be true when the callback returns
    // normally, i.e. when the lock becomes broken or is removed.
    bool ok = co_await state->Scope.Wrap([&]() -> async<void> {
        co_await lock->OnBrokenEvent.Wait();
    });
    if (ok && lock->IsBroken()) {
        // Even when this coroutine is resumed because the lock becomes broken,
        // the request may have been destroyed concurrently. Make sure we don't
        // try working with the missing request.
        state = LockRowsRequests.FindPtr(requestId);
        if (state && !state->Result) {
            state->Result = std::make_unique<NEvents::TDataEvents::TEvLockRowsResult>(
                TabletID(), requestId.RequestId,
                NKikimrDataEvents::TEvLockRowsResult::STATUS_LOCKS_BROKEN,
                TStringBuilder() << "Lock " << lock->GetLockId() << " is broken at shard " << TabletID());
            state->Result->AddLock(
                lock->GetLockId(),
                TabletID(),
                lock->GetGeneration(),
                lock->GetCounter(),
                tableId.PathId.OwnerId,
                tableId.PathId.LocalPathId,
                lock->IsWriteLock());
            state->Scope.Cancel();
            // Note: awaiter queue size may change when Cancel is called
            UpdateProposeQueueSize();
        }
    }
}

} // namespace NKikimr::NDataShard
