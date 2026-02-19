#include "datashard_impl.h"
#include "datashard_locks_db.h"
#include "setup_sys_locks.h"

#include <ydb/library/actors/async/continuation.h>

namespace NKikimr::NDataShard {

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

    switch (msg->Record.GetLockMode()) {
        case NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE:
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
                    takenLocks[0].Generation == res.first[0].Generation ||
                    takenLocks[0].Counter == res.first[0].Counter);
            }
            takenLocks = std::move(res.first);
            if (!ok) {
                takenLocks[0].Counter = TSysTables::TLocksTable::TLock::ErrorBroken;
            }
        }
        SubscribeNewLocks();
        return ok;
    };

    TRuntimeLockHolder runtimeLock;
    TVector<TRawTypeValue> typedKey;
    size_t processedKeys = 0;

    auto success = std::make_unique<NEvents::TDataEvents::TEvLockRowsResult>(
            TabletID(), requestId.RequestId, NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS);

    for (;;) {
        bool reschedule = false;
        std::optional<ui64> waitForLock;

        // We need to run each iteration in a transaction
        co_await TTxLockRows::Run(this, [&](TTransactionContext& txc) {
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

            TDataShardLocksDb locksDb(*this, txc);
            TSetupSysLocks guardLocks(lockId, lockNodeId, *this, &locksDb);

            // 1. Ensure lock exists or is allowed to be created
            switch (SysLocksTable().EnsureCurrentLock()) {
                case EEnsureCurrentLock::Success:
                    // Lock is valid, we may continue with reads and side-effects
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

                // Special case when this lock is already the key owner
                // We must not add ourselves to the wait queue in that case
                // Note: we don't try to lock the key out-of-order however
                if (row.LockTxId == lockId) {
                    if (row.LockMode != NTable::ELockMode::Exclusive) {
                        state.Result = makeError(NKikimrDataEvents::TEvLockRowsResult::STATUS_INTERNAL_ERROR, TStringBuilder()
                            << "Upgrading to exclusive locks is unsupported at shard " << TabletID());
                        return ETxLockRows::Rollback;
                    }

                    success->Record.AddLockedKeys(processedKeys);
                    if (modified) {
                        success->Record.AddModifiedKeys(processedKeys);
                    }
                    runtimeLock.Reset();
                    ++processedKeys;
                    continue;
                }

                // Key is either not locked, or locked by someone else
                // We need to establish a runtime lock for fair locking
                // TODO: we need to jump the queue when another lock from the
                // current transaction is waiting already, or owns the lock.
                // Otherwise transaction may indirectly start waiting on itself,
                // causing deadlocks.
                if (!runtimeLock.IsValid()) {
                    runtimeLock = SysLocksTable().AddRuntimeLock(tableId, key);
                    Y_ENSURE(runtimeLock.IsValid());
                    if (!runtimeLock.IsOwner()) {
                        if (skipLocked) {
                            success->Record.AddSkippedKeys(processedKeys);
                            runtimeLock.Reset();
                            ++processedKeys;
                            continue;
                        }

                        applyLocks();

                        // Wait until the runtime lock is acquired
                        return ETxLockRows::CommitAsync;
                    }
                }

                Y_ENSURE(runtimeLock.IsOwner());

                if (row.LockMode != NTable::ELockMode::None) {
                    auto currentOwner = SysLocksTable().GetRawLock(row.LockTxId);
                    if (currentOwner) {
                        if (skipLocked) {
                            success->Record.AddSkippedKeys(processedKeys);
                            runtimeLock.Reset();
                            ++processedKeys;
                            continue;
                        }

                        waitForLock = row.LockTxId;
                        applyLocks();
                        return ETxLockRows::CommitAsync;
                    }
                }

                typedKey.clear();
                for (size_t i = 0; i < columnIds.size(); ++i) {
                    if (key[i].IsNull()) {
                        typedKey.emplace_back();
                    } else {
                        auto typeId = tableInfo.Columns.at(columnIds[i]).PType.GetTypeId();
                        typedKey.emplace_back(key[i].Data(), key[i].Size(), typeId);
                    }
                }

                GetConflictsCache().GetTableCache(localTid).AddUncommittedWrite(key, lockId, txc.DB);
                txc.DB.LockRowTx(localTid, NTable::ELockMode::Exclusive, typedKey, lockId);
                SysLocksTable().SetWriteLock(tableId, key);
                advanced = true;

                success->Record.AddLockedKeys(processedKeys);
                if (modified) {
                    success->Record.AddModifiedKeys(processedKeys);
                }
                runtimeLock.Reset();
                ++processedKeys;
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
            Send(ev->Sender, state.Result.release(), 0, ev->Cookie);
            co_return;
        }

        if (reschedule) {
            // Note: reschedule transaction without waiting for anything
            continue;
        }

        if (runtimeLock.IsValid()) {
            struct TResumeCallback : public TLockCallback {
                TAsyncContinuation<void> Continuation;

                void Run() override {
                    if (Continuation) {
                        Continuation.Resume();
                    }
                }
            } resumeCallback;

            if (!runtimeLock.IsOwner()) {
                // Wait until runtime lock becomes the owner
                co_await WithAsyncContinuation<void>([&](auto continuation) {
                    // TODO: we need to somehow notify the service about new waiting graph edges,
                    // which must also take into account that earlier requests may be cancelled while
                    // we wait, and the previous runtime lock in the waiting list might change.
                    resumeCallback.Continuation = std::move(continuation);
                    runtimeLock.AddActivationCallback(resumeCallback);
                    setWaiting(true);
                });
                Y_ENSURE(runtimeLock.IsOwner());
            }

            if (waitForLock) {
                // Wait until current row's persistent owner is removed
                auto currentOwner = SysLocksTable().GetRawLock(*waitForLock);
                if (currentOwner) {
                    co_await WithAsyncContinuation<void>([&](auto continuation) {
                        // TODO: we need to somehow notify the service about new waiting graph edge
                        // between our lockId and *waitForLock id.
                        resumeCallback.Continuation = std::move(continuation);
                        currentOwner->AddOnRemovedCallback(resumeCallback);
                        setWaiting(true);
                    });
                }
                waitForLock.reset();
            }
        }

        setWaiting(false);

        // Cycle in the low-priority queue before doing anything expensive
        co_await LowPriorityQueue.Next();
    }
}

void TDataShard::HandleLockRowsCancel(NEvents::TDataEvents::TEvLockRowsCancel::TPtr& ev) {
    auto* msg = ev->Get();

    TLockRowsRequestId requestId(ev->Sender, msg->Record.GetRequestId());
    auto it = LockRowsRequests.find(requestId);
    if (it != LockRowsRequests.end()) {
        it->second.Scope.Cancel();
        // Note: awaiter queue size changes when Cancel is called
        UpdateProposeQueueSize();
    }
}

} // namespace NKikimr::NDataShard
