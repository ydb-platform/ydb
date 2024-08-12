#include "volatile_tx.h"
#include "datashard_impl.h"

namespace NKikimr::NDataShard {

    class TDataShard::TTxVolatileTxCommit
        : public NTabletFlatExecutor::TTransactionBase<TDataShard>
    {
    public:
        TTxVolatileTxCommit(TDataShard* self)
            : TBase(self)
        { }

        TTxType GetTxType() const override { return TXTYPE_VOLATILE_TX_COMMIT; }

        bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override {
            NIceDb::TNiceDb db(txc.DB);

            Y_ABORT_UNLESS(Self->VolatileTxManager.PendingCommitTxScheduled);
            Self->VolatileTxManager.PendingCommitTxScheduled = false;

            // We may have changed our mind
            if (Self->VolatileTxManager.PendingCommits.Empty()) {
                TxId = 0;
                return true;
            }

            auto* info = Self->VolatileTxManager.PendingCommits.PopFront();
            Y_ABORT_UNLESS(info && info->State == EVolatileTxState::Committed);
            TxId = info->TxId;

            // Schedule another transaction if needed
            Self->VolatileTxManager.RunPendingCommitTx();

            for (auto& pr : Self->GetUserTables()) {
                auto tid = pr.second->LocalTid;
                for (ui64 commitTxId : info->CommitTxIds) {
                    if (txc.DB.HasOpenTx(tid, commitTxId)) {
                        txc.DB.CommitTx(tid, commitTxId, info->Version);
                        Self->GetConflictsCache().GetTableCache(tid).RemoveUncommittedWrites(commitTxId, txc.DB);
                    }
                }
            }

            auto getGroup = [&]() -> ui64 {
                if (!info->ChangeGroup) {
                    if (info->Version.TxId != info->TxId) {
                        // Assume it's an immediate transaction and allocate new group
                        info->ChangeGroup = Self->AllocateChangeRecordGroup(db);
                    } else {
                        // Distributed transactions commit changes with group zero
                        info->ChangeGroup = 0;
                    }
                }
                return *info->ChangeGroup;
            };

            // First commit change records from any committed locks
            for (ui64 commitTxId : info->CommitTxIds) {
                if (commitTxId != info->TxId && Self->HasLockChangeRecords(commitTxId)) {
                    Self->CommitLockChangeRecords(db, commitTxId, getGroup(), info->Version, Collected);
                }
            }

            // Commit change records from the transaction itself
            if (info->CommitTxIds.contains(info->TxId) && Self->HasLockChangeRecords(info->TxId)) {
                Self->CommitLockChangeRecords(db, info->TxId, getGroup(), info->Version, Collected);
            }

            Self->VolatileTxManager.PersistRemoveVolatileTx(TxId, txc);

            Self->VolatileTxManager.RemoveFromCommitOrder(info);

            if (info->AddCommitted) {
                OnCommitted(ctx);
            } else {
                Delayed = true;
            }

            return true;
        }

        void Complete(const TActorContext& ctx) override {
            if (TxId == 0) {
                return;
            }

            if (Delayed) {
                OnCommitted(ctx);
            }

            if (Collected) {
                Self->EnqueueChangeRecords(std::move(Collected));
            }
        }

        void OnCommitted(const TActorContext& ctx) {
            auto* info = Self->VolatileTxManager.FindByTxId(TxId);
            Y_ABORT_UNLESS(info && info->State == EVolatileTxState::Committed);
            Y_ABORT_UNLESS(info->AddCommitted);

            Self->VolatileTxManager.UnblockDependents(info);

            Self->VolatileTxManager.RemoveFromTxMap(info);

            Self->VolatileTxManager.RemoveVolatileTx(TxId);

            Self->CheckSplitCanStart(ctx);
        }

    private:
        ui64 TxId;
        TVector<IDataShardChangeCollector::TChange> Collected;
        bool Delayed = false;
    };

    class TDataShard::TTxVolatileTxAbort
        : public NTabletFlatExecutor::TTransactionBase<TDataShard>
    {
    public:
        TTxVolatileTxAbort(TDataShard* self)
            : TBase(self)
        { }

        TTxType GetTxType() const override { return TXTYPE_VOLATILE_TX_ABORT; }

        bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext&) override {
            Y_ABORT_UNLESS(Self->VolatileTxManager.PendingAbortTxScheduled);
            Self->VolatileTxManager.PendingAbortTxScheduled = false;

            // We may have changed our mind
            if (Self->VolatileTxManager.PendingAborts.Empty()) {
                TxId = 0;
                return true;
            }

            auto* info = Self->VolatileTxManager.PendingAborts.PopFront();
            Y_ABORT_UNLESS(info && info->State == EVolatileTxState::Aborting);
            TxId = info->TxId;

            // Schedule another transaction if needed
            Self->VolatileTxManager.RunPendingAbortTx();

            for (auto& pr : Self->GetUserTables()) {
                auto tid = pr.second->LocalTid;
                for (ui64 commitTxId : info->CommitTxIds) {
                    if (txc.DB.HasOpenTx(tid, commitTxId)) {
                        txc.DB.RemoveTx(tid, commitTxId);
                        Self->GetConflictsCache().GetTableCache(tid).RemoveUncommittedWrites(commitTxId, txc.DB);
                    }
                }
            }

            if (!info->ArbiterReadSets.empty()) {
                NKikimrTx::TReadSetData data;
                data.SetDecision(NKikimrTx::TReadSetData::DECISION_ABORT);

                TString bodyStr;
                bool ok = data.SerializeToString(&bodyStr);
                Y_ABORT_UNLESS(ok, "Failed to serialize an abort decision readset");

                NIceDb::TNiceDb db(txc.DB);
                for (ui64 seqNo : info->ArbiterReadSets) {
                    auto rsInfo = Self->OutReadSets.ReplaceReadSet(db, seqNo, bodyStr);
                    if (Y_LIKELY(rsInfo.TxId == TxId)) {
                        auto msg = Self->PrepareReadSet(rsInfo.Step, rsInfo.TxId, rsInfo.From, rsInfo.To, bodyStr, seqNo);
                        ReadSets.push_back(std::move(msg));
                    }
                }
                info->ArbiterReadSets.clear();
            }

            Self->VolatileTxManager.PersistRemoveVolatileTx(TxId, txc);
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            if (TxId == 0) {
                return;
            }

            auto* info = Self->VolatileTxManager.FindByTxId(TxId);
            Y_ABORT_UNLESS(info && info->State == EVolatileTxState::Aborting);
            Y_ABORT_UNLESS(info->AddCommitted);

            for (auto& ev : info->DelayedAcks) {
                TActivationContext::Send(ev.Release());
            }
            info->DelayedAcks.clear();

            // Arbiter notifies other shards on abort
            if (!ReadSets.empty()) {
                Self->SendReadSets(ctx, std::move(ReadSets));
            }

            // Make a copy since it will disappear soon
            auto commitTxIds = info->CommitTxIds;

            // Run callbacks only after we successfully persist aborted tx
            Self->VolatileTxManager.RunAbortCallbacks(info);

            Self->VolatileTxManager.UnblockDependents(info);

            Self->VolatileTxManager.RemoveFromTxMap(info);

            Self->VolatileTxManager.RemoveVolatileTx(TxId);

            // Schedule removal of all lock changes we were supposed to commit
            for (ui64 commitTxId : commitTxIds) {
                Self->ScheduleRemoveLockChanges(commitTxId);
            }

            Self->CheckSplitCanStart(ctx);
        }

    private:
        ui64 TxId;
        TVector<THolder<TEvTxProcessing::TEvReadSet>> ReadSets;
    };

    void TVolatileTxManager::TTxMap::Add(ui64 txId, TRowVersion version) {
        Map[txId] = version;
    }

    void TVolatileTxManager::TTxMap::Remove(ui64 txId) {
        Map.erase(txId);
    }

    const TRowVersion* TVolatileTxManager::TTxMap::Find(ui64 txId) const {
        auto it = Map.find(txId);
        if (it != Map.end()) {
            return &it->second;
        }
        return nullptr;
    }

    void TVolatileTxManager::Clear() {
        VolatileTxs.clear();
        VolatileTxByVersion.clear();
        VolatileTxByCommitTxId.clear();
        VolatileTxByCommitOrder.Clear();
        TxMap.Reset();
        NextCommitOrder = 1;
    }

    bool TVolatileTxManager::Load(NIceDb::TNiceDb& db) {
        using Schema = TDataShard::Schema;

        Y_ABORT_UNLESS(
            VolatileTxs.empty() &&
            VolatileTxByVersion.empty() &&
            VolatileTxByCommitTxId.empty() &&
            VolatileTxByCommitOrder.Empty() &&
            !TxMap,
            "Unexpected Load into non-empty volatile tx manager");

        // Tables may not exist in some inactive shards, which cannot have transactions
        if (db.HaveTable<Schema::TxVolatileDetails>() &&
            db.HaveTable<Schema::TxVolatileParticipants>())
        {
            if (!LoadTxDetails(db)) {
                return false;
            }
            if (!LoadTxParticipants(db)) {
                return false;
            }
        }

        return true;
    }

    void TVolatileTxManager::Start(const TActorContext& ctx) {
        for (auto& pr : VolatileTxs) {
            if (!pr.second->Dependencies.empty()) {
                continue;
            }
            switch (pr.second->State) {
                case EVolatileTxState::Waiting:
                    for (ui64 target : pr.second->Participants) {
                        if (Self->AddExpectation(target, pr.second->Version.Step, pr.second->TxId)) {
                            Self->SendReadSetExpectation(ctx, pr.second->Version.Step, pr.second->TxId, Self->TabletID(), target);
                        }
                    }
                    break;
                case EVolatileTxState::Committed:
                    if (ReadyToDbCommit(pr.second.get())) {
                        PendingCommits.PushBack(pr.second.get());
                    }
                    break;
                case EVolatileTxState::Aborting:
                    PendingAborts.PushBack(pr.second.get());
                    Y_ABORT("FIXME: unexpected persistent aborting state");
                    break;
            }
        }

        RunPendingCommitTx();
        RunPendingAbortTx();
    }

    bool TVolatileTxManager::LoadTxDetails(NIceDb::TNiceDb& db) {
        using Schema = TDataShard::Schema;

        auto rowset = db.Table<Schema::TxVolatileDetails>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        ui64 maxCommitOrder = 0;

        while (!rowset.EndOfSet()) {
            ui64 txId = rowset.GetValue<Schema::TxVolatileDetails::TxId>();
            EVolatileTxState state = rowset.GetValue<Schema::TxVolatileDetails::State>();
            auto details = rowset.GetValue<Schema::TxVolatileDetails::Details>();

            Y_VERIFY_S(txId == details.GetTxId(),
                "Volatile txId# " << txId << " has unexpected details with txId# " << details.GetTxId());

            auto res = VolatileTxs.insert(
                std::make_pair(txId, std::make_unique<TVolatileTxInfo>()));
            Y_VERIFY_S(res.second, "Unexpected duplicate volatile txId# " << txId);

            auto* info = res.first->second.get();
            info->TxId = txId;
            info->State = state;
            info->Version = TRowVersion(details.GetVersionStep(), details.GetVersionTxId());
            info->CommitTxIds.insert(details.GetCommitTxIds().begin(), details.GetCommitTxIds().end());
            info->Dependencies.insert(details.GetDependencies().begin(), details.GetDependencies().end());
            if (details.HasChangeGroup()) {
                info->ChangeGroup = details.GetChangeGroup();
            }
            info->AddCommitted = true; // we loaded it from local db, so it is committed
            info->CommitOrder = details.GetCommitOrder();
            info->CommitOrdered = details.GetCommitOrdered();
            info->IsArbiter = details.GetIsArbiter();

            maxCommitOrder = Max(maxCommitOrder, info->CommitOrder);

            if (!rowset.Next()) {
                return false;
            }
        }

        NextCommitOrder = maxCommitOrder + 1;

        // Prepare and sort a vector later (intrusive list sorting isn't good enough)
        std::vector<TVolatileTxInfo*> byCommitOrder;
        byCommitOrder.reserve(VolatileTxs.size());

        auto postProcessTxInfo = [this, &byCommitOrder](TVolatileTxInfo* info) {
            switch (info->State) {
                case EVolatileTxState::Waiting:
                case EVolatileTxState::Committed: {
                    if (!TxMap) {
                        TxMap = MakeIntrusive<TTxMap>();
                    }

                    // Waiting and Committed transactions need to be added to TxMap until they are fully resolved
                    // Note that aborting transactions are deleted and we should never observe it as a persistent state
                    for (ui64 commitTxId : info->CommitTxIds) {
                        auto res2 = VolatileTxByCommitTxId.emplace(commitTxId, info);
                        Y_VERIFY_S(res2.second, "Unexpected duplicate commitTxId# " << commitTxId);
                        TxMap->Add(commitTxId, info->Version);
                    }

                    for (auto it = info->Dependencies.begin(); it != info->Dependencies.end(); /* nothing */) {
                        ui64 dependencyTxId = *it;
                        auto* dependency = FindByTxId(dependencyTxId);
                        if (!dependency) {
                            // Skip dependencies that have been removed already
                            info->Dependencies.erase(it++);
                            continue;
                        }
                        dependency->Dependents.insert(info->TxId);
                        ++it;
                    }

                    byCommitOrder.push_back(info);
                    return;
                }

                case EVolatileTxState::Aborting: {
                    // Aborting transactions don't have dependencies
                    info->Dependencies.clear();
                    Y_ABORT("FIXME: unexpected persistent aborting state");
                    return;
                }
            }

            Y_VERIFY_S(false, "Unexpected volatile txId# " << info->TxId << " @" << info->Version << " with state# " << ui32(info->State));
        };

        for (auto& pr : VolatileTxs) {
            postProcessTxInfo(pr.second.get());
            VolatileTxByVersion.insert(pr.second.get());
        }

        std::sort(byCommitOrder.begin(), byCommitOrder.end(), [](TVolatileTxInfo* a, TVolatileTxInfo* b) -> bool {
            return a->CommitOrder < b->CommitOrder;
        });
        for (TVolatileTxInfo* info : byCommitOrder) {
            VolatileTxByCommitOrder.PushBack(info);
        }

        return true;
    }

    bool TVolatileTxManager::LoadTxParticipants(NIceDb::TNiceDb& db) {
        using Schema = TDataShard::Schema;

        auto rowset = db.Table<Schema::TxVolatileParticipants>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        TVolatileTxInfo* lastInfo = nullptr;
        while (!rowset.EndOfSet()) {
            ui64 txId = rowset.GetValue<Schema::TxVolatileParticipants::TxId>();
            ui64 shardId = rowset.GetValue<Schema::TxVolatileParticipants::ShardId>();

            auto* info = (lastInfo && lastInfo->TxId == txId) ? lastInfo : FindByTxId(txId);
            Y_VERIFY_S(info, "Unexpected failure to find volatile txId# " << txId);

            // Only waiting transactions may have participants
            Y_VERIFY_S(info->State == EVolatileTxState::Waiting,
                "Unexpected volatile txId# " << txId << " with participant# " << shardId
                << " in state# " << ui32(info->State));

            info->Participants.insert(shardId);

            if (!rowset.Next()) {
                return false;
            }
        }

        for (auto& pr : VolatileTxs) {
            auto* info = pr.second.get();

            // Sanity check that are are no waiting transactions without participants
            if (info->State == EVolatileTxState::Waiting) {
                Y_VERIFY_S(!info->Participants.empty(),
                    "Unexpected waiting volatile txId# " << info->TxId << " without participants");
            }
        }

        return true;
    }

    TVolatileTxInfo* TVolatileTxManager::FindByTxId(ui64 txId) const {
        auto it = VolatileTxs.find(txId);
        if (it != VolatileTxs.end()) {
            return it->second.get();
        }
        return nullptr;
    }

    TVolatileTxInfo* TVolatileTxManager::FindByCommitTxId(ui64 txId) const {
        auto it = VolatileTxByCommitTxId.find(txId);
        if (it != VolatileTxByCommitTxId.end()) {
            return it->second;
        }
        return nullptr;
    }

    void TVolatileTxManager::PersistAddVolatileTx(
            ui64 txId, const TRowVersion& version,
            TConstArrayRef<ui64> commitTxIds,
            const absl::flat_hash_set<ui64>& dependencies,
            TConstArrayRef<ui64> participants,
            std::optional<ui64> changeGroup,
            bool commitOrdered,
            bool isArbiter,
            TTransactionContext& txc)
    {
        using Schema = TDataShard::Schema;

        Y_VERIFY_S(!commitTxIds.empty(),
            "Unexpected volatile txId# " << txId << " @" << version << " without commits");

        auto res = VolatileTxs.insert(
            std::make_pair(txId, std::make_unique<TVolatileTxInfo>()));
        Y_VERIFY_S(res.second, "Cannot add volatile txId# " << txId << " @" << version
            << ": duplicate volatile tx @" << res.first->second->Version << " already exists");

        auto* info = res.first->second.get();
        info->TxId = txId;
        info->Version = version;
        info->CommitTxIds.insert(commitTxIds.begin(), commitTxIds.end());
        info->Dependencies = dependencies;
        info->Participants.insert(participants.begin(), participants.end());
        info->ChangeGroup = changeGroup;
        info->CommitOrder = NextCommitOrder++;
        info->CommitOrdered = commitOrdered;
        info->IsArbiter = isArbiter;

        if (info->Participants.empty()) {
            // Transaction is committed when we don't have to wait for other participants
            info->State = EVolatileTxState::Committed;
        }

        VolatileTxByVersion.insert(info);
        VolatileTxByCommitOrder.PushBack(info);

        if (!TxMap) {
            TxMap = MakeIntrusive<TTxMap>();
        }

        for (ui64 commitTxId : commitTxIds) {
            auto res2 = VolatileTxByCommitTxId.emplace(commitTxId, info);
            Y_VERIFY_S(res2.second, "Cannot add volatile txId# " << txId << " @" << version << " with commitTxId# " << commitTxId
                << ": already registered for txId# " << res.first->second->TxId << " @" << res.first->second->Version);
            TxMap->Add(commitTxId, version);
        }

        for (ui64 dependencyTxId : info->Dependencies) {
            auto* dependency = FindByTxId(dependencyTxId);
            Y_VERIFY_S(dependency, "Cannot find dependency txId# " << dependencyTxId
                << " for volatile txId# " << txId << " @" << version);
            dependency->Dependents.insert(txId);
        }

        NIceDb::TNiceDb db(txc.DB);

        NKikimrTxDataShard::TTxVolatileDetails details;
        details.SetTxId(txId);
        details.SetVersionStep(version.Step);
        details.SetVersionTxId(version.TxId);

        if (!info->CommitTxIds.empty()) {
            auto* m = details.MutableCommitTxIds();
            m->Add(info->CommitTxIds.begin(), info->CommitTxIds.end());
            std::sort(m->begin(), m->end());
        }

        if (!info->Dependencies.empty()) {
            auto* m = details.MutableDependencies();
            m->Add(info->Dependencies.begin(), info->Dependencies.end());
            std::sort(m->begin(), m->end());
        }

        if (info->ChangeGroup) {
            details.SetChangeGroup(*info->ChangeGroup);
        }

        details.SetCommitOrder(info->CommitOrder);
        if (info->CommitOrdered) {
            details.SetCommitOrdered(true);
        }
        if (info->IsArbiter) {
            details.SetIsArbiter(true);
        }

        db.Table<Schema::TxVolatileDetails>().Key(info->TxId).Update(
            NIceDb::TUpdate<Schema::TxVolatileDetails::State>(info->State),
            NIceDb::TUpdate<Schema::TxVolatileDetails::Details>(std::move(details)));
        for (ui64 shardId : participants) {
            db.Table<Schema::TxVolatileParticipants>().Key(info->TxId, shardId).Update();
        }

        txc.DB.OnRollback([this, txId]() {
            RollbackAddVolatileTx(txId);
        });
        txc.DB.OnPersistent([this, txId]() {
            auto* info = FindByTxId(txId);
            Y_VERIFY_S(info, "Unexpected failure to find volatile txId# " << txId);
            Y_VERIFY_S(!info->AddCommitted, "Unexpected commit of a committed volatile txId# " << txId);
            info->AddCommitted = true;
            if (info->State == EVolatileTxState::Committed) {
                RunCommitCallbacks(info);
            }
        });

        if (ReadyToDbCommit(info)) {
            AddPendingCommit(info->TxId);
        }
    }

    void TVolatileTxManager::RollbackAddVolatileTx(ui64 txId) {
        auto* info = FindByTxId(txId);
        Y_VERIFY_S(info, "Rollback cannot find volatile txId# " << txId);

        // Unlink dependencies
        for (ui64 dependencyTxId : info->Dependencies) {
            if (auto* dependency = FindByTxId(dependencyTxId)) {
                dependency->Dependents.erase(txId);
            }
        }

        // Unlink commits
        for (ui64 commitTxId : info->CommitTxIds) {
            TxMap->Remove(commitTxId);
            VolatileTxByCommitTxId.erase(commitTxId);
        }

        VolatileTxByVersion.erase(info);

        // FIXME: do we need to handle WaitingSnapshotEvents somehow?

        // This will also unlink from linked lists
        VolatileTxs.erase(txId);
    }

    void TVolatileTxManager::PersistRemoveVolatileTx(ui64 txId, TTransactionContext& txc) {
        using Schema = TDataShard::Schema;

        auto* info = FindByTxId(txId);
        Y_VERIFY_S(info, "Unexpected failure to find volatile tx " << txId);

        NIceDb::TNiceDb db(txc.DB);

        for (ui64 shardId : info->DelayedConfirmations) {
            db.Table<Schema::TxVolatileParticipants>().Key(info->TxId, shardId).Delete();
        }
        for (ui64 shardId : info->Participants) {
            db.Table<Schema::TxVolatileParticipants>().Key(info->TxId, shardId).Delete();
            Self->RemoveExpectation(shardId, info->TxId);
        }
        db.Table<Schema::TxVolatileDetails>().Key(info->TxId).Delete();
    }

    void TVolatileTxManager::RemoveVolatileTx(ui64 txId) {
        auto* info = FindByTxId(txId);
        Y_VERIFY_S(info, "Unexpected failure to find volatile tx " << txId);

        Y_VERIFY_S(info->Dependencies.empty(), "Unexpected remove of volatile tx " << txId << " with dependencies");
        Y_VERIFY_S(info->Dependents.empty(), "Unexpected remove of volatile tx " << txId << " with dependents");
        Y_VERIFY_S(!info->IsInList<TVolatileTxInfoCommitOrderListTag>(),
            "Unexpected remove of volatile tx " << txId << " which is in commit order linked list");

        UnblockWaitingRemovalOperations(info);

        TRowVersion prevUncertain = GetMinUncertainVersion();

        for (ui64 commitTxId : info->CommitTxIds) {
            VolatileTxByCommitTxId.erase(commitTxId);
        }
        VolatileTxByVersion.erase(info);
        VolatileTxs.erase(txId);

        if (prevUncertain < GetMinUncertainVersion()) {
            Self->PromoteFollowerReadEdge();
        }

        Self->EmitHeartbeats();

        if (!WaitingSnapshotEvents.empty()) {
            TVolatileTxInfo* next = !VolatileTxByVersion.empty() ? *VolatileTxByVersion.begin() : nullptr;
            while (!WaitingSnapshotEvents.empty()) {
                auto& top = WaitingSnapshotEvents.front();
                if (next && next->Version <= top.Snapshot) {
                    // Still waiting
                    break;
                }
                TActivationContext::Send(std::move(top.Event));
                std::pop_heap(WaitingSnapshotEvents.begin(), WaitingSnapshotEvents.end());
                WaitingSnapshotEvents.pop_back();
            }
        }
    }

    bool TVolatileTxManager::AttachVolatileTxCallback(ui64 txId, IVolatileTxCallback::TPtr callback) {
        Y_ABORT_UNLESS(callback, "Unexpected nullptr callback");

        auto it = VolatileTxs.find(txId);
        if (it == VolatileTxs.end()) {
            return false;
        }

        switch (it->second->State) {
            case EVolatileTxState::Waiting:
                it->second->Callbacks.push_back(std::move(callback));
                break;

            case EVolatileTxState::Committed:
                // We call commit callbacks only when effects are committed
                if (it->second->AddCommitted) {
                    callback->OnCommit(txId);
                } else {
                    it->second->Callbacks.push_back(std::move(callback));
                }
                break;

            case EVolatileTxState::Aborting:
                // The rollback transaction will handle callbacks
                it->second->Callbacks.push_back(std::move(callback));
                break;
        }

        return true;
    }

    bool TVolatileTxManager::AttachBlockedOperation(ui64 txId, ui64 dependentTxId) {
        auto it = VolatileTxs.find(txId);
        if (it == VolatileTxs.end()) {
            return false;
        }

        switch (it->second->State) {
            case EVolatileTxState::Waiting:
            case EVolatileTxState::Aborting:
                it->second->BlockedOperations.insert(dependentTxId);
                return true;

            case EVolatileTxState::Committed:
                break;
        }

        return false;
    }

    bool TVolatileTxManager::AttachWaitingRemovalOperation(ui64 txId, ui64 dependentTxId) {
        auto it = VolatileTxs.find(txId);
        if (it == VolatileTxs.end()) {
            return false;
        }

        it->second->WaitingRemovalOperations.insert(dependentTxId);
        return true;
    }

    void TVolatileTxManager::AttachWaitingSnapshotEvent(const TRowVersion& snapshot, std::unique_ptr<IEventHandle>&& event) {
        Y_ABORT_UNLESS(!VolatileTxByVersion.empty() && (*VolatileTxByVersion.begin())->Version <= snapshot);

        WaitingSnapshotEvents.emplace_back(snapshot, std::move(event));
        std::push_heap(WaitingSnapshotEvents.begin(), WaitingSnapshotEvents.end());
    }

    void TVolatileTxManager::AbortWaitingTransaction(TVolatileTxInfo* info) {
        Y_ABORT_UNLESS(info && info->State == EVolatileTxState::Waiting);

        ui64 txId = info->TxId;

        // Move tx to aborting, but don't persist yet, we need a separate transaction for that
        info->State = EVolatileTxState::Aborting;

        // Aborted transactions don't have dependencies
        for (ui64 dependencyTxId : info->Dependencies) {
            auto* dependency = FindByTxId(dependencyTxId);
            Y_ABORT_UNLESS(dependency);
            dependency->Dependents.erase(txId);
        }
        info->Dependencies.clear();

        // We will unblock operations when we persist the abort
        AddPendingAbort(txId);

        // Note that abort is always enqueued, never executed immediately,
        // so it is safe to use info in this call.
        RemoveFromCommitOrder(info);
    }

    bool TVolatileTxManager::ProcessReadSet(
            const TEvTxProcessing::TEvReadSet& rs,
            THolder<IEventHandle>&& ack,
            TTransactionContext& txc)
    {
        using Schema = TDataShard::Schema;

        const auto& record = rs.Record;
        const ui64 txId = record.GetTxId();

        auto* info = FindByTxId(txId);
        Y_ABORT_UNLESS(info, "ProcessReadSet called for an unknown volatile tx");

        switch (info->State) {
            case EVolatileTxState::Waiting:
                break;

            case EVolatileTxState::Committed:
                // We may ack normally, since committed state is persistent
                return true;

            case EVolatileTxState::Aborting:
                // Aborting state will not change as long as we're still leader
                return true;
                // Ack readset normally as long as we're still a leader
                return true;
        }

        ui64 srcTabletId = record.GetTabletSource();
        ui64 dstTabletId = record.GetTabletDest();

        if (dstTabletId != Self->TabletID()) {
            LOG_WARN_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                "Unexpected readset from " << srcTabletId << " to " << dstTabletId << " at tablet " << Self->TabletID());
            return true;
        }

        if (!info->Participants.contains(srcTabletId)) {
            // We are not waiting for readset from this participant
            if (info->DelayedConfirmations.contains(srcTabletId)) {
                // Confirmation is delayed, delay this new ack as well
                info->DelayedAcks.push_back(std::move(ack));
                return false;
            }
            return true;
        }

        bool committed = [&]() {
            if (record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_NO_DATA) {
                Y_ABORT_UNLESS(!(record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_EXPECT_READSET),
                    "Unexpected FLAG_EXPECT_READSET + FLAG_NO_DATA in ProcessReadSet");
                LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                    "Processed readset without data from " << srcTabletId << " to " << dstTabletId
                    << " at tablet " << Self->TabletID());
                return false;
            }

            NKikimrTx::TReadSetData data;
            bool ok = data.ParseFromString(record.GetReadSet());
            Y_ABORT_UNLESS(ok, "Failed to parse readset from %" PRIu64 " to %" PRIu64, srcTabletId, dstTabletId);

            if (data.GetDecision() != NKikimrTx::TReadSetData::DECISION_COMMIT) {
                LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                    "Processed readset with decision " << ui32(data.GetDecision()) << " from "
                    << srcTabletId << " to " << dstTabletId << " at tablet " << Self->TabletID());
                return false;
            }

            if (record.GetStep() != info->Version.Step) {
                LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                    "Processed readset from " << srcTabletId << " to " << dstTabletId
                    << " with step " << record.GetStep() << " expecting " << info->Version.Step
                    << ", treating like abort due to divergence at tablet " << Self->TabletID());
                return false;
            }

            return true;
        }();

        if (!committed) {
            AbortWaitingTransaction(info);
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        info->Participants.erase(srcTabletId);
        info->DelayedAcks.push_back(std::move(ack));
        info->DelayedConfirmations.insert(srcTabletId);

        Self->RemoveExpectation(srcTabletId, txId);

        if (info->Participants.empty()) {
            // Move tx to committed.
            // Note that we don't need to wait until the new state is committed (it's repeatable),
            // but we need to wait until the initial effects are committed and persisted.
            info->State = EVolatileTxState::Committed;
            db.Table<Schema::TxVolatileDetails>().Key(txId).Update(
                NIceDb::TUpdate<Schema::TxVolatileDetails::State>(info->State));

            // Remove all delayed confirmations, since this tx is already writing
            for (ui64 shardId : info->DelayedConfirmations) {
                db.Table<Schema::TxVolatileParticipants>().Key(txId, shardId).Delete();
            }
            info->DelayedConfirmations.clear();

            // Send delayed acks when changes are persisted
            // TODO: maybe move it into a parameter?
            struct TDelayedAcksState : public TThrRefBase {
                TVector<THolder<IEventHandle>> DelayedAcks;

                TDelayedAcksState(TVolatileTxInfo* info)
                    : DelayedAcks(std::move(info->DelayedAcks))
                {}
            };
            txc.DB.OnPersistent([state = MakeIntrusive<TDelayedAcksState>(info)]() {
                for (auto& ev : state->DelayedAcks) {
                    TActivationContext::Send(ev.Release());
                }
            });
            info->DelayedAcks.clear();

            // We may run callbacks immediately when effects are committed
            if (info->AddCommitted) {
                RunCommitCallbacks(info);
            }
            if (info->Dependencies.empty() && ReadyToDbCommit(info)) {
                AddPendingCommit(txId);
            }
        }

        return false;
    }

    void TVolatileTxManager::RunCommitCallbacks(TVolatileTxInfo* info) {
        if (info->IsArbiterOnHold && !info->ArbiterReadSets.empty()) {
            Self->OutReadSets.ReleaseOnHoldReadSets(info->ArbiterReadSets,
                TActivationContext::ActorContextFor(Self->SelfId()));
            info->ArbiterReadSets.clear();
        }

        auto callbacks = std::move(info->Callbacks);
        info->Callbacks.clear();
        for (auto& callback : callbacks) {
            callback->OnCommit(info->TxId);
        }
        UnblockOperations(info, true);
    }

    void TVolatileTxManager::RunAbortCallbacks(TVolatileTxInfo* info) {
        auto callbacks = std::move(info->Callbacks);
        info->Callbacks.clear();
        for (auto& callback : callbacks) {
            callback->OnAbort(info->TxId);
        }
        UnblockOperations(info, false);
    }

    void TVolatileTxManager::RemoveFromTxMap(TVolatileTxInfo* info) {
        if (TxMap) {
            for (ui64 commitTxId : info->CommitTxIds) {
                TxMap->Remove(commitTxId);
            }
        }
    }

    void TVolatileTxManager::UnblockDependents(TVolatileTxInfo* info) {
        for (ui64 dependentTxId : info->Dependents) {
            auto* dependent = FindByTxId(dependentTxId);
            Y_VERIFY_S(dependent, "Unexpected failure to find dependent tx "
                << dependentTxId << " that depended on " << info->TxId);
            dependent->Dependencies.erase(info->TxId);
            if (dependent->Dependencies.empty()) {
                switch (dependent->State) {
                    case EVolatileTxState::Waiting:
                        break;
                    case EVolatileTxState::Committed:
                        AddPendingCommit(dependentTxId);
                        break;
                    case EVolatileTxState::Aborting:
                        Y_ABORT("FIXME: unexpected dependency removed from aborting tx");
                        break;
                }
            }
        }
        info->Dependents.clear();
    }

    void TVolatileTxManager::UnblockOperations(TVolatileTxInfo* info, bool success) {
        bool added = false;
        for (ui64 dependentTxId : info->BlockedOperations) {
            // Note: operation may have been cancelled, it's ok when missing
            if (auto op = Self->Pipeline.FindOp(dependentTxId)) {
                op->RemoveVolatileDependency(info->TxId, success);
                if (!op->HasVolatileDependencies() && !op->HasRuntimeConflicts()) {
                    Self->Pipeline.AddCandidateOp(op);
                    added = true;
                }
            }
        }
        info->BlockedOperations.clear();

        if (added && Self->Pipeline.CanRunAnotherOp()) {
            auto ctx = Self->ActorContext();
            Self->PlanQueue.Progress(ctx);
        }
    }

    void TVolatileTxManager::UnblockWaitingRemovalOperations(TVolatileTxInfo* info) {
        bool added = false;
        for (ui64 dependentTxId : info->WaitingRemovalOperations) {
            if (auto op = Self->Pipeline.FindOp(dependentTxId)) {
                op->RemoveVolatileDependency(info->TxId, info->State == EVolatileTxState::Committed);
                if (!op->HasVolatileDependencies() && !op->HasRuntimeConflicts()) {
                    Self->Pipeline.AddCandidateOp(op);
                    added = true;
                }
            }
        }
        info->WaitingRemovalOperations.clear();

        if (added && Self->Pipeline.CanRunAnotherOp()) {
            auto ctx = TActivationContext::ActorContextFor(Self->SelfId());
            Self->PlanQueue.Progress(ctx);
        }
    }

    void TVolatileTxManager::AddPendingCommit(ui64 txId) {
        if (auto* info = FindByTxId(txId)) {
            PendingCommits.PushBack(info);
            RunPendingCommitTx();
        }
    }

    void TVolatileTxManager::AddPendingAbort(ui64 txId) {
        if (auto* info = FindByTxId(txId)) {
            PendingAborts.PushBack(info);
            RunPendingAbortTx();
        }
    }

    void TVolatileTxManager::RunPendingCommitTx() {
        if (!PendingCommitTxScheduled && !PendingCommits.Empty()) {
            PendingCommitTxScheduled = true;
            Self->Execute(new TDataShard::TTxVolatileTxCommit(Self));
        }
    }

    void TVolatileTxManager::RunPendingAbortTx() {
        if (!PendingAbortTxScheduled && !PendingAborts.Empty()) {
            PendingAbortTxScheduled = true;
            Self->EnqueueExecute(new TDataShard::TTxVolatileTxAbort(Self));
        }
    }

    void TVolatileTxManager::RemoveFromCommitOrder(TVolatileTxInfo* info) {
        Y_ABORT_UNLESS(info->IsInList<TVolatileTxInfoCommitOrderListTag>(),
            "Volatile transaction is not in a commit order linked list");
        Y_ABORT_UNLESS(!VolatileTxByCommitOrder.Empty(), "Commit order linked list is unexpectedly empty");
        const bool wasFirst = VolatileTxByCommitOrder.Front() == info;
        info->UnlinkFromList<TVolatileTxInfoCommitOrderListTag>();
        if (wasFirst && !VolatileTxByCommitOrder.Empty()) {
            auto* next = VolatileTxByCommitOrder.Front();
            if (next->CommitOrdered && ReadyToDbCommit(next)) {
                AddPendingCommit(next->TxId);
            }
        }
    }

    bool TVolatileTxManager::ReadyToDbCommit(TVolatileTxInfo* info) const {
        if (info->State == EVolatileTxState::Committed && info->Dependencies.empty()) {
            if (info->CommitOrdered) {
                Y_DEBUG_ABORT_UNLESS(!VolatileTxByCommitOrder.Empty());
                return VolatileTxByCommitOrder.Front() == info;
            }

            return true;
        }

        return false;
    }

} // namespace NKikimr::NDataShard
