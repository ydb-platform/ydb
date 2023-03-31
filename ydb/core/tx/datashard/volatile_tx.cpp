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
            Y_VERIFY(Self->VolatileTxManager.PendingCommitTxScheduled);
            Self->VolatileTxManager.PendingCommitTxScheduled = false;

            Y_VERIFY(!Self->VolatileTxManager.PendingCommits.empty());
            TxId = Self->VolatileTxManager.PendingCommits.front();
            Self->VolatileTxManager.PendingCommits.pop_front();

            // Schedule another transaction if needed
            Self->VolatileTxManager.RunPendingCommitTx();

            auto* info = Self->VolatileTxManager.FindByTxId(TxId);
            Y_VERIFY(info && info->State == EVolatileTxState::Committed);

            for (auto& pr : Self->GetUserTables()) {
                auto tid = pr.second->LocalTid;
                for (ui64 commitTxId : info->CommitTxIds) {
                    if (txc.DB.HasOpenTx(tid, commitTxId)) {
                        txc.DB.CommitTx(tid, commitTxId, info->Version);
                    }
                }
            }

            // TODO: commit change records

            Self->VolatileTxManager.PersistRemoveVolatileTx(TxId, txc);

            if (info->AddCommitted) {
                OnCommitted(ctx);
            } else {
                Delayed = true;
            }

            return true;
        }

        void Complete(const TActorContext& ctx) override {
            if (Delayed) {
                OnCommitted(ctx);
            }
        }

        void OnCommitted(const TActorContext& ctx) {
            auto* info = Self->VolatileTxManager.FindByTxId(TxId);
            Y_VERIFY(info && info->State == EVolatileTxState::Committed);
            Y_VERIFY(info->AddCommitted);

            Self->VolatileTxManager.UnblockDependents(info);

            Self->VolatileTxManager.RemoveFromTxMap(info);

            Self->VolatileTxManager.RemoveVolatileTx(TxId);

            Self->CheckSplitCanStart(ctx);
        }

    private:
        ui64 TxId;
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
            Y_VERIFY(Self->VolatileTxManager.PendingAbortTxScheduled);
            Self->VolatileTxManager.PendingAbortTxScheduled = false;

            Y_VERIFY(!Self->VolatileTxManager.PendingAborts.empty());
            TxId = Self->VolatileTxManager.PendingAborts.front();
            Self->VolatileTxManager.PendingAborts.pop_front();

            // Schedule another transaction if needed
            Self->VolatileTxManager.RunPendingAbortTx();

            auto* info = Self->VolatileTxManager.FindByTxId(TxId);
            Y_VERIFY(info && info->State == EVolatileTxState::Aborting);

            for (auto& pr : Self->GetUserTables()) {
                auto tid = pr.second->LocalTid;
                for (ui64 commitTxId : info->CommitTxIds) {
                    if (txc.DB.HasOpenTx(tid, commitTxId)) {
                        txc.DB.RemoveTx(tid, commitTxId);
                    }
                }
            }

            // TODO: abort change records

            Self->VolatileTxManager.PersistRemoveVolatileTx(TxId, txc);
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            auto* info = Self->VolatileTxManager.FindByTxId(TxId);
            Y_VERIFY(info && info->State == EVolatileTxState::Aborting);
            Y_VERIFY(info->AddCommitted);

            // Run callbacks only after we successfully persist aborted tx
            Self->VolatileTxManager.RunAbortCallbacks(info);

            Self->VolatileTxManager.UnblockDependents(info);

            Self->VolatileTxManager.RemoveFromTxMap(info);

            Self->VolatileTxManager.RemoveVolatileTx(TxId);

            Self->CheckSplitCanStart(ctx);
        }

    private:
        ui64 TxId;
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
        TxMap.Reset();
    }

    bool TVolatileTxManager::Load(NIceDb::TNiceDb& db) {
        using Schema = TDataShard::Schema;

        Y_VERIFY(
            VolatileTxs.empty() &&
            VolatileTxByVersion.empty() &&
            VolatileTxByCommitTxId.empty() &&
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
                    PendingCommits.push_back(pr.first);
                    break;
                case EVolatileTxState::Aborting:
                    PendingAborts.push_back(pr.first);
                    Y_FAIL("FIXME: unexpected persistent aborting state");
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
            info->AddCommitted = true; // we loaded it from local db, so it is committed

            if (!rowset.Next()) {
                return false;
            }
        }

        auto postProcessTxInfo = [this](TVolatileTxInfo* info) {
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

                    return;
                }

                case EVolatileTxState::Aborting: {
                    // Aborting transactions don't have dependencies
                    info->Dependencies.clear();
                    Y_FAIL("FIXME: unexpected persistent aborting state");
                    return;
                }
            }

            Y_VERIFY_S(false, "Unexpected volatile txId# " << info->TxId << " @" << info->Version << " with state# " << ui32(info->State));
        };

        for (auto& pr : VolatileTxs) {
            postProcessTxInfo(pr.second.get());
            VolatileTxByVersion.insert(pr.second.get());
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
            TConstArrayRef<ui64> dependencies,
            TConstArrayRef<ui64> participants,
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
        info->Dependencies.insert(dependencies.begin(), dependencies.end());
        info->Participants.insert(participants.begin(), participants.end());

        if (info->Participants.empty()) {
            // Transaction is committed when we don't have to wait for other participants
            info->State = EVolatileTxState::Committed;
        }

        VolatileTxByVersion.insert(info);

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

        db.Table<Schema::TxVolatileDetails>().Key(info->TxId).Update(
            NIceDb::TUpdate<Schema::TxVolatileDetails::State>(info->State),
            NIceDb::TUpdate<Schema::TxVolatileDetails::Details>(std::move(details)));
        for (ui64 shardId : participants) {
            db.Table<Schema::TxVolatileParticipants>().Key(info->TxId, shardId).Update();
        }

        txc.OnCommitted([this, txId]() {
            auto* info = FindByTxId(txId);
            Y_VERIFY_S(info, "Unexpected failure to find volatile txId# " << txId);
            Y_VERIFY_S(!info->AddCommitted, "Unexpected commit of a committed volatile txId# " << txId);
            info->AddCommitted = true;
            if (info->State == EVolatileTxState::Committed) {
                RunCommitCallbacks(info);
            }
        });
        txc.OnRollback([txId]() {
            Y_VERIFY_S(false, "Unexpected rollback of volatile txId# " << txId);
        });

        if (info->State == EVolatileTxState::Committed && info->Dependencies.empty()) {
            AddPendingCommit(info->TxId);
        }
    }

    void TVolatileTxManager::PersistRemoveVolatileTx(ui64 txId, TTransactionContext& txc) {
        using Schema = TDataShard::Schema;

        auto* info = FindByTxId(txId);
        Y_VERIFY_S(info, "Unexpected failure to find volatile tx " << txId);

        NIceDb::TNiceDb db(txc.DB);

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

        UnblockWaitingRemovalOperations(info);

        for (ui64 commitTxId : info->CommitTxIds) {
            VolatileTxByCommitTxId.erase(commitTxId);
        }
        VolatileTxByVersion.erase(info);
        VolatileTxs.erase(txId);

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
        Y_VERIFY(callback, "Unexpected nullptr callback");

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
        Y_VERIFY(!VolatileTxByVersion.empty() && (*VolatileTxByVersion.begin())->Version <= snapshot);

        WaitingSnapshotEvents.emplace_back(snapshot, std::move(event));
        std::push_heap(WaitingSnapshotEvents.begin(), WaitingSnapshotEvents.end());
    }

    void TVolatileTxManager::AbortWaitingTransaction(TVolatileTxInfo* info) {
        Y_VERIFY(info && info->State == EVolatileTxState::Waiting);

        ui64 txId = info->TxId;

        // Move tx to aborting, but don't persist yet, we need a separate transaction for that
        info->State = EVolatileTxState::Aborting;

        // Aborted transactions don't have dependencies
        for (ui64 dependencyTxId : info->Dependencies) {
            auto* dependency = FindByTxId(dependencyTxId);
            Y_VERIFY(dependency);
            dependency->Dependents.erase(txId);
        }
        info->Dependencies.clear();

        // We will unblock operations when we persist the abort
        AddPendingAbort(txId);
    }

    void TVolatileTxManager::ProcessReadSet(
            const TEvTxProcessing::TEvReadSet& rs,
            TTransactionContext& txc)
    {
        using Schema = TDataShard::Schema;

        const auto& record = rs.Record;
        const ui64 txId = record.GetTxId();

        auto* info = FindByTxId(txId);
        Y_VERIFY(info, "ProcessReadSet called for an unknown volatile tx");

        if (info->State != EVolatileTxState::Waiting) {
            // Transaction is already decided
            return;
        }

        ui64 srcTabletId = record.GetTabletSource();
        ui64 dstTabletId = record.GetTabletDest();

        if (dstTabletId != Self->TabletID()) {
            LOG_WARN_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                "Unexpected readset from " << srcTabletId << " to " << dstTabletId << " at tablet " << Self->TabletID());
            return;
        }

        if (!info->Participants.contains(srcTabletId)) {
            // We are not waiting for readset from this participant
            return;
        }

        bool committed = [&]() {
            if (record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_NO_DATA) {
                Y_VERIFY(!(record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_EXPECT_READSET),
                    "Unexpected FLAG_EXPECT_READSET + FLAG_NO_DATA in ProcessReadSet");
                LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                    "Processed readset without data from " << srcTabletId << " to " << dstTabletId
                    << " at tablet " << Self->TabletID());
                return false;
            }

            NKikimrTx::TReadSetData data;
            bool ok = data.ParseFromString(record.GetReadSet());
            Y_VERIFY(ok, "Failed to parse readset from %" PRIu64 " to %" PRIu64, srcTabletId, dstTabletId);

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
            return;
        }

        info->Participants.erase(srcTabletId);

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::TxVolatileParticipants>().Key(txId, srcTabletId).Delete();
        Self->RemoveExpectation(srcTabletId, txId);

        if (info->Participants.empty()) {
            // Move tx to committed.
            // Note that we don't need to wait until the new state is committed (it's repeatable),
            // but we need to wait until the initial effects are committed and persisted.
            info->State = EVolatileTxState::Committed;
            db.Table<Schema::TxVolatileDetails>().Key(txId).Update(
                NIceDb::TUpdate<Schema::TxVolatileDetails::State>(info->State));
            // We may run callbacks immediately when effects are committed
            if (info->AddCommitted) {
                RunCommitCallbacks(info);
            }
            if (info->Dependencies.empty()) {
                AddPendingCommit(txId);
            }
        }
    }

    void TVolatileTxManager::RunCommitCallbacks(TVolatileTxInfo* info) {
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
                        Y_FAIL("FIXME: unexpected dependency removed from aborting tx");
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
            auto ctx = TActivationContext::ActorContextFor(Self->SelfId());
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
        PendingCommits.push_back(txId);
        RunPendingCommitTx();
    }

    void TVolatileTxManager::AddPendingAbort(ui64 txId) {
        PendingAborts.push_back(txId);
        RunPendingAbortTx();
    }

    void TVolatileTxManager::RunPendingCommitTx() {
        if (!PendingCommitTxScheduled && !PendingCommits.empty()) {
            PendingCommitTxScheduled = true;
            Self->Execute(new TDataShard::TTxVolatileTxCommit(Self));
        }
    }

    void TVolatileTxManager::RunPendingAbortTx() {
        if (!PendingAbortTxScheduled && !PendingAborts.empty()) {
            PendingAbortTxScheduled = true;
            Self->EnqueueExecute(new TDataShard::TTxVolatileTxAbort(Self));
        }
    }

} // namespace NKikimr::NDataShard
