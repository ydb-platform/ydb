#include "volatile_tx.h"
#include "datashard_impl.h"

namespace NKikimr::NDataShard {

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
        VolatileTxByCommitTxId.clear();
        TxMap.Reset();
    }

    bool TVolatileTxManager::Load(NIceDb::TNiceDb& db) {
        using Schema = TDataShard::Schema;

        Y_VERIFY(VolatileTxs.empty() && VolatileTxByCommitTxId.empty() && !TxMap,
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
                    // Note that aborted transactions are removed from TxMap and don't need to be re-added
                    for (ui64 commitTxId : info->CommitTxIds) {
                        auto res2 = VolatileTxByCommitTxId.emplace(commitTxId, info);
                        Y_VERIFY_S(res2.second, "Unexpected duplicate commitTxId# " << commitTxId);
                        TxMap->Add(commitTxId, info->Version);
                    }

                    for (auto it = info->Dependencies.begin(); it != info->Dependencies.end(); /* nothing */) {
                        ui64 dependencyTxId = *it;
                        auto* dependency = FindByTxId(dependencyTxId);
                        if (!dependency || dependency->State == EVolatileTxState::Aborted) {
                            // Skip dependencies that have been aborted already
                            info->Dependencies.erase(it++);
                            continue;
                        }
                        dependency->Dependents.insert(info->TxId);
                        ++it;
                    }

                    if (info->Dependencies.empty() && info->State == EVolatileTxState::Committed) {
                        // TODO: committed transactions without dependencies are ready to fully commit
                    }

                    return;
                }

                case EVolatileTxState::Aborted: {
                    // Aborted transactions don't have dependencies
                    info->Dependencies.clear();

                    // TODO: aborted transactions are ready to be rolled back
                    return;
                }
            }

            Y_VERIFY_S(false, "Unexpected volatile txId# " << info->TxId << " @" << info->Version << " with state# " << ui32(info->State));
        };

        for (auto& pr : VolatileTxs) {
            postProcessTxInfo(pr.second.get());
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
            Y_VERIFY_S(dependency->State != EVolatileTxState::Aborted,
                "Unexpected aborted dependency txId# " << dependencyTxId
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

        txc.OnCommit([this, txId]() {
            auto* info = FindByTxId(txId);
            Y_VERIFY_S(info, "Unexpected failure to find volatile txId# " << txId);
            Y_VERIFY_S(!info->AddCommitted, "Unexpected commit of a committed volatile txId# " << txId);
            info->AddCommitted = true;
            // TODO: activate pending responses
        });
        txc.OnRollback([txId]() {
            Y_VERIFY_S(false, "Unexpected rollback of volatile txId# " << txId);
        });
    }

} // namespace NKikimr::NDataShard
