#include "schemeshard__operation_memory_changes.h"

#include "schemeshard_impl.h"

#include <type_traits>

namespace NKikimr::NSchemeShard {

void TMemoryChanges::Arm(TSchemeShard*) {
    Y_ABORT_UNLESS(!Armed);
    Armed = true;
}

void TMemoryChanges::Disarm() {
    Armed = false;
}

namespace {

template <class V>
void GrabNewReferenceEntry(TMemoryChanges& changes, TDbRefMap<V>& map, const TPathId& pathId) {
    Y_ABORT_UNLESS(changes.IsArmed());
    Y_ABORT_UNLESS(!map.contains(pathId));
    Y_ABORT_UNLESS(changes.IsPathTracked(pathId));
    changes.RecordUndo([&map, pathId]() {
        map.RestoreMembershipWithoutRefcount(pathId, {});
    });
}

template <class T>
void GrabReferenceEntry(TMemoryChanges& changes, TDbRefMap<TIntrusivePtr<T>>& map, const TPathId& pathId) {
    Y_ABORT_UNLESS(changes.IsArmed());
    auto original = map.Update(pathId);
    auto snapshot = [&]() {
        if constexpr (std::is_same_v<T, TTableInfo>) {
            return TTableInfo::DeepCopy(*original);
        } else {
            return MakeIntrusive<T>(*original);
        }
    }();
    changes.RecordUndo([&map, pathId, original, snapshot]() {
        if constexpr (std::is_same_v<T, TTableInfo>) {
            original->RestoreSnapshot(*snapshot);
        } else {
            *original = *snapshot;
        }
        map.RestoreMembershipWithoutRefcount(pathId, original);
    });
}

} // namespace

template <typename I, typename C, typename H>
static void GrabNew(const I& id, const C& cont, H& holder) {
    Y_ABORT_UNLESS(!cont.contains(id));
    holder.emplace(id, nullptr);
}

template <typename T, typename I, typename C, typename H>
static void Grab(const I& id, const C& cont, H& holder) {
    Y_ABORT_UNLESS(cont.contains(id));
    holder.emplace(id, new T(*cont.at(id)));
}

void TMemoryChanges::GrabNewTable(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->Tables, pathId);
}

void TMemoryChanges::GrabTable(TSchemeShard* ss, const TPathId& pathId) {
    GrabReferenceEntry(*this, ss->Tables, pathId);
}

void TMemoryChanges::GrabNewIndex(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->Indexes, pathId);
}

void TMemoryChanges::GrabIndex(TSchemeShard* ss, const TPathId& pathId) {
    GrabReferenceEntry(*this, ss->Indexes, pathId);
}

void TMemoryChanges::GrabNewSequence(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->Sequences, pathId);
}

void TMemoryChanges::GrabSequence(TSchemeShard* ss, const TPathId& pathId) {
    GrabReferenceEntry(*this, ss->Sequences, pathId);
}

void TMemoryChanges::GrabNewCdcStream(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->CdcStreams, pathId);
}

void TMemoryChanges::GrabCdcStream(TSchemeShard* ss, const TPathId& pathId) {
    GrabReferenceEntry(*this, ss->CdcStreams, pathId);
}

void TMemoryChanges::GrabNewReplication(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->Replications, pathId);
}

void TMemoryChanges::GrabNewBlobDepot(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->BlobDepots, pathId);
}

void TMemoryChanges::GrabNewTopic(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->Topics, pathId);
}

void TMemoryChanges::GrabNewRtmrVolume(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->RtmrVolumes, pathId);
}

void TMemoryChanges::GrabNewSolomonVolume(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->SolomonVolumes, pathId);
}

void TMemoryChanges::GrabNewBlockStoreVolume(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->BlockStoreVolumes, pathId);
}

void TMemoryChanges::GrabNewFileStoreInfo(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->FileStoreInfos, pathId);
}

void TMemoryChanges::GrabNewKesusInfo(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->KesusInfos, pathId);
}

void TMemoryChanges::GrabNewOlapStore(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->OlapStores, pathId);
}

void TMemoryChanges::GrabNewExternalTable(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->ExternalTables, pathId);
}

void TMemoryChanges::GrabExternalTable(TSchemeShard* ss, const TPathId& pathId) {
    GrabReferenceEntry(*this, ss->ExternalTables, pathId);
}

void TMemoryChanges::GrabNewExternalDataSource(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->ExternalDataSources, pathId);
}

void TMemoryChanges::GrabExternalDataSource(TSchemeShard* ss, const TPathId& pathId) {
    GrabReferenceEntry(*this, ss->ExternalDataSources, pathId);
}

void TMemoryChanges::GrabNewView(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->Views, pathId);
}

void TMemoryChanges::GrabView(TSchemeShard* ss, const TPathId& pathId) {
    GrabReferenceEntry(*this, ss->Views, pathId);
}

void TMemoryChanges::GrabNewResourcePool(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->ResourcePools, pathId);
}

void TMemoryChanges::GrabResourcePool(TSchemeShard* ss, const TPathId& pathId) {
    GrabReferenceEntry(*this, ss->ResourcePools, pathId);
}

void TMemoryChanges::GrabNewBackupCollection(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->BackupCollections, pathId);
}

void TMemoryChanges::GrabBackupCollection(TSchemeShard* ss, const TPathId& pathId) {
    GrabReferenceEntry(*this, ss->BackupCollections, pathId);
}

void TMemoryChanges::GrabNewSysView(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->SysViews, pathId);
}

void TMemoryChanges::GrabSysView(TSchemeShard* ss, const TPathId& pathId) {
    GrabReferenceEntry(*this, ss->SysViews, pathId);
}

void TMemoryChanges::GrabNewSecret(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->Secrets, pathId);
}

void TMemoryChanges::GrabSecret(TSchemeShard* ss, const TPathId& pathId) {
    GrabReferenceEntry(*this, ss->Secrets, pathId);
}

void TMemoryChanges::GrabNewStreamingQuery(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->StreamingQueries, pathId);
}

void TMemoryChanges::GrabStreamingQuery(TSchemeShard* ss, const TPathId& pathId) {
    GrabReferenceEntry(*this, ss->StreamingQueries, pathId);
}

void TMemoryChanges::GrabNewTestShardSet(TSchemeShard* ss, const TPathId& pathId) {
    GrabNewReferenceEntry(*this, ss->TestShardSets, pathId);
}

void TMemoryChanges::GrabTestShardSet(TSchemeShard* ss, const TPathId& pathId) {
    GrabReferenceEntry(*this, ss->TestShardSets, pathId);
}

void TMemoryChanges::GrabNewTxState(TSchemeShard* ss, const TOperationId& opId) {
    GrabNew(opId, ss->TxInFlight, TxStates);
}

void TMemoryChanges::GrabNewPath(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->PathsById, Paths);
}

void TMemoryChanges::GrabPath(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TPathElement>(pathId, ss->PathsById, Paths);
}

void TMemoryChanges::GrabNewColumnTable(TSchemeShard* ss, const TPathId& pathId) {
    Y_ABORT_UNLESS(!ss->ColumnTables.contains(pathId));
    ColumnTables.emplace(pathId, nullptr);
}

void TMemoryChanges::GrabColumnTable(TSchemeShard* ss, const TPathId& pathId) {
    Y_ABORT_UNLESS(ss->ColumnTables.contains(pathId));
    ColumnTables.emplace(pathId, std::make_shared<TColumnTableInfo>(*ss->ColumnTables.GetVerified(pathId)));
}

void TMemoryChanges::GrabNewShard(TSchemeShard*, const TShardIdx& shardId) {
    Shards.emplace(shardId, nullptr);
}

void TMemoryChanges::GrabShard(TSchemeShard *ss, const TShardIdx &shardId) {
    Y_ABORT_UNLESS(ss->ShardInfos.contains(shardId));

    const auto& shard = ss->ShardInfos.at(shardId);
    Shards.emplace(shardId, MakeHolder<TShardInfo>(shard));
}

void TMemoryChanges::GrabDomain(TSchemeShard* ss, const TPathId& pathId) {
    // Copy TSubDomainInfo from ss->SubDomains to local SubDomains.
    // Make sure that copy will be made only when needed.
    const auto found = ss->SubDomains.find(pathId);
    Y_ABORT_UNLESS(found != ss->SubDomains.end());
    if (!SubDomains.contains(pathId)) {
        SubDomains.emplace(pathId, MakeIntrusive<TSubDomainInfo>(*found->second));
    }
}

void TMemoryChanges::GrabNewTableSnapshot(TSchemeShard* ss, const TPathId& pathId, TTxId snapshotTxId) {
    Y_ABORT_UNLESS(!ss->TablesWithSnapshots.contains(pathId));
    TablesWithSnapshots.emplace(pathId, snapshotTxId);
}

void TMemoryChanges::GrabNewLongLock(TSchemeShard* ss, const TPathId& pathId) {
    Y_ABORT_UNLESS(!ss->LockedPaths.contains(pathId));
    LockedPaths.emplace(pathId, InvalidTxId); // will be removed on UnDo()
}

void TMemoryChanges::GrabLongLock(TSchemeShard* ss, const TPathId& pathId, TTxId lockTxId) {
    Y_ABORT_UNLESS(ss->LockedPaths.contains(pathId));
    Y_ABORT_UNLESS(ss->LockedPaths.at(pathId) == lockTxId);
    LockedPaths.emplace(pathId, lockTxId); // will be restored on UnDo()
}

void TMemoryChanges::GrabNewLongIncrementalRestoreOp(TSchemeShard* ss, const TOperationId& opId) {
    Y_ABORT_UNLESS(!ss->LongIncrementalRestoreOps.contains(opId));
    LongIncrementalRestoreOps.emplace(opId, std::nullopt);
}

void TMemoryChanges::GrabLongIncrementalRestoreOp(TSchemeShard* ss, const TOperationId& opId) {
    Y_ABORT_UNLESS(ss->LongIncrementalRestoreOps.contains(opId));
    LongIncrementalRestoreOps.emplace(opId, ss->LongIncrementalRestoreOps.at(opId));
}

void TMemoryChanges::GrabNewLongIncrementalBackupOp(TSchemeShard* ss, ui64 id) {
    Y_ABORT_UNLESS(!ss->IncrementalBackups.contains(id));
    IncrementalBackups.emplace(id, nullptr);
}

void TMemoryChanges::GrabNewFullBackupOp(TSchemeShard* ss, ui64 id) {
    Y_ABORT_UNLESS(!ss->FullBackups.contains(id));
    FullBackups.emplace(id, nullptr);
}

void TMemoryChanges::GrabNewBCPathToFullBackup(TSchemeShard* ss, const TPathId& bcPathId) {
    Y_ABORT_UNLESS(!ss->BCPathToFullBackup.contains(bcPathId));
    BCPathToFullBackup.emplace(bcPathId, std::nullopt);
}

void TMemoryChanges::GrabNewSharedShard(TSchemeShard* ss, const TShardIdx& shardIdx, const TPathId& pathId) {
    auto shardIt = ss->SharedShards.find(shardIdx);
    if (shardIt != ss->SharedShards.end()) {
        Y_ABORT_UNLESS(!shardIt->second.contains(pathId));
    }
    SharedShardEntries.emplace(shardIdx, pathId, std::nullopt);
}

void TMemoryChanges::GrabSharedShard(TSchemeShard* ss, const TShardIdx& shardIdx, const TPathId& pathId) {
    auto shardIt = ss->SharedShards.find(shardIdx);
    Y_ABORT_UNLESS(shardIt != ss->SharedShards.end());
    auto pathIt = shardIt->second.find(pathId);
    Y_ABORT_UNLESS(pathIt != shardIt->second.end());
    SharedShardEntries.emplace(shardIdx, pathId, pathIt->second);
}

void TMemoryChanges::UnDo(TSchemeShard* ss) {
    // be aware of the order of grab & undo ops
    // stack is the best way to manage it right

    while (Paths) {
        const auto& [id, elem] = Paths.top();
        if (elem) {
            ss->PathsById[id] = elem;
        } else {
            // Paths snapshots own the counter rollback: the parent ref dies with
            // the erased element; OwnDbRef is dropped disarmed, not released.
            if (auto refIt = ss->OwnDbRefs.find(id); refIt != ss->OwnDbRefs.end()) {
                refIt->second.DetachWithoutRelease();
                ss->OwnDbRefs.erase(refIt);
            }
            ss->PathsById.erase(id);
        }
        Paths.pop();
    }

    // Restore membership and explicitly recorded fields in reverse mutation order.
    // Paths above already restored the counters, so membership undo does not release.
    while (UndoActions) {
        UndoActions.top()();
        UndoActions.pop();
    }

    while (TablesWithSnapshots) {
        const auto& [id, snapshotTxId] = TablesWithSnapshots.top();

        ss->TablesWithSnapshots.erase(id);
        auto it = ss->SnapshotTables.find(snapshotTxId);
        if (it != ss->SnapshotTables.end()) {
            it->second.erase(id);
            if (it->second.empty()) {
                ss->SnapshotTables.erase(it);
            }
        }

        TablesWithSnapshots.pop();
    }

    while (LockedPaths) {
        const auto& [id, lockTxId] = LockedPaths.top();
        if (lockTxId != InvalidTxId) {
            ss->LockedPaths[id] = lockTxId;
        } else {
            ss->LockedPaths.erase(id);
        }
        LockedPaths.pop();
    }

    while (ColumnTables) {
        const auto& [id, elem] = ColumnTables.top();
        // Drop current entry first (if any), then re-create with the saved value (if any)
        ss->ColumnTables.Drop(id);
        if (elem) {
            ss->ColumnTables.BuildNew(id, elem);
        }
        ColumnTables.pop();
    }

    while (Shards) {
        const auto& [id, elem] = Shards.top();
        if (elem) {
            ss->ShardInfos[id] = *elem;
        } else {
            ss->ShardInfos.erase(id);
            ss->OnShardRemoved(id);
        }
        Shards.pop();
    }

    // Restore ss->SubDomains entries to saved copies of TSubDomainInfo objects.
    // No copy, simple pointer replacement.
    for (const auto& [id, savedState] : SubDomains) {
        ss->SubDomains.RestoreMembershipWithoutRefcount(id, savedState);
        const auto& subdomain = ss->SubDomains.Update(id);
        if (ss->GetCurrentSubDomainPathId() == id) {
            subdomain->UpdateCounters(ss);
        }
    }
    SubDomains.clear();

    while (TxStates) {
        const auto& [id, elem] = TxStates.top();
        if (!elem) {
            // counter rollback is owned by the Paths snapshots - disarm, do not release
            if (auto* txState = ss->TxInFlight.FindPtr(id)) {
                txState->DisarmPathRefs();
            }
            ss->TxInFlight.erase(id);
        } else {
            Y_ABORT("No such cases are exist");
        }
        TxStates.pop();
    }

    while (LongIncrementalRestoreOps) {
        const auto& [id, elem] = LongIncrementalRestoreOps.top();
        if (elem.has_value()) {
            ss->LongIncrementalRestoreOps[id] = elem.value();
        } else {
            ss->LongIncrementalRestoreOps.erase(id);
        }
        LongIncrementalRestoreOps.pop();
    }

    while (IncrementalBackups) {
        const auto& [id, elem] = IncrementalBackups.top();
        if (elem) {
            ss->IncrementalBackups[id] = elem;
        } else {
            ss->IncrementalBackups.erase(id);
        }
        IncrementalBackups.pop();
    }

    while (FullBackups) {
        const auto& [id, elem] = FullBackups.top();
        if (elem) {
            ss->FullBackups[id] = elem;
        } else {
            ss->FullBackups.erase(id);
        }
        FullBackups.pop();
    }

    while (BCPathToFullBackup) {
        const auto& [bcPathId, prevId] = BCPathToFullBackup.top();
        if (prevId.has_value()) {
            ss->BCPathToFullBackup[bcPathId] = *prevId;
        } else {
            ss->BCPathToFullBackup.erase(bcPathId);
        }
        BCPathToFullBackup.pop();
    }

    while (SharedShardEntries) {
        const auto& [shardIdx, pathId, elem] = SharedShardEntries.top();
        if (elem) {
            ss->SharedShards[shardIdx][pathId] = *elem;
        } else {
            auto shardIt = ss->SharedShards.find(shardIdx);
            if (shardIt != ss->SharedShards.end()) {
                shardIt->second.erase(pathId);
                if (shardIt->second.empty()) {
                    ss->SharedShards.erase(shardIt);
                }
            }
        }
        SharedShardEntries.pop();
    }

#ifndef NDEBUG
    ss->DebugCheckDbRefIntegrity();
#endif
}

}
