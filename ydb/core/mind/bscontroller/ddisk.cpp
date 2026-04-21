#include "impl.h"
#include "group_layout_checker.h"

namespace NKikimr::NBsController {

    class TBlobStorageController::TTxAllocateDDiskBlockGroup : public TTransactionBase<TBlobStorageController> {
        std::unique_ptr<TEventHandle<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>> RequestEv;
        std::unique_ptr<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult> Result;
        bool CompatReply = false;

        struct TExError : std::exception, TStringBuilder {
            const char *what() const noexcept override { return TString::c_str(); }
        };

        class TStoragePoolRecord {
            using TEntityId = NLayoutChecker::TEntityId;
            using TCommonId = std::tuple<TEntityId, TEntityId>; // value that all DDisks of a group have the same
            using TDistinctId = TEntityId;

            THashMap<TNodeId, std::tuple<TCommonId, TDistinctId>> NodeMap; // NodeId -> (CommonId, DistinctId)
            THashMap<TCommonId, THashMap<TDistinctId, std::vector<TNodeId>>> Trie; // CommonId -> DistinctId -> NodeIds
            std::map<TDDiskId, std::tuple<ui32, ui32>> ClaimPerDDisk;
            std::set<std::tuple<ui32, TDDiskId>> DDiskPerClaim;

            THashMap<TNodeId, std::set<std::tuple<ui32, TDDiskId>>> PersistentBuffersPerNode;

            struct TNodeClaim {
                struct TPDiskClaim {
                    ui32 Count;
                    THashMap<TDDiskId, ui32> DDisks;
                };
                ui32 Count;
                THashMap<TPDiskId, TPDiskClaim> PDisks;
            };
            THashMap<TNodeId, TNodeClaim> NodeClaims;

        public:
            TStoragePoolRecord(TBlobStorageController *self, TBoxStoragePoolId poolId, const TStoragePoolInfo& pool) {
                NLayoutChecker::TDomainMapper mapper;
                TGroupGeometryInfo geom(pool.ErasureSpecies, pool.GetGroupGeometry());

                for (const auto& [vslotId, vslot] : self->VSlots) {
                    if (vslot->Group && vslot->Group->StoragePoolId == poolId) {
                        const TNodeId nodeId = vslotId.NodeId;
                        if (!NodeMap.contains(nodeId)) {
                            const auto& location = self->HostRecords->GetLocation(nodeId);
                            const NLayoutChecker::TPDiskLayoutPosition pos(mapper, location, vslotId.ComprisingPDiskId(), geom);
                            const TCommonId commonId(pos.RealmGroup, pos.Realm);
                            const TDistinctId distinctId(pos.Domain);

                            NodeMap.try_emplace(nodeId, commonId, distinctId);
                            Trie[commonId][distinctId].push_back(nodeId);
                        }
                        const TDDiskId ddiskId = vslotId.GetKey();
                        ClaimPerDDisk.try_emplace(ddiskId, vslot->DDiskNumVChunksClaimed, Max<ui32>());
                        DDiskPerClaim.emplace(vslot->DDiskNumVChunksClaimed, ddiskId);
                        PersistentBuffersPerNode[nodeId].insert({vslot->PersistentBufferRefs, ddiskId});
                        NodeClaims[nodeId].Count = 0;
                        NodeClaims[nodeId].PDisks[vslotId.ComprisingPDiskId()].Count = 0;
                        NodeClaims[nodeId].PDisks[vslotId.ComprisingPDiskId()].DDisks.emplace(ddiskId, 0);
                    }
                }
            }

            bool AllocateDDisk(std::vector<TDDiskId>& group, ui32 numChunks) {
                std::optional<TCommonId> commonId;
                THashSet<TDistinctId> distinctIds;
                ParseGroup(group, commonId, distinctIds);
                TDDiskId bestDDiskId;
                std::tuple<ui32, ui32, ui32> bestChoice = {Max<ui32>(), Max<ui32>(), Max<ui32>()};
                auto bestIt = DDiskPerClaim.begin();
                // TODO(alexvru): optimize semi-linear search
                for (auto it = DDiskPerClaim.begin(); it != DDiskPerClaim.end(); ++it) {
                    auto& [_, candidateDDiskId] = *it;

                    const auto jt = NodeMap.find(candidateDDiskId.NodeId);
                    Y_ABORT_UNLESS(jt != NodeMap.end());
                    const auto& [diskCommonId, diskDistinctId] = jt->second;

                    if (commonId.value_or(diskCommonId) == diskCommonId && !distinctIds.contains(diskDistinctId)) {
                        Y_DEBUG_ABORT_UNLESS(ClaimPerDDisk.contains(candidateDDiskId));
                        auto& [chunksClaimed, chunksMax] = ClaimPerDDisk[candidateDDiskId];
                        if (numChunks > chunksMax - chunksClaimed) {
                            continue; // not enough capacity to claim here
                        }
                        auto pdiskId = candidateDDiskId.ComprisingPDiskId();
                        std::tuple<ui32, ui32, ui32> choice = {
                            NodeClaims[candidateDDiskId.NodeId].Count,
                            NodeClaims[candidateDDiskId.NodeId].PDisks[pdiskId].Count,
                            NodeClaims[candidateDDiskId.NodeId].PDisks[pdiskId].DDisks[candidateDDiskId],
                        };
                        if (choice < bestChoice) {
                            bestChoice = choice;
                            bestDDiskId = candidateDDiskId;
                            bestIt = it;
                        }
                        if (std::get<0>(bestChoice) == 0 && std::get<1>(bestChoice) == 0 && std::get<2>(bestChoice) == 0) {
                            break;
                        }
                    }
                }

                if (std::get<0>(bestChoice) != Max<ui32>()) {
                    auto& [chunksClaimed, chunksMax] = ClaimPerDDisk[bestDDiskId];
                    chunksClaimed += numChunks;
                    auto nh = DDiskPerClaim.extract(bestIt);
                    auto& [revChunksClaimed, _] = nh.value();
                    revChunksClaimed += numChunks;
                    DDiskPerClaim.insert(std::move(nh));
                    group.push_back(bestDDiskId);
                    auto pdiskId = bestDDiskId.ComprisingPDiskId();
                    NodeClaims[bestDDiskId.NodeId].Count++;
                    NodeClaims[bestDDiskId.NodeId].PDisks[pdiskId].Count++;
                    NodeClaims[bestDDiskId.NodeId].PDisks[pdiskId].DDisks[bestDDiskId]++;
                    return true;
                }

                return false;
            }

            void ReleaseDDisk(TDDiskId ddiskId, ui32 numChunks) {
                UpdateDDisk(ddiskId, numChunks, 0);
            }

            void UpdateDDisk(TDDiskId ddiskId, ui32 currentNumChunks, ui32 newNumChunks) {
                auto it = ClaimPerDDisk.find(ddiskId);
                if (it == ClaimPerDDisk.end()) {
                    throw TExError() << "DDiskId# " << ddiskId.ToString() << " not found";
                }
                auto& [chunksClaimed, chunksMax] = it->second;
                auto nh = DDiskPerClaim.extract({chunksClaimed, it->first});
                auto& [revChunksClaimed, _] = nh.value();

                if (currentNumChunks < newNumChunks) {
                    const ui32 incr = newNumChunks - currentNumChunks;
                    if (incr > chunksMax - chunksClaimed) {
                        throw TExError() << "not enough capacity for DDiskId# " << ddiskId.ToString();
                    }
                    chunksClaimed += incr;
                    revChunksClaimed += incr;
                } else if (newNumChunks < currentNumChunks) {
                    const ui32 decr = currentNumChunks - newNumChunks;
                    Y_ABORT_UNLESS(decr <= chunksClaimed);
                    chunksClaimed -= decr;
                    revChunksClaimed -= decr;
                }

                DDiskPerClaim.insert(std::move(nh));
            }
            TDDiskId GetPreferredPersistentBuffer(ui32 node) {
                return std::get<1>(*PersistentBuffersPerNode[TNodeId(node)].begin());
            }

            bool AllocatePersistentBuffer(std::vector<TDDiskId>& group, const THashSet<TDDiskId>& ddisks, const std::vector<TNodeId>& preferredNodes) {
                std::optional<TCommonId> commonId;
                THashSet<TDistinctId> distinctIds;
                ParseGroup(group, commonId, distinctIds);
                auto tryNode = [&](TNodeId nodeId) -> std::optional<TDDiskId> {
                    const auto it = PersistentBuffersPerNode.find(nodeId);
                    if (it == PersistentBuffersPerNode.end()) {
                        return std::nullopt;
                    }

                    const auto jt = NodeMap.find(nodeId);
                    Y_ABORT_UNLESS(jt != NodeMap.end());
                    const auto& [diskCommonId, diskDistinctId] = jt->second;
                    if (commonId.value_or(diskCommonId) == diskCommonId && !distinctIds.contains(diskDistinctId)) {
                        auto candidatePbIt = it->second.begin();
                        for (auto pbIt = it->second.begin(); pbIt != it->second.end(); pbIt++) {
                            if (!ddisks.contains(std::get<1>(*pbIt))) {
                                candidatePbIt = pbIt;
                                break;
                            }
                        }
                        auto nh = it->second.extract(candidatePbIt);
                        auto& [cnt, ddId] = nh.value();
                        auto ddiskId = ddId;
                        cnt++;
                        it->second.insert(std::move(nh));
                        return std::make_optional(ddiskId);
                    }
                    return std::nullopt;
                };
                for (TDDiskId ddId : ddisks) {
                    if (auto ddiskId = tryNode(ddId.NodeId)) {
                        group.push_back(*ddiskId);
                        return true;
                    }
                }
                for (TNodeId nodeId : preferredNodes) {
                    if (auto ddiskId = tryNode(nodeId)) {
                        group.push_back(*ddiskId);
                        return true;
                    }
                }
                for (const auto& [nodeId, _] : PersistentBuffersPerNode) {
                    if (auto ddiskId = tryNode(nodeId)) {
                        group.push_back(*ddiskId);
                        return true;
                    }
                }
                return false;
            }

            void ReleasePersistentBuffer(TDDiskId /*persistentBufferDDiskId*/) {
            }

        private:
            void ParseGroup(const std::vector<TDDiskId>& group, std::optional<TCommonId>& commonId, THashSet<TDistinctId>& distinctIds) {
                for (const TDDiskId& ddiskId : group) {
                    const auto it = NodeMap.find(ddiskId.NodeId);
                    if (it == NodeMap.end()) {
                        throw TExError() << "incorrect DDiskId# " << ddiskId.ToString() << ": no containing node found";
                    }
                    const auto& [diskCommonId, diskDistinctId] = it->second;

                    if (!commonId) {
                        commonId.emplace(diskCommonId);
                    } else if (*commonId != diskCommonId) {
                        throw TExError() << "DDisks do not share common prefix";
                    }
                    if (!distinctIds.insert(diskDistinctId).second) {
                        throw TExError() << "DDisks have repeating distinct infix";
                    }
                }
            }
        };

    public:
        TTxAllocateDDiskBlockGroup(TBlobStorageController *self,
                std::unique_ptr<TEventHandle<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>> requestEv)
            : TTransactionBase(self)
            , RequestEv(std::move(requestEv))
        {}

        TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_ALLOCATE_DDISK_BLOCK_GROUP; }

        void Transform(NKikimrBlobStorage::TEvControllerAllocateDDiskBlockGroup& record) {
            ui32 numDDisksInGroup = 0;
            ui32 numPersistentBuffersInGroup = 0;

            for (const auto& [poolId, pool] : Self->StoragePools) {
                if (record.HasDDiskPoolName() && pool.Name == record.GetDDiskPoolName()) {
                    numDDisksInGroup = pool.NumFailDomainsPerFailRealm.GetOrElse(0);
                }
                if (record.HasPersistentBufferDDiskPoolName() && pool.Name == record.GetPersistentBufferDDiskPoolName()) {
                    numPersistentBuffersInGroup = pool.NumFailDomainsPerFailRealm.GetOrElse(0);
                }
            }

            for (const auto& query : record.GetQueries()) {
                const ui64 directBlockGroupId = query.GetDirectBlockGroupId();
                const ui32 targetNumVChunks = query.GetTargetNumVChunks();
                auto *item = record.AddDirectBlockGroupOperations();
                item->SetDirectBlockGroupId(directBlockGroupId);
                auto *define = item->MutableDefineDirectBlockGroup();
                define->SetNumDDisks(numDDisksInGroup);
                define->SetNumChunksPerDDisk(targetNumVChunks);
                define->SetNumPersistentBuffers(numPersistentBuffersInGroup);
                CompatReply = true;
            }
        }

        TStoragePoolRecord ProcessStoragePool(const TString& ddiskPoolName) {
            THashSet<TNodeId> nodeIds;

            const TStoragePoolInfo *pool = nullptr;
            TBoxStoragePoolId poolId;

            for (const auto& [storagePoolId, storagePool] : Self->StoragePools) {
                if (storagePool.Name != ddiskPoolName) {
                    continue;
                }
                if (pool) {
                    throw TExError() << "ambigous pool name " << ddiskPoolName;
                }
                pool = &storagePool;
                poolId = storagePoolId;
            }
            if (!pool) {
                throw TExError() << "pool not found by name " << ddiskPoolName;
            }
            if (!pool->DDisk) {
                throw TExError() << "incorrect type for pool " << ddiskPoolName;
            }

            return {Self, poolId, *pool};
        }

        bool Prefetch(NIceDb::TNiceDb& db, ui64 tabletId, const auto& record) {
            bool ready = true;
            for (const auto& op : record.GetDirectBlockGroupOperations()) {
                const ui64 directBlockGroupId = op.GetDirectBlockGroupId();
                auto row = db.Table<Schema::DirectBlockGroupClaims>().Key(tabletId, directBlockGroupId).Select();
                ready &= row.IsReady();
            }
            return ready;
        }

        bool Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) override {
            auto& record = RequestEv->Get()->Record;

            if (record.QueriesSize() && record.DirectBlockGroupOperationsSize()) {
                Result = std::make_unique<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult>();
                Result->Record.SetStatus(NKikimrProto::ERROR);
                Result->Record.SetErrorReason("Queries and DirectBlockGroupOperations can't be provided at the same time");
                return true;
            }

            Transform(record);

            NIceDb::TNiceDb db(txc.DB);
            const ui64 tabletId = record.GetTabletId();
            using Table = Schema::DirectBlockGroupClaims;

            // prefetch direct block group ids we are going to work with
            if (!Prefetch(db, tabletId, record)) {
                return false;
            }

            Result = std::make_unique<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult>();
            auto& rr = Result->Record;
            rr.SetStatus(NKikimrProto::OK);

            std::vector<std::tuple<Table::TKey::Type, NKikimrBlobStorage::NDDisk::TDirectBlockGroupAllocation>> updates;
            THashMap<TVSlotId, std::tuple<i32, i32>> vslotUpdates;

            try {
                std::optional<TStoragePoolRecord> ddiskPool;
                auto getDDiskPool = [&]() -> TStoragePoolRecord& {
                    return ddiskPool
                        ? *ddiskPool
                        : ddiskPool.emplace(ProcessStoragePool(record.GetDDiskPoolName()));
                };

                std::optional<TStoragePoolRecord> persistentBufferPool;
                auto getPersistentBufferPool = [&]() -> TStoragePoolRecord& {
                    return persistentBufferPool
                        ? *persistentBufferPool
                        : persistentBufferPool.emplace(ProcessStoragePool(record.GetPersistentBufferDDiskPoolName()));
                };

                // process operations
                for (const auto& op : record.GetDirectBlockGroupOperations()) {
                    const ui64 directBlockGroupId = op.GetDirectBlockGroupId();
                    const Table::TKey::Type key(tabletId, directBlockGroupId);

                    auto row = db.Table<Table>().Key(key).Select();
                    Y_ABORT_UNLESS(row.IsReady());
                    NKikimrBlobStorage::NDDisk::TDirectBlockGroupAllocation allocation;
                    if (row.IsValid()) {
                        const bool success = allocation.ParseFromString(row.GetValue<Table::Allocation>());
                        Y_DEBUG_ABORT_UNLESS(success);
                        if (!success) {
                            throw TExError() << "failed to parse TDirectBlockGroupAllocation"
                                << " TabletId# " << tabletId
                                << " DirectBlockGroupId# " << directBlockGroupId;
                        }
                    }

                    auto *ddiskRecord = allocation.MutableDDiskRecord();
                    auto *persistentBufferDDiskId = allocation.MutablePersistentBufferDDiskId();
                    bool changes = false;

                    std::vector<TDDiskId> ddiskIds;
                    for (const auto& item : *ddiskRecord) {
                        if (item.HasDDiskId()) {
                            ddiskIds.emplace_back(item.GetDDiskId());
                        }
                    }

                    std::vector<TDDiskId> persistentBufferIds{
                        persistentBufferDDiskId->begin(),
                        persistentBufferDDiskId->end()
                    };

                    auto defineDirectBlockGroupDisk = [&](int index, ui32 numChunks) {
                        // create required empty items
                        while (ddiskRecord->size() <= index) {
                            ddiskRecord->Add();
                            changes = true;
                        }

                        auto *item = ddiskRecord->Mutable(index);
                        const ui32 currentNumChunks = item->GetNumChunksClaimed();
                        if (numChunks != currentNumChunks) {
                            item->SetNumChunksClaimed(numChunks);

                            std::optional<TDDiskId> ddiskId;
                            if (item->HasDDiskId()) {
                                ddiskId.emplace(item->GetDDiskId());
                            }

                            auto& pool = getDDiskPool();

                            if (!currentNumChunks && numChunks) { // allocate new DDisk
                                // allocate DDisk through allocator, serialize it to entity and update neighbour list
                                if (!pool.AllocateDDisk(ddiskIds, numChunks)) {
                                    throw TExError() << "can't allocate DDisk";
                                }
                                ddiskIds.back().Serialize(item->MutableDDiskId());
                                ddiskId.emplace(ddiskIds.back());
                            } else if (currentNumChunks && !numChunks) { // drop DDisk
                                // release DDisk from allocator
                                Y_ABORT_UNLESS(ddiskId);
                                pool.ReleaseDDisk(*ddiskId, currentNumChunks);

                                // remove DDisk from neighbour list
                                auto it = std::ranges::find(ddiskIds, *ddiskId);
                                Y_ABORT_UNLESS(it != ddiskIds.end());
                                std::swap(*it, ddiskIds.back());
                                ddiskIds.pop_back();

                                // clean up entity in item
                                item->ClearDDiskId();
                            } else {
                                Y_ABORT_UNLESS(ddiskId);
                                pool.UpdateDDisk(*ddiskId, currentNumChunks, numChunks);
                            }

                            // update claim on VDisk
                            Y_ABORT_UNLESS(ddiskId);
                            auto& [chunks, refs] = vslotUpdates[TVSlotId(ddiskId->GetKey())];
                            chunks += numChunks - currentNumChunks;

                            changes = true;
                        }
                    };

                    // define initial direct block group
                    if (op.HasDefineDirectBlockGroup()) {
                        const auto& cmd = op.GetDefineDirectBlockGroup();

                        int numDDisks = cmd.GetNumDDisks();
                        for (int i = 0; i < Max(numDDisks, ddiskRecord->size()); ++i) {
                            defineDirectBlockGroupDisk(i, i < numDDisks ? cmd.GetNumChunksPerDDisk() : 0);
                        }

                        int numPersistentBuffers = cmd.GetNumPersistentBuffers();
                        while (persistentBufferDDiskId->size() < numPersistentBuffers) { // create missing persistent buffers
                            THashSet<TDDiskId> ddisks;
                            if (persistentBufferDDiskId->size() < ddiskRecord->size()) {
                                const auto& rec = ddiskRecord->Get(persistentBufferDDiskId->size());
                                if (rec.HasDDiskId()) {
                                    ddisks.insert(rec.GetDDiskId());
                                }
                            }
                            if (!getPersistentBufferPool().AllocatePersistentBuffer(persistentBufferIds, ddisks, {})) {
                                throw TExError() << "failed to allocate persistent buffer";
                            }
                            persistentBufferIds.back().Serialize(persistentBufferDDiskId->Add());
                            auto& [chunks, refs] = vslotUpdates[persistentBufferIds.back().GetKey()];
                            ++refs;
                            changes = true;
                        }
                        while (numPersistentBuffers < persistentBufferDDiskId->size()) { // delete excessive ones
                            getPersistentBufferPool().ReleasePersistentBuffer(persistentBufferIds.back());
                            Y_ABORT_UNLESS(persistentBufferIds.back() == *persistentBufferDDiskId->rbegin());
                            auto& [chunks, refs] = vslotUpdates[persistentBufferIds.back().GetKey()];
                            --refs;
                            persistentBufferIds.pop_back();
                            persistentBufferDDiskId->RemoveLast();
                            changes = true;
                        }
                    }

                    for (const auto& cmd : op.GetDefineDirectBlockGroupDisks()) {
                        defineDirectBlockGroupDisk(cmd.GetDDiskIndex(), cmd.GetNumChunksPerDDisk());
                    }

                    for (const auto& cmd : op.GetReassignDirectBlockGroupDisks()) {
                        const ui32 from = cmd.GetDDiskIndexFrom();
                        const ui32 to = cmd.GetDDiskIndexTo();
                        defineDirectBlockGroupDisk(to, 0);
                        auto *itemFrom = ddiskRecord->Mutable(from);
                        ddiskRecord->Mutable(to)->CopyFrom(*itemFrom);
                        itemFrom->ClearDDiskId();
                        itemFrom->SetNumChunksClaimed(0);
                    }

                    for (const auto& cmd : op.GetReassignPersistentBuffers()) {
                        size_t index = cmd.GetPersistentBufferIndex();
                        if (persistentBufferIds.size() <= index) {
                            throw TExError() << "PersistentBufferIndex is out of bounds";
                        }
                        std::swap(persistentBufferIds[index], persistentBufferIds.back());
                        getPersistentBufferPool().ReleasePersistentBuffer(persistentBufferIds.back());
                        {
                            auto& [chunks, refs] = vslotUpdates[persistentBufferIds.back().GetKey()];
                            --refs;
                        }
                        persistentBufferIds.pop_back();
                        auto nodes = cmd.GetPreferredNodeIds();
                        if (!getPersistentBufferPool().AllocatePersistentBuffer(persistentBufferIds, {}, {nodes.begin(), nodes.end()})) {
                            throw TExError() << "failed to reallocate persistent buffer";
                        }
                        {
                            auto& [chunks, refs] = vslotUpdates[persistentBufferIds.back().GetKey()];
                            ++refs;
                        }
                        persistentBufferIds.back().Serialize(persistentBufferDDiskId->Mutable(index));
                        std::swap(persistentBufferIds[index], persistentBufferIds.back());
                        changes = true;
                    }

                    // trim excessive items
                    while (!ddiskRecord->empty() && ddiskRecord->rbegin()->GetNumChunksClaimed() == 0) {
                        ddiskRecord->RemoveLast();
                        changes = true;
                    }

                    if (CompatReply) {
                        auto *res = rr.AddResponses();
                        res->SetDirectBlockGroupId(directBlockGroupId);
                        for (const auto& item : *ddiskRecord) {
                            res->AddNodes()->MutableDDiskId()->CopyFrom(item.GetDDiskId());
                            res->SetActualNumVChunks(Max(res->GetActualNumVChunks(), item.GetNumChunksClaimed()));
                        }
                        for (int i = 0; i < persistentBufferDDiskId->size(); ++i) {
                            auto *node = i < (int)res->NodesSize()
                                ? res->MutableNodes(i)
                                : res->AddNodes();
                            node->MutablePersistentBufferDDiskId()->CopyFrom(persistentBufferDDiskId->Get(i));
                        }
                    } else {
                        auto *res = rr.AddDirectBlockGroups();
                        res->SetDirectBlockGroupId(directBlockGroupId);
                        for (const auto& item : *ddiskRecord) {
                            res->AddDDiskId()->CopyFrom(item.GetDDiskId());
                        }
                        for (const auto& item : *persistentBufferDDiskId) {
                            res->AddPersistentBufferDDiskId()->CopyFrom(item);
                        }
                    }

                    if (changes) {
                        updates.emplace_back(key, std::move(allocation));
                    }
                }
            } catch (std::exception& e) {
                rr.SetStatus(NKikimrProto::ERROR);
                rr.SetErrorReason(e.what());
                return true;
            }

            for (auto& [key, allocation] : updates) {
                TString s;
                const bool success = allocation.SerializeToString(&s);
                Y_ABORT_UNLESS(success);
                db.Table<Table>().Key(key).Update<Table::Allocation>(s);
            }

            for (auto& [vslotId, update] : vslotUpdates) {
                TVSlotInfo *vslot = Self->FindVSlot(vslotId);
                Y_DEBUG_ABORT_UNLESS(vslot);
                if (vslot) {
                    auto& [chunks, refs] = update;
                    vslot->DDiskNumVChunksClaimed += chunks;
                    vslot->PersistentBufferRefs += refs;
                    db.Table<Schema::VSlot>().Key(vslotId.GetKey()).Update<Schema::VSlot::DDiskNumVChunksClaimed,
                        Schema::VSlot::PersistentBufferRefs>(vslot->DDiskNumVChunksClaimed, vslot->PersistentBufferRefs);
                }
            }

            return true;
        }

        void Complete(const TActorContext& ctx) override {
            auto h = std::make_unique<IEventHandle>(RequestEv->Sender, ctx.SelfID, Result.release(), 0, RequestEv->Cookie);
            if (RequestEv->InterconnectSession) {
                h->Rewrite(TEvInterconnect::EvForward, RequestEv->InterconnectSession);
            }
            TActivationContext::Send(h.release());
        }
    };

    void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup::TPtr ev) {
        Execute(std::make_unique<TTxAllocateDDiskBlockGroup>(this,
            std::unique_ptr<TEventHandle<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>>(ev.Release())));
    }

} // NKikimr::NBsController
