#include "impl.h"

namespace NKikimr::NBsController {

    class TBlobStorageController::TTxAllocateDDiskBlockGroup : public TTransactionBase<TBlobStorageController> {
        std::unique_ptr<TEventHandle<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>> RequestEv;
        std::unique_ptr<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult> Result;
        THashMap<TString, std::multimap<ui32, TGroupId>> GroupRating;

    public:
        TTxAllocateDDiskBlockGroup(TBlobStorageController *self,
                std::unique_ptr<TEventHandle<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>> requestEv)
            : TTransactionBase(self)
            , RequestEv(std::move(requestEv))
        {}

        TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_ALLOCATE_DDISK_BLOCK_GROUP; }

        bool Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) override {
            const auto& record = RequestEv->Get()->Record;

            NIceDb::TNiceDb db(txc.DB);
            const ui64 tabletId = record.GetTabletId();
            const TString ddiskPoolName = record.GetDDiskPoolName();
            const std::optional<TString> persistentBufferDDiskPoolName = record.HasPersistentBufferDDiskPoolName()
                ? std::make_optional(record.GetPersistentBufferDDiskPoolName())
                : std::nullopt;

            // prepare claims
            auto claims = db.Table<Schema::DirectBlockGroupClaims>();
            std::deque<std::tuple<ui64, TString, std::optional<TString>, ui32, ui32, TString>> changes;
            for (const auto& item : record.GetQueries()) {
                const ui64 directBlockGroupId = item.GetDirectBlockGroupId();
                auto rows = claims.Key(tabletId, directBlockGroupId).Select();
                if (!rows.IsReady()) {
                    return false;
                } else {
                    const ui32 currentNumVChunks = rows.IsValid()
                        ? rows.GetValue<Schema::DirectBlockGroupClaims::NumVChunksClaimed>()
                        : 0;
                    const TString currentAllocation = rows.IsValid()
                        ? rows.GetValue<Schema::DirectBlockGroupClaims::Allocation>()
                        : TString();
                    changes.emplace_back(directBlockGroupId, ddiskPoolName, persistentBufferDDiskPoolName,
                        currentNumVChunks, item.GetTargetNumVChunks(), currentAllocation);
                }
            }

            // process claim updates and deletions
            Result = std::make_unique<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult>();
            auto& rr = Result->Record;
            rr.SetStatus(NKikimrProto::OK);

            PrepareForAllocation();

            for (auto& [directBlockGroupId, ddiskPoolName, persistentBufferDDiskPoolName, actualNumVChunks,
                    targetNumVChunks, allocation] : changes) {
                auto key = claims.Key(tabletId, directBlockGroupId);

                if (!targetNumVChunks) { // claim is going to be deleted
                    if (actualNumVChunks) {
                        key.Delete();
                    }
                } else if (actualNumVChunks != targetNumVChunks) {
                    if (!actualNumVChunks) { // allocate DDisks for data and for persistent buffers
                        NKikimrBlobStorage::TDirectBlockGroupAllocation pb;
                        std::vector<TDDiskId> ddiskIds;
                        if (AllocateDDiskGroup(ddiskPoolName, targetNumVChunks, &ddiskIds)) {
                            for (const auto& ddiskId : ddiskIds) {
                                ddiskId.Serialize(pb.AddDDiskId());
                                if (persistentBufferDDiskPoolName) {
                                    if (const auto bufferId = AllocatePeristentBuffer(*persistentBufferDDiskPoolName, ddiskId)) {
                                        bufferId->Serialize(pb.AddPersistentBufferDDiskId());
                                    } else { // failed to allocate persistent buffer
                                        targetNumVChunks = 0;
                                        break;
                                    }
                                }
                            }
                        } else { // failed to allocate DDisks for operation, report zero meaning error
                            targetNumVChunks = 0;
                        }
                        if (targetNumVChunks) {
                            const bool success = pb.SerializeToString(&allocation);
                            Y_ABORT_UNLESS(success);
                        }
                    } else { // check that reallocation is possible
                    }

                    ui32 incr = actualNumVChunks < targetNumVChunks ? targetNumVChunks - actualNumVChunks : 0;
                    ui32 decr = targetNumVChunks < actualNumVChunks ? actualNumVChunks - targetNumVChunks : 0;
                    actualNumVChunks = targetNumVChunks;

                    if (actualNumVChunks) {
                        using T = Schema::DirectBlockGroupClaims;
                        key.Update<T::NumVChunksClaimed, T::Allocation>(actualNumVChunks, allocation);

                        NKikimrBlobStorage::TDirectBlockGroupAllocation pb;
                        const bool success = pb.ParseFromString(allocation);
                        Y_ABORT_UNLESS(success);

                        for (const auto& ddiskId : pb.GetDDiskId()) {
                            auto *vslot = Self->FindVSlot(TVSlotId(ddiskId.GetNodeId(), ddiskId.GetPDiskId(),
                                ddiskId.GetDDiskSlotId()));
                            if (vslot) {
                                vslot->DDiskNumVChunksClaimed = vslot->DDiskNumVChunksClaimed + incr - decr;
                                db.Table<Schema::VSlot>().Key(ddiskId.GetNodeId(), ddiskId.GetPDiskId(),
                                    ddiskId.GetDDiskSlotId()).Update<Schema::VSlot::DDiskNumVChunksClaimed>(
                                    vslot->DDiskNumVChunksClaimed);
                            } else {
                                Y_DEBUG_ABORT();
                            }
                        }
                    }
                }

                auto *item = rr.AddResponses();
                item->SetDirectBlockGroupId(directBlockGroupId);
                item->SetActualNumVChunks(actualNumVChunks);
                if (allocation) {
                    NKikimrBlobStorage::TDirectBlockGroupAllocation pb;
                    const bool success = pb.ParseFromString(allocation);
                    Y_ABORT_UNLESS(success);
                    size_t numDDisks = pb.DDiskIdSize();
                    size_t numPersistentBuffers = pb.PersistentBufferDDiskIdSize();
                    for (size_t i = 0; i < Max(numDDisks, numPersistentBuffers); ++i) {
                        auto *node = item->AddNodes();
                        if (i < numDDisks) {
                            node->MutableDDiskId()->CopyFrom(pb.GetDDiskId(i));
                        }
                        if (i < numPersistentBuffers) {
                            node->MutablePersistentBufferDDiskId()->CopyFrom(pb.GetPersistentBufferDDiskId(i));
                        }
                    }
                }
            }

            return true;
        }

        void PrepareForAllocation() {
            for (const auto& [groupId, groupInfo] : Self->GroupMap) {
                if (!groupInfo->DDisk) {
                    continue;
                }
                ui32 rating = 0;
                for (const auto& vslot : groupInfo->VDisksInGroup) {
                    rating = Max(rating, vslot->DDiskNumVChunksClaimed);
                }

                const auto poolIt = Self->StoragePools.find(groupInfo->StoragePoolId);
                Y_ABORT_UNLESS(poolIt != Self->StoragePools.end());
                GroupRating[poolIt->second.Name].emplace(rating, groupId);
            }
        }

        bool AllocateDDiskGroup(const TString& ddiskPoolName, ui32 numVChunks, std::vector<TDDiskId> *ddiskIds) {
            const auto it = GroupRating.find(ddiskPoolName);
            if (it == GroupRating.end()) {
                return false;
            }

            auto& rating = it->second;
            if (rating.empty()) {
                return false;
            }

            // pick the group with minimum rating, increase it and put it back
            auto node = rating.extract(rating.begin());
            node.key() += numVChunks;
            const TGroupId groupId = node.mapped();
            rating.insert(std::move(node));

            const auto *group = Self->FindGroup(groupId);
            Y_ABORT_UNLESS(group);
            for (const auto& vslot : group->VDisksInGroup) {
                const auto& id = vslot->VSlotId;
                ddiskIds->emplace_back(id.NodeId, id.PDiskId, id.VSlotId);
            }

            return true;
        }

        std::optional<TDDiskId> AllocatePeristentBuffer(const TString& ddiskPoolName, const TDDiskId& ddiskId) {
            (void)ddiskPoolName;
            return ddiskId;
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
