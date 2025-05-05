#include "impl.h"
#include "group_geometry_info.h"
#include "group_mapper_helper.h"

namespace NKikimr::NBsController {

    class TBlobStorageController::TBlobStorageController::TTxDescribeCmd
        : public TTransactionBase<TBlobStorageController>
    {
        const NKikimrBlobStorage::TEvControllerDescribeRequest Request;
        const TActorId NotifyId;
        const ui64 Cookie;
        std::unique_ptr<TEvBlobStorage::TEvControllerDescribeResponse> Response;

    public:
        TTxDescribeCmd(const NKikimrBlobStorage::TEvControllerDescribeRequest &request, const TActorId &notifyId, ui64 cookie, TBlobStorageController *controller)
        : TTransactionBase(controller)
        , Request(request)
        , NotifyId(notifyId)
        , Cookie(cookie)
        , Response(std::make_unique<TEvBlobStorage::TEvControllerDescribeResponse>())
        {
        }

        bool Execute(TTransactionContext&, const TActorContext&) override {
            for (auto& target : Request.GetTargets()) {
                switch (target.GetTargetCase()) {
                    case NKikimrBlobStorage::TEvControllerDescribeRequest_TTarget::kVDiskId: {
                        const auto vdiskId = VDiskIDFromVDiskID(target.GetVDiskId());

                        HandleDescribeVDisk(vdiskId);
                        
                        break;
                    }
                    case NKikimrBlobStorage::TEvControllerDescribeRequest_TTarget::kPDiskId: {
                        auto& id = target.GetPDiskId();
                        TPDiskId pdiskId (id.GetNodeId(), id.GetPDiskId());

                        HandleDescribePDisk(TPDiskId(id.GetNodeId(), id.GetPDiskId()));

                        break;
                    }
                    case NKikimrBlobStorage::TEvControllerDescribeRequest_TTarget::kGroupId: {
                        const auto groupId = target.GetGroupId();

                        HandleDescribeGroup(groupId);
                        
                        break;
                    }
                    default:
                        break;
                }
            }
            return true;
        }

        void Complete(const TActorContext&) override {
            TActivationContext::Send(new IEventHandle(NotifyId, Self->SelfId(), Response.release(), 0, Cookie));
        }

    private:
        class TFinalizer {
        private:
            std::function<void()> func;
        public:
            explicit TFinalizer(std::function<void()> f) : func(std::move(f)) {}
            ~TFinalizer() { func(); }
        };

        void HandleDescribeVDisk(const TVDiskID& vdiskId) {
            TStringStream ss;

            TFinalizer finalizer([&] {
                auto* vdiskResponse = Response->Record.AddResults()->MutableVDiskResponse();
                vdiskResponse->SetResult(ss.Str());
            });

            TVSlotInfo *slot = Self->FindVSlot(vdiskId);
            if (!slot) {
                ss << "VDisk " << vdiskId << " not found" << Endl;
                return;
            }

            ss << "VDisk " << vdiskId << Endl;

            auto group = slot->Group;

            {
                auto status = group->Status;

                if (status.OperatingStatus != NKikimrBlobStorage::TGroupStatus::FULL) {
                    ss << "Group's effective status is not FULL, but " << status.OperatingStatus << Endl;

                    for (const TVSlotInfo *slot : group->VDisksInGroup) {
                        if (!slot->IsReady) {
                            const auto& vid = slot->GetVDiskId();

                            if (vid == vdiskId) {
                                ss << "---->";
                            } else {
                                ss << "     ";
                            }

                            ss << "VDisk " << slot->GetVDiskId() << " on PDisk " << slot->VSlotId.ComprisingPDiskId()  << " is not ready" << Endl;
                        }
                    }

                    ss << Endl;
                }

                if (status.ExpectedStatus != NKikimrBlobStorage::TGroupStatus::FULL) {
                    ss << "Group's projected status is not FULL, but " << status.ExpectedStatus << Endl;
                    
                    for (const TVSlotInfo *slot : group->VDisksInGroup) {
                        if (!slot->PDisk->HasGoodExpectedStatus()) {
                            const auto& vid = slot->GetVDiskId();

                            if (vid == vdiskId) {
                                ss << "---->";
                            } else {
                                ss << "     ";
                            }

                            ss << "PDisk " << slot->VSlotId.ComprisingPDiskId() << " has bad projected Status# " << slot->PDisk->Status << Endl;
                        } else if (!slot->IsReady) {
                            const auto& vid = slot->GetVDiskId();

                            if (vid == vdiskId) {
                                ss << "---->";
                            } else {
                                ss << "     ";
                            }

                            ss << "VDisk " << slot->GetVDiskId() << " on PDisk " << slot->VSlotId.ComprisingPDiskId() << " is not ready" << Endl;
                        }
                    }

                    ss << Endl;
                }
            }

            {
                auto groupId = group->ID.GetRawId();

                const auto& storagePoolIt = Self->StoragePools.find(group->StoragePoolId);
                if (storagePoolIt == Self->StoragePools.end()) {
                    ss << "Storage pool " << group->StoragePoolId << " not found" << Endl;
                    return;
                }

                const auto& storagePool = storagePoolIt->second;

                TGroupGeometryInfo geometry(TBlobStorageGroupType(storagePool.ErasureSpecies), storagePool.GetGroupGeometry());

                TGroupMapper mapper(geometry);

                THashSet<TPDiskId> pdisksToRemove;
                PopulateGroupMapper(mapper, *Self, Self->PDisks, Self->HostRecords, pdisksToRemove, group->StoragePoolId, storagePool, false,
                    Self->PDiskSpaceMarginPromille, false, false);

                TGroupMapper::TGroupDefinition groupDefinition;
                geometry.ResizeGroup(groupDefinition);

                for (auto& vdisk : group->VDisksInGroup) {
                    const auto& vid = vdisk->GetVDiskId();
                    auto& slotId = vdisk->VSlotId;

                    if (slotId == slot->VSlotId) {
                        continue;
                    }

                    groupDefinition[vid.FailRealm][vid.FailDomain][vid.VDisk] = TPDiskId(slotId.NodeId, slotId.PDiskId);
                }
                
                THashMap<TVDiskIdShort, TPDiskId> replacedDisks;

                replacedDisks.emplace(slot->GetShortVDiskId(), TPDiskId(slot->VSlotId.NodeId, slot->VSlotId.PDiskId));

                TString error;

                i64 requiredSpace = Min<i64>();
                if (slot->Metrics.HasAllocatedSize()) {
                    requiredSpace = slot->Metrics.GetAllocatedSize();
                }

                bool allocated = mapper.AllocateGroup(groupId, groupDefinition, replacedDisks, {}, requiredSpace, true, error);

                if (allocated) {
                    // Add info about allocated group.
                    auto& newPdiskId = groupDefinition[vdiskId.FailRealm][vdiskId.FailDomain][vdiskId.VDisk];
                    ss << "VDisk has potential reassignment targets, default one is PDisk " << newPdiskId;
                } else {
                    ss << "There is no other PDisk for this VDisk: " << error;
                }

                ss << Endl;
            }

            ss << Endl;
            
            {
                bool dontTouch = false;

                const TInstant now = TInstant::Now();
                const auto* topology = group->Topology.get();
                const auto& checker = topology->GetQuorumChecker();
                TBlobStorageGroupInfo::TGroupVDisks failedByReadiness(topology);
                TBlobStorageGroupInfo::TGroupVDisks failedByBadness(topology);

                ui32 numReplicatingWithPhantomsOnly = 0;
                for (const auto& vdisk : group->VDisksInGroup) {
                    const auto& vid = vdisk->GetVDiskId();
                    if (vdisk->VDiskStatus) {
                        switch (vdisk->VDiskStatus.value()) {
                            case NKikimrBlobStorage::EVDiskStatus::REPLICATING:
                                if (vdisk->OnlyPhantomsRemain && !numReplicatingWithPhantomsOnly) {
                                    ++numReplicatingWithPhantomsOnly;
                                    break;
                                }
                                [[fallthrough]];
                            case NKikimrBlobStorage::EVDiskStatus::INIT_PENDING:
                                dontTouch = true;
                                break;

                            default:
                                break;
                        }
                    }

                    if (dontTouch) {
                        break;
                    }

                    if (!(vdisk->IsReady && now >= vdisk->LastSeenReady + ReadyStablePeriod)) {
                        failedByReadiness |= {topology, vid};
                    }

                    if (vdisk->PDisk->BadInTermsOfSelfHeal()) {
                        failedByBadness |= {topology, vid};
                    }
                }

                if (dontTouch) {
                    ss << "Don't touch group with replicating or starting disks" << Endl;
                    for (const auto& vdisk : group->VDisksInGroup) {
                        const auto& vid = vdisk->GetVDiskId();
                        ss << "VDisk " << vid << " Status# " << vdisk->VDiskStatus.value() << Endl;
                    }
                    return;
                }

                const auto failed = failedByReadiness | failedByBadness; // assume disks marked as Bad may become non-ready any moment now
                
                if (slot->PDisk->ShouldBeSettledBySelfHeal()) {
                    const auto newFailed = failed | TBlobStorageGroupInfo::TGroupVDisks(topology, vdiskId);
                    if (!checker.CheckFailModelForGroup(newFailed)) {
                        ss << "Self-Heal unavailable, it may break the group" << Endl;
                    } else if (checker.IsDegraded(failed) < checker.IsDegraded(newFailed)) {
                        ss << "Self-Heal unavailable, group may become degraded" << Endl;
                    } else {
                        ss << "Self-Heal will commence" << Endl;
                    }
                } else {
                    if (slot->IsReady) {
                        ss << "Disk is ready and does not require self-heal" << Endl;
                    } else {
                        ss << "Disk is not ready, but is not eligible for self-heal, because PDisk has Status# " << slot->PDisk->Status << Endl;
                    }
                }
            }
        }

        void HandleDescribeGroup(const ui32 groupId) {
            TStringStream ss;

            TFinalizer finalizer([&] {
                auto* groupResponse = Response->Record.AddResults()->MutableGroupResponse();
                groupResponse->SetResult(ss.Str());
            });

            const auto groupIt = Self->GroupMap.find(TGroupId::FromValue(groupId));
            if (groupIt == Self->GroupMap.end()) {
                ss << "Group " << groupId << " not found";
                return;
            }

            ss << "Describe Group not implemented yet";
        }

        void HandleDescribePDisk(const TPDiskId pdiskId) {
            TStringStream ss;

            TFinalizer finalizer([&] {
                auto* pdiskResponse = Response->Record.AddResults()->MutablePDiskResponse();
                pdiskResponse->SetResult(ss.Str());
            });

            const auto pdiskIt = Self->PDisks.find(pdiskId);
            if (pdiskIt == Self->PDisks.end()) {
                ss << "PDisk " << pdiskId << " not found";
                return;
            }

            ss << "Describe PDisk not implemented yet";
        }
    };

    void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerDescribeRequest::TPtr &ev) {
        NKikimrBlobStorage::TEvControllerDescribeRequest& request(ev->Get()->Record);
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXCC01, "Execute TEvControllerConfigRequest", (Request, request));
        Execute(new TTxDescribeCmd(request, ev->Sender, ev->Cookie, this));
    }

} // NKikimr::NBsController
