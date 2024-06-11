#include "impl.h"
#include "select_groups.h"

namespace NKikimr::NBsController {

class TBlobStorageController::TTxSelectGroups : public TTransactionBase<TBlobStorageController> {
    TEvBlobStorage::TEvControllerSelectGroups::TPtr Request;
    std::unique_ptr<IEventHandle> Response;

public:
    TTxSelectGroups(TEvBlobStorage::TEvControllerSelectGroups::TPtr& ev, TBlobStorageController *controller)
        : TTransactionBase(controller)
        , Request(ev)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_SELECT_GROUPS; }

    bool Execute(TTransactionContext& /*txc*/, const TActorContext&) override {
        Self->TabletCounters->Cumulative()[NBlobStorageController::COUNTER_SELECT_GROUPS_COUNT].Increment(1);
        TRequestCounter counter(Self->TabletCounters, NBlobStorageController::COUNTER_SELECT_GROUPS_USEC);

        auto request = std::move(Request);

        THPTimer timer;

        const auto& record = request->Get()->Record;
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXSG01, "Handle TEvControllerSelectGroups", (Request, record));

        auto result = MakeHolder<TEvBlobStorage::TEvControllerSelectGroupsResult>();
        auto& out = result->Record;
        out.SetStatus(NKikimrProto::OK);
        out.SetNewStyleQuerySupported(true);

        if (!record.GetReturnAllMatchingGroups()) {
            Y_DEBUG_ABORT("obsolete command");
            out.SetStatus(NKikimrProto::ERROR);
        } else {
            TVector<const TGroupInfo*> groups;

            for (const auto& params : record.GetGroupParameters()) {
                STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXSG02, "Searching for group with parameters", (Params, params));

                if (!TGroupSelector::PopulateGroups(groups, params, *Self)) {
                    STLOG(PRI_ERROR, BS_CONTROLLER, BSCTXSG03, "Handle TEvControllerSelectGroups: invalid parameters requested",
                        (Params, params));
                    out.SetStatus(NKikimrProto::ERROR);
                    break;
                }

                auto *pb = out.AddMatchingGroups();
                for (const TGroupInfo *group : groups) {
                    if (!group->Down && (group->SeenOperational || !record.GetOnlySeenOperational())) {
                        auto *reportedGroup = pb->AddGroups();
                        reportedGroup->SetErasureSpecies(group->ErasureSpecies);
                        reportedGroup->SetGroupID(group->ID.GetRawId());
                        reportedGroup->SetStoragePoolName(Self->StoragePools.at(group->StoragePoolId).Name);
                        reportedGroup->SetPhysicalGroup(group->IsPhysicalGroup());
                        reportedGroup->SetDecommitted(group->IsDecommitted());
                        group->FillInGroupParameters(reportedGroup);
                    }
                }
            }
        }

        if (record.GetBlockUntilAllResourcesAreComplete()) {
            auto it = Self->SelectGroupsQueue.emplace(Self->SelectGroupsQueue.end(), request->Sender, request->Cookie,
                std::move(result));
            Self->ProcessSelectGroupsQueueItem(it);
        } else {
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXSG04, "TEvControllerSelectGroups finished", (Result, result->Record));
            Response = std::make_unique<IEventHandle>(request->Sender, Self->SelfId(), result.Release(), 0, request->Cookie);

            const TDuration passed = TDuration::Seconds(timer.Passed());
            Self->TabletCounters->Percentile()[NBlobStorageController::COUNTER_PERCENTILE_SELECT_GROUPS].IncrementFor(passed.MicroSeconds());
        }

        return true;
    }

    void Complete(const TActorContext&) override {
        if (Response) {
            TActivationContext::Send(Response.release());
        }
    }
};

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerSelectGroups::TPtr &ev) {
    Execute(new TTxSelectGroups(ev, this));
}

void TBlobStorageController::ProcessSelectGroupsQueueItem(TList<TSelectGroupsQueueItem>::iterator it) {
    for (const TPDiskId& key : std::exchange(it->BlockedPDisks, {})) {
        const ui32 num = PDiskToQueue.erase(std::make_pair(key, it));
        Y_ABORT_UNLESS(num);
    }

    bool missing = false;

    auto& record = it->Event->Record;
    for (size_t i = 0; i < record.MatchingGroupsSize(); ++i) {
        auto& mg = *record.MutableMatchingGroups(i);
        for (size_t j = 0; j < mg.GroupsSize(); ++j) {
            auto& g = *mg.MutableGroups(j);

            const auto hasResources = [&] {
                const auto& assured = g.GetAssuredResources();
                const auto& current = g.GetCurrentResources();
                return assured.HasSpace()
                    && assured.HasIOPS()
                    && assured.HasReadThroughput()
                    && assured.HasWriteThroughput()
                    && current.HasSpace()
                    && current.HasIOPS()
                    && current.HasReadThroughput()
                    && current.HasWriteThroughput();

            };

            if (TGroupInfo *group = FindGroup(TGroupId::FromProto(&g, &NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters::GetGroupID)); group && !hasResources()) {
                group->FillInGroupParameters(&g);
                if (!hasResources()) {
                    // any of PDisks will do
                    for (const TVSlotInfo *vslot : group->VDisksInGroup) {
                        const TPDiskId pdiskId = vslot->VSlotId.ComprisingPDiskId();
                        it->BlockedPDisks.insert(pdiskId);
                        PDiskToQueue.emplace(pdiskId, it);
                    }
                    missing = true;
                }
            }
        }
    }

    if (!missing) {
        Send(it->RespondTo, it->Event.Release(), 0, it->Cookie);
        SelectGroupsQueue.erase(it);
    }
}

} // NKikimr::NBsController
