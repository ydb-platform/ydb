#include "impl.h"
#include "select_groups.h"

#define YDB_LOG_THIS_FILE_COMPONENT BS_CONTROLLER

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
        YDB_LOG_DEBUG("Handle TEvControllerSelectGroups",
            {"marker", "BSCTXSG01"},
            {"request", record},
            {"sender", request->Sender},
            {"cookie", request->Cookie});

        auto result = MakeHolder<TEvBlobStorage::TEvControllerSelectGroupsResult>();
        auto& out = result->Record;
        out.SetStatus(NKikimrProto::OK);
        out.SetNewStyleQuerySupported(true);

        THashSet<TGroupId> missingGroupIds;

        if (!record.GetReturnAllMatchingGroups()) {
            Y_DEBUG_ABORT("obsolete command");
            out.SetStatus(NKikimrProto::ERROR);
        } else {
            TVector<const TGroupInfo*> groups;

            for (const auto& params : record.GetGroupParameters()) {
                YDB_LOG_DEBUG("Searching for group with parameters",
                    {"marker", "BSCTXSG02"},
                    {"params", params});

                if (!TGroupSelector::PopulateGroups(groups, params, *Self)) {
                    YDB_LOG_ERROR("Handle TEvControllerSelectGroups: invalid parameters requested",
                        {"marker", "BSCTXSG03"},
                        {"params", params});
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
                        if (!group->FillInGroupParameters(reportedGroup, Self)) {
                            missingGroupIds.insert(group->ID);
                        }
                    }
                }
            }
        }

        if (record.GetBlockUntilAllResourcesAreComplete() && !missingGroupIds.empty()) {
            YDB_LOG_DEBUG("TEvControllerSelectGroups failed",
                {"marker", "BSCTXSG05"},
                {"missingGroupIds", missingGroupIds},
                {"sender", request->Sender},
                {"cookie", request->Cookie});
            auto iter = Self->WaitingSelectGroups.emplace(Self->WaitingSelectGroups.end(), request, std::move(missingGroupIds));
            for (TGroupId groupId : iter->MissingGroups) {
                Self->GroupToWaitingSelectGroupsItem.emplace(groupId, iter);
            }
        } else {
            YDB_LOG_DEBUG("TEvControllerSelectGroups finished",
                {"marker", "BSCTXSG04"},
                {"result", result->Record},
                {"sender", request->Sender},
                {"cookie", request->Cookie});
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

void TBlobStorageController::UpdateWaitingGroups(const THashSet<TGroupId>& groupIds) {
    auto process = [&](TGroupId groupId) {
        auto it = GroupToWaitingSelectGroupsItem.lower_bound(std::make_tuple(groupId,
            std::list<TWaitingSelectGroupsItem>::iterator()));
        while (it != GroupToWaitingSelectGroupsItem.end() && std::get<0>(*it) == groupId) {
            auto iter = std::get<1>(*it);
            it = GroupToWaitingSelectGroupsItem.erase(it);
            const size_t n = iter->MissingGroups.erase(groupId);
            Y_ABORT_UNLESS(n == 1);
            if (iter->MissingGroups.empty()) {
                TActivationContext::Send(iter->Request.Release()); // restart this query
                WaitingSelectGroups.erase(iter);
            }
        }
    };
    for (TGroupId groupId : groupIds) {
        process(groupId);
        if (TGroupInfo *group = FindGroup(groupId); group && group->BridgeProxyGroupId) {
            process(*group->BridgeProxyGroupId);
        }
    }
}

} // NKikimr::NBsController
