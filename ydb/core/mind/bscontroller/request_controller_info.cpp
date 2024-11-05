#include "impl.h"

namespace NKikimr::NBsController {

class TBlobStorageController::TTxRequestControllerInfo : public TTransactionBase<TBlobStorageController> {
    TEvBlobStorage::TEvRequestControllerInfo::TPtr Request;
    std::unique_ptr<IEventHandle> Response;

public:
    TTxRequestControllerInfo(TEvBlobStorage::TEvRequestControllerInfo::TPtr& ev, TBlobStorageController *controller)
        : TTransactionBase(controller)
        , Request(ev)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_REQUEST_CONTROLLER_INFO; }

    bool Execute(TTransactionContext& /*txc*/, const TActorContext&) override {
        Self->TabletCounters->Cumulative()[NBlobStorageController::COUNTER_REQUEST_INFO_COUNT].Increment(1);
        TRequestCounter counter(Self->TabletCounters, NBlobStorageController::COUNTER_REQUEST_INFO_USEC);

        auto request = std::move(Request);
        const auto& requestRecord = request->Get()->Record;

        auto response = std::make_unique<TEvBlobStorage::TEvResponseControllerInfo>();
        auto& responseRecord = response->Record;

        auto processGroup = [&](TGroupInfo *group) {
            auto *protoGroupInfo = responseRecord.AddBSGroupInfo();
            protoGroupInfo->SetGroupId(group->ID.GetRawId());
            protoGroupInfo->SetErasureSpecies(group->ErasureSpecies);
            const TResourceRawValues& groupResources = group->GetResourceCurrentValues();
            protoGroupInfo->SetDataSize(groupResources.DataSize);

            for (const TVSlotInfo *vslot : group->VDisksInGroup) {
                auto *vDiskInfo = protoGroupInfo->AddVDiskInfo();
                vDiskInfo->SetNodeId(vslot->VSlotId.NodeId);
                vDiskInfo->SetPDiskId(vslot->VSlotId.PDiskId);
                vDiskInfo->SetVDiskCategory(vslot->Kind);
                vDiskInfo->SetPDiskCategory(vslot->PDisk->Kind.GetRaw());
                VDiskIDFromVDiskID(vslot->GetVDiskId(), vDiskInfo->MutableVDiskId());
            }
        };

        if (requestRecord.HasGroupId()) {
            if (TGroupInfo *group = Self->FindGroup(TGroupId::FromProto(&requestRecord, &NKikimrBlobStorage::TEvRequestBSControllerInfo::GetGroupId))) {
                processGroup(group);
            }
        } else {
            for (const auto& [groupId, group] : Self->GroupMap) {
                processGroup(group.Get());
            }
        }

        Response = std::make_unique<IEventHandle>(request->Sender, Self->SelfId(), response.release());
        return true;
    }

    void Complete(const TActorContext&) override {
        TActivationContext::Send(Response.release());
    }
};

void TBlobStorageController::Handle(TEvBlobStorage::TEvRequestControllerInfo::TPtr& ev) {
    Execute(new TTxRequestControllerInfo(ev, this));
}

} // NKikimr::NBsController
