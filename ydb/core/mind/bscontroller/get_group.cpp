#include "impl.h"

namespace NKikimr::NBsController {

class TBlobStorageController::TTxGetGroup : public TTransactionBase<TBlobStorageController> {
    TEvBlobStorage::TEvControllerGetGroup::TPtr Request;
    std::unique_ptr<IEventHandle> Response;

public:
    TTxGetGroup(TEvBlobStorage::TEvControllerGetGroup::TPtr& ev, TBlobStorageController *controller)
        : TTransactionBase(controller)
        , Request(ev)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_GET_GROUP; }

    bool Execute(TTransactionContext& /*txc*/, const TActorContext&) override {
        Self->TabletCounters->Cumulative()[NBlobStorageController::COUNTER_GET_GROUP_COUNT].Increment(1);
        TRequestCounter counter(Self->TabletCounters, NBlobStorageController::COUNTER_GET_GROUP_USEC);

        auto request = std::move(Request);
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXGG01, "Handle TEvControllerGetGroup", (Request, request->Get()->Record));

        const auto& v = request->Get()->Record.GetGroupIDs();
        TSet<ui32> groupIDsToRead(v.begin(), v.end());

        const TNodeId nodeId = request->Get()->Record.GetNodeID();
        auto res = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(NKikimrProto::OK, nodeId);
        Self->ReadGroups(groupIDsToRead, true, res.get(), nodeId);

        Response = std::make_unique<IEventHandle>(MakeBlobStorageNodeWardenID(nodeId), Self->SelfId(), res.release());
        return true;
    }

    void Complete(const TActorContext&) override {
        TActivationContext::Send(Response.release());
    }
};

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerGetGroup::TPtr& ev) {
    if (!StopGivingGroups) {
        Execute(new TTxGetGroup(ev, this));
    }
}

} // NKikimr::NBsController
