#include "impl.h"

namespace NKikimr::NBsController {

class TBlobStorageController::TTxGetGroup : public TTransactionBase<TBlobStorageController> {
    TEvBlobStorage::TEvControllerGetGroup::TPtr Request;
    std::unique_ptr<TEvBlobStorage::TEvControllerNodeServiceSetUpdate> Response;
    TNodeId NodeId = {};

public:
    TTxGetGroup(TEvBlobStorage::TEvControllerGetGroup::TPtr& ev, TBlobStorageController *controller)
        : TTransactionBase(controller)
        , Request(ev)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_GET_GROUP; }

    bool Execute(TTransactionContext& /*txc*/, const TActorContext&) override {
        Self->TabletCounters->Cumulative()[NBlobStorageController::COUNTER_GET_GROUP_COUNT].Increment(1);
        TRequestCounter counter(Self->TabletCounters, NBlobStorageController::COUNTER_GET_GROUP_USEC);

        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXGG01, "Handle TEvControllerGetGroup", (Request, Request->Get()->Record));

        NodeId = Request->Get()->Record.GetNodeID();

        if (Request->Cookie != Max<ui64>() && !Self->ValidateIncomingNodeWardenEvent(*Request)) {
            Response = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(NKikimrProto::ERROR, NodeId);
            return true;
        }

        const auto& v = Request->Get()->Record.GetGroupIDs();
        TSet<ui32> groupIDsToRead(v.begin(), v.end());

        Response = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(NKikimrProto::OK, NodeId);
        Self->ReadGroups(groupIDsToRead, true, Response.get(), NodeId);

        auto& node = Self->GetNode(NodeId);
        for (ui32 groupId : v) {
            node.GroupsRequested.insert(TGroupId::FromValue(groupId));
            Self->GroupToNode.emplace(TGroupId::FromValue(groupId), NodeId);
        }

        return true;
    }

    void Complete(const TActorContext&) override {
        if (NodeId) {
            Self->SendToWarden(NodeId, std::move(Response), Request->Cookie);
        } else {
            Self->SendInReply(*Request, std::move(Response));
        }
    }
};

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerGetGroup::TPtr& ev) {
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXGG02, "TEvControllerGetGroup", (Sender, ev->Sender), (Cookie, ev->Cookie),
        (Recipient, ev->Recipient), (RecipientRewrite, ev->GetRecipientRewrite()), (Request, ev->Get()->Record),
        (StopGivingGroups, StopGivingGroups));

    if (!StopGivingGroups) {
        Execute(new TTxGetGroup(ev, this));
    }
}

} // NKikimr::NBsController
