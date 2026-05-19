#include "impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_CONTROLLER

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

        YDB_LOG_DEBUG("Handle TEvControllerGetGroup",
            {"Marker", "BSCTXGG01"},
            {"Request", Request->Get()->Record});

        NodeId = Request->Get()->Record.GetNodeID();

        if (Request->Cookie != Max<ui64>() && !Self->ValidateIncomingNodeWardenEvent(*Request)) {
            Response = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(NKikimrProto::ERROR, NodeId);
            return true;
        }

        TSet<TGroupId> groupIDsToRead;
        auto& node = Self->GetNode(NodeId);
        for (const ui32 groupIdProto : Request->Get()->Record.GetGroupIDs()) {
            auto groupId = TGroupId::FromValue(groupIdProto);
            groupIDsToRead.insert(groupId);
            node.GroupsRequested.insert(groupId);
            Self->GroupToNode.emplace(groupId, NodeId);
        }

        Response = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(NKikimrProto::OK, NodeId);
        Self->ReadGroups(groupIDsToRead, true, Response.get(), NodeId);

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
    YDB_LOG_DEBUG("TEvControllerGetGroup",
        {"Marker", "BSCTXGG02"},
        {"Sender", ev->Sender},
        {"Cookie", ev->Cookie},
        {"Recipient", ev->Recipient},
        {"RecipientRewrite", ev->GetRecipientRewrite()},
        {"Request", ev->Get()->Record},
        {"StopGivingGroups", StopGivingGroups});

    if (!StopGivingGroups) {
        Execute(new TTxGetGroup(ev, this));
    }
}

} // NKikimr::NBsController
