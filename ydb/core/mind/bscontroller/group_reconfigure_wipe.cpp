#include "impl.h"

namespace NKikimr {
namespace NBsController {

class TBlobStorageController::TTxGroupReconfigureWipe : public TTransactionBase<TBlobStorageController> {
protected:
    TEvBlobStorage::TEvControllerGroupReconfigureWipe::TPtr Event;
    NKikimrProto::EReplyStatus Status;
    TString ErrorReason;
    typedef TMap<TNodeId, THolder<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>> TResultForNode;
    TResultForNode ResultForNode;

public:
    TTxGroupReconfigureWipe(TEvBlobStorage::TEvControllerGroupReconfigureWipe::TPtr &ev,
            TBlobStorageController *controller)
        : TBase(controller)
        , Event(ev)
        , Status(NKikimrProto::OK)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_GROUP_RECONFIGURE_WIPE; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXGRW01, "TTxGroupReconfigureWipe execute");
        NIceDb::TNiceDb db(txc.DB);

        Status = NKikimrProto::ERROR;
        ResultForNode.clear();

        auto& record = Event->Get()->Record;
        const TVSlotId id(record.GetVSlotId());

        if (TVSlotInfo *info = Self->FindVSlot(id); !info) {
            STLOG(PRI_ERROR, BS_CONTROLLER, BSCTXGRW02, "VSlot does not exist", (VSlotId, id));
            ErrorReason = TStringBuilder() << "VSlotId# " << id << " does not exist";
        } else if (info->Mood != TMood::Normal) { // Reply ERROR if From VSlot is improper mood
            STLOG(PRI_ERROR, BS_CONTROLLER, BSCTXGRW03, "VSlot is not in the 'Normal' mood", (VSlotId, id),
                (Mood, TMood::Name(TMood::EValue(info->Mood))));
            ErrorReason = TStringBuilder() << "VSlotId# " << id << " is in Mood# " << TMood::Name(TMood::EValue(info->Mood));
        } else {
            // TODO(cthulhu): Prohibit more than Max simultaneous reconfigurations on one group

            // Mark VSlot for wipe
            info->Mood = TMood::Wipe;
            db.Table<Schema::VSlot>().Key(id.GetKey()).Update<Schema::VSlot::Mood>(info->Mood);

            // Prepare results for nodes
            if (TNodeInfo *node = Self->FindNode(id.NodeId); node && node->ConnectedCount) {
                auto& msg = ResultForNode[id.NodeId];
                msg = MakeHolder<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(NKikimrProto::OK, id.NodeId);
                Self->ReadVSlot(*info, msg.Get());
                TSet<ui32> groupIDsToRead;
                groupIDsToRead.insert(info->GroupId);
                Self->ReadGroups(groupIDsToRead, false, msg.Get(), id.NodeId);
                for (const TGroupId groupId : groupIDsToRead) {
                    STLOG(PRI_ERROR, BS_CONTROLLER, BSCTXGRW05, "No configuration for group", (GroupId, groupId));
                }
            }

            Status = NKikimrProto::OK;
        }

        return true;
    }

    void Complete(const TActorContext&) override {
        // Send results to nodes
        if (Status == NKikimrProto::OK) {
            for (auto& [nodeId, msg] : ResultForNode) {
                // TODO(cthulhu): Availability Domain !!!
                const auto& node = nodeId;
                const auto& record = msg->Record;
                STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXGRW07, "Sending update", (NodeId, node), (Message, record));
                TActivationContext::Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), Self->SelfId(), msg.Release()));
            }
        }

        // Respond
        auto response = MakeHolder<TEvBlobStorage::TEvControllerGroupReconfigureWipeResult>(Status, ErrorReason);
        auto& record = Event->Get()->Record;
        STLOG(Status == NKikimrProto::OK ? PRI_DEBUG : PRI_ERROR, BS_CONTROLLER, BSCTXGRW06, "TTxGroupReconfigureWipe complete",
            (Status, Status), (Request, record), (Response, response->Record));
        TActivationContext::Send(new IEventHandle(Event->Sender, Self->SelfId(), response.Release(), 0, Event->Cookie));
    }
};

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerGroupReconfigureWipe::TPtr& ev) {
    Execute(new TTxGroupReconfigureWipe(ev, this));
}

}
}
