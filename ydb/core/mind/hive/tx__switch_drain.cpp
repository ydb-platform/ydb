#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxSwitchDrainOn : public TTransactionBase<THive> {
    TNodeId NodeId;
    TDrainSettings Settings;
    TActorId Initiator;
    NKikimrProto::EReplyStatus Status = NKikimrProto::UNKNOWN;
public:
    TTxSwitchDrainOn(TNodeId nodeId, TDrainSettings settings, const TActorId& initiator, THive* hive)
        : TBase(hive)
        , NodeId(nodeId)
        , Settings(std::move(settings))
        , Initiator(initiator)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        BLOG_D("THive::TTxSwitchDrainOn::Execute Node: " << NodeId
                << " Persist: " << Settings.Persist << " DownPolicy: " << static_cast<int>(Settings.DownPolicy));
        NIceDb::TNiceDb db(txc.DB);
        TNodeInfo* node = Self->FindNode(NodeId);
        if (node != nullptr) {
            if (!(node->Drain) && Self->BalancerNodes.count(NodeId) != 0) {
                Status = NKikimrProto::ALREADY; // another balancer is active on the node
            } else {
                Status = NKikimrProto::OK;
                node->Drain = true;
                node->DrainInitiators.emplace_back(Initiator);
                if (Settings.Persist) {
                    db.Table<Schema::Node>().Key(NodeId).Update<Schema::Node::Drain, Schema::Node::DrainInitiators>(node->Drain, node->DrainInitiators);
                }
                if (Settings.DownPolicy != NKikimrHive::EDrainDownPolicy::DRAIN_POLICY_NO_DOWN) {
                    if (!node->Down && Settings.DownPolicy == NKikimrHive::EDrainDownPolicy::DRAIN_POLICY_KEEP_DOWN_UNTIL_RESTART) {
                        node->BecomeUpOnRestart = true;
                    }
                    node->SetDown(true);
                    if (Settings.Persist) {
                        db.Table<Schema::Node>().Key(NodeId).Update<Schema::Node::BecomeUpOnRestart>(node->BecomeUpOnRestart);
                        if (Settings.DownPolicy == NKikimrHive::DRAIN_POLICY_KEEP_DOWN) {
                            db.Table<Schema::Node>().Key(NodeId).Update<Schema::Node::Down>(true);
                        }
                    }
                }
                Self->StartHiveDrain(NodeId, std::move(Settings));
            }
        } else {
            Status = NKikimrProto::ERROR;
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        BLOG_D("THive::TTxSwitchDrainOn::Complete NodeId: " << NodeId << " Status: " << Status);
        if (Status != NKikimrProto::OK) {
            if (Initiator) {
                Self->Send(Initiator, new TEvHive::TEvDrainNodeResult(Status));
            }
        }
    }
};

class TTxSwitchDrainOff : public TTransactionBase<THive> {
    TNodeId NodeId;
    TDrainSettings Settings;
    NKikimrProto::EReplyStatus Status;
    ui32 Movements;
    TVector<TActorId> Initiators;

public:
    TTxSwitchDrainOff(TNodeId nodeId, TDrainSettings settings, NKikimrProto::EReplyStatus status, ui32 movements, THive* hive)
        : TBase(hive)
        , NodeId(nodeId)
        , Settings(std::move(settings))
        , Status(status)
        , Movements(movements)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        BLOG_D("THive::TTxSwitchDrainOff::Execute Node: " << NodeId);
        NIceDb::TNiceDb db(txc.DB);
        TNodeInfo* node = Self->FindNode(NodeId);
        if (node != nullptr) {
            Initiators = std::move(node->DrainInitiators);
            node->Drain = false;
            node->DrainInitiators.clear();
            db.Table<Schema::Node>().Key(NodeId).Update<Schema::Node::Drain, Schema::Node::DrainInitiators>(node->Drain, node->DrainInitiators);
            if (Settings.DownPolicy == NKikimrHive::EDrainDownPolicy::DRAIN_POLICY_NO_DOWN) {
                // node->SetDown(false); // it has already been dropped by Drain actor
                if (Settings.Persist) {
                    db.Table<Schema::Node>().Key(NodeId).Update<Schema::Node::Down>(false);
                }
            }
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        BLOG_D("THive::TTxSwitchDrainOff::Complete NodeId: " << NodeId
            << " Status: " << NKikimrProto::EReplyStatus_Name(Status) << " Movements: " << Movements);
        for (const TActorId& initiator : Initiators) {
            Self->Send(initiator, new TEvHive::TEvDrainNodeResult(Status, Movements));
        }
    }
};

ITransaction* THive::CreateSwitchDrainOn(NHive::TNodeId nodeId, TDrainSettings settings, const TActorId& initiator) {
    return new TTxSwitchDrainOn(nodeId, std::move(settings), initiator, this);
}

ITransaction* THive::CreateSwitchDrainOff(NHive::TNodeId nodeId, TDrainSettings settings, NKikimrProto::EReplyStatus status, ui32 movements) {
    return new TTxSwitchDrainOff(nodeId, std::move(settings), status, movements, this);
}

} // NHive
} // NKikimr


