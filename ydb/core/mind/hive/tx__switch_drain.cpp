#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxSwitchDrainOn : public TTransactionBase<THive> {
    TNodeId NodeId;
    TDrainSettings Settings;
    TActorId Initiator;
    NKikimrProto::EReplyStatus Status = NKikimrProto::UNKNOWN;
    ui64 SeqNo;
    bool ShouldStartDrain = true;
public:
    TTxSwitchDrainOn(TNodeId nodeId, TDrainSettings settings, const TActorId& initiator, ui64 seqNo, THive* hive)
        : TBase(hive)
        , NodeId(nodeId)
        , Settings(std::move(settings))
        , Initiator(initiator)
        , SeqNo(seqNo)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        BLOG_D("THive::TTxSwitchDrainOn::Execute Node: " << NodeId
                << " Persist: " << Settings.Persist << " DownPolicy: " << static_cast<int>(Settings.DownPolicy));
        NIceDb::TNiceDb db(txc.DB);
        TNodeInfo* node = Self->FindNode(NodeId);
        if (node != nullptr) {
            if (!(node->Drain) && (Self->BalancerNodes.count(NodeId) != 0 || (SeqNo != 0 && SeqNo <= node->DrainSeqNo))) {
                Status = NKikimrProto::ALREADY;
                ShouldStartDrain = false;
            } else {
                Status = NKikimrProto::OK;
                if (node->Drain) {
                    ShouldStartDrain = false;
                }
                node->Drain = true;
                node->DrainInitiators.emplace_back(Initiator);
                if (Settings.Persist) {
                    if (Self->AreWeRootHive()) {
                        ++node->DrainSeqNo;
                    }
                    node->DrainSeqNo = std::max(node->DrainSeqNo, SeqNo);
                    db.Table<Schema::Node>().Key(NodeId).Update<Schema::Node::Drain, Schema::Node::DrainInitiators, Schema::Node::DrainSeqNo>(node->Drain, node->DrainInitiators, node->DrainSeqNo);
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
            }
        } else {
            Status = NKikimrProto::ERROR;
            ShouldStartDrain = false;
        }
        return true;
    }

    void Complete(const TActorContext&) noexcept override {
        BLOG_D("THive::TTxSwitchDrainOn::Complete NodeId: " << NodeId << " Status: " << Status);
        if (ShouldStartDrain) {
            Self->StartHiveDrain(NodeId, std::move(Settings));
        } else {
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

    void Complete(const TActorContext&) noexcept override {
        BLOG_D("THive::TTxSwitchDrainOff::Complete NodeId: " << NodeId
            << " Status: " << NKikimrProto::EReplyStatus_Name(Status) << " Movements: " << Movements);
        for (const TActorId& initiator : Initiators) {
            Self->Send(initiator, new TEvHive::TEvDrainNodeResult(Status, Movements));
        }
    }
};

ITransaction* THive::CreateSwitchDrainOn(NHive::TNodeId nodeId, TDrainSettings settings, const TActorId& initiator, ui64 seqNo) {
    return new TTxSwitchDrainOn(nodeId, std::move(settings), initiator, seqNo, this);
}

ITransaction* THive::CreateSwitchDrainOff(NHive::TNodeId nodeId, TDrainSettings settings, NKikimrProto::EReplyStatus status, ui32 movements) {
    return new TTxSwitchDrainOff(nodeId, std::move(settings), status, movements, this);
}

} // NHive
} // NKikimr


