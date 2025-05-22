#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxKillNode : public TTransactionBase<THive> {
protected:
    TNodeId NodeId;
    TActorId Local;
    TSideEffects SideEffects;
public:
    TTxKillNode(TNodeId nodeId, const TActorId& local, THive *hive)
        : TBase(hive)
        , NodeId(nodeId)
        , Local(local)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_KILL_NODE; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        BLOG_D("THive::TTxKillNode(" << NodeId << ")::Execute");
        SideEffects.Reset(Self->SelfId());
        TInstant now = TInstant::Now();
        TNodeInfo* node = Self->FindNode(NodeId);
        if (node != nullptr) {
            Local = node->Local;
            NIceDb::TNiceDb db(txc.DB);
            for (const auto& t : node->Tablets) {
                for (TTabletInfo* tablet : t.second) {
                    if (tablet->NodeId != 0) {
                        TTabletId tabletId = tablet->GetLeader().Id;
                        if (tablet->IsLeader()) {
                            db.Table<Schema::Tablet>().Key(tabletId).Update<Schema::Tablet::LeaderNode>(0);
                        } else {
                            db.Table<Schema::TabletFollowerTablet>().Key(tablet->GetFullTabletId()).Update<Schema::TabletFollowerTablet::FollowerNode>(0);
                        }
                    }
                }
            }
            if (node->IsAlive()) {
                node->Statistics.SetLastAliveTimestamp(now.MilliSeconds());
                db.Table<Schema::Node>().Key(NodeId).Update<Schema::Node::Statistics>(node->Statistics);
            }
            node->BecomeDisconnected();
            if (node->LocationAcquired) {
                Self->RemoveRegisteredDataCentersNode(node->Location.GetDataCenterId(), node->Id);
            }
            for (const TActorId& pipeServer : node->PipeServers) {
                BLOG_TRACE("THive::TTxKillNode - killing pipe server " << pipeServer);
                SideEffects.Send(pipeServer, new TEvents::TEvPoisonPill());
            }
            node->PipeServers.clear();
            Self->ObjectDistributions.RemoveNode(*node);
            if (Self->TryToDeleteNode(node)) {
                db.Table<Schema::Node>().Key(NodeId).Delete();
            } else {
                db.Table<Schema::Node>().Key(NodeId).Update<Schema::Node::Local>(TActorId());
            }
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxKillNode(" << NodeId << ")::Complete");
        SideEffects.Complete(ctx);
        if (Local) {
            TNodeInfo* node = Self->FindNode(Local.NodeId());
            if (node == nullptr || node->IsDisconnected()) {
                BLOG_D("THive::TTxKillNode(" << NodeId << ")::Complete - send Reconnect to " << Local);
                Self->SendReconnect(Local); // defibrillation
            }
        }
    }
};

ITransaction* THive::CreateKillNode(TNodeId nodeId, const TActorId& local) {
    return new TTxKillNode(nodeId, local, this);
}

} // NHive
} // NKikimr
