#include "hive_impl.h"
#include "hive_log.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

namespace NKikimr {
namespace NHive {

class TTxKillNode : public TTransactionBase<THive> {
protected:
    TNodeId NodeId;
    TActorId Local;
    TString Reason;
    TString ReasonExtra;
    TSideEffects SideEffects;
public:
    TTxKillNode(TNodeId nodeId, const TActorId& local, const TString& reason, TString reasonExtra, THive *hive)
        : TBase(hive)
        , NodeId(nodeId)
        , Local(local)
        , Reason(reason)
        , ReasonExtra(std::move(reasonExtra))
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_KILL_NODE; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        YDB_LOG_DEBUG("THive::TTxKillNode::Execute killing node",
            {"logPrefix", GetLogPrefix()},
            {"nodeId", NodeId},
            {"reason", Reason},
            {"reasonExtra", ReasonExtra});
        SideEffects.Reset(Self->SelfId());
        TInstant now = TActivationContext::Now();
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
            if (!node->IsDisconnected()) {
                TStringBuilder extra;
                extra << ReasonExtra << (ReasonExtra.empty() ? "" : " ") << "tablets=" << node->GetTabletsTotal();
                if (node->IsAlive() && node->StartTime) {
                    extra << " uptime=" << (now - node->StartTime);
                }
                Self->RecordNodeEvent(*node, ENodeEvent::Killed, Reason, extra);
            }
            node->BecomeDisconnected();
            if (node->LocationAcquired) {
                Self->RemoveRegisteredDataCentersNode(node->Location.GetDataCenterId(), node->Id);
            }
            for (const TActorId& pipeServer : node->PipeServers) {
                YDB_LOG_TRACE("THive::TTxKillNode::Execute killing pipe server",
                    {"logPrefix", GetLogPrefix()},
                    {"pipeServer", pipeServer});
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
        YDB_LOG_DEBUG("THive::TTxKillNode::Complete",
            {"logPrefix", GetLogPrefix()},
            {"nodeId", NodeId});
        SideEffects.Complete(ctx, Self->Requests);
        if (Local) {
            TNodeInfo* node = Self->FindNode(Local.NodeId());
            if (node == nullptr || node->IsDisconnected()) {
                YDB_LOG_DEBUG("THive::TTxKillNode::Complete sending reconnect",
                    {"logPrefix", GetLogPrefix()},
                    {"nodeId", NodeId},
                    {"local", Local});
                Self->SendReconnect(Local); // defibrillation
            }
        }
    }
};

ITransaction* THive::CreateKillNode(TNodeId nodeId, const TActorId& local, const TString& reason, TString reasonExtra) {
    return new TTxKillNode(nodeId, local, reason, std::move(reasonExtra), this);
}

} // NHive
} // NKikimr
