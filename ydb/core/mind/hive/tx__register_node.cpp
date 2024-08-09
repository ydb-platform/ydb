#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxRegisterNode : public TTransactionBase<THive> {
    TActorId Local;
    NKikimrLocal::TEvRegisterNode Record;

public:
    TTxRegisterNode(const TActorId& local, NKikimrLocal::TEvRegisterNode record, THive *hive)
        : TBase(hive)
        , Local(local)
        , Record(std::move(record))
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_REGISTER_NODE; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        BLOG_D("THive::TTxRegisterNode(" << Local.NodeId() << ")::Execute");
        NIceDb::TNiceDb db(txc.DB);
        TNodeId nodeId = Local.NodeId();
        TNodeInfo& node = Self->GetNode(nodeId);
        if (node.Local != Local) {
            TInstant now = TInstant::Now();
            node.Statistics.AddRestartTimestamp(now.MilliSeconds());
            node.ActualizeNodeStatistics(now);
            for (const auto& t : node.Tablets) {
                for (TTabletInfo* tablet : t.second) {
                    if (tablet->IsLeader()) {
                        db.Table<Schema::Tablet>().Key(tablet->GetLeader().Id).Update<Schema::Tablet::LeaderNode>(0);
                    } else {
                        db.Table<Schema::TabletFollowerTablet>().Key(tablet->GetFullTabletId()).Update<Schema::TabletFollowerTablet::FollowerNode>(0);
                    }
                }
            }
            TVector<TSubDomainKey> servicedDomains(Record.GetServicedDomains().begin(), Record.GetServicedDomains().end());
            if (servicedDomains.empty()) {
                servicedDomains.emplace_back(Self->RootDomainKey);
            } else {
                Sort(servicedDomains);
            }
            const TString& name = Record.GetName();
            db.Table<Schema::Node>().Key(nodeId).Update(
                NIceDb::TUpdate<Schema::Node::Local>(Local),
                NIceDb::TUpdate<Schema::Node::ServicedDomains>(servicedDomains),
                NIceDb::TUpdate<Schema::Node::Statistics>(node.Statistics),
                NIceDb::TUpdate<Schema::Node::Name>(name)
            );
 
            node.BecomeDisconnected();
            if (node.LastSeenServicedDomains != servicedDomains) {
                // new tenant - new rules
                node.Down = false;
                node.Freeze = false;
                db.Table<Schema::Node>().Key(nodeId).Update<Schema::Node::Down, Schema::Node::Freeze>(false, false);
            }
            node.Local = Local;
            node.ServicedDomains.swap(servicedDomains);
            node.LastSeenServicedDomains = node.ServicedDomains;
            node.Name = name;
        }
        if (Record.HasSystemLocation() && Record.GetSystemLocation().HasDataCenter()) {
            node.Location = TNodeLocation(Record.GetSystemLocation());
            node.LocationAcquired = true;
        }
        node.TabletAvailability.clear();
        for (const NKikimrLocal::TTabletAvailability& tabletAvailability : Record.GetTabletAvailability()) {
            node.TabletAvailability.emplace(tabletAvailability.GetType(), tabletAvailability);
        }
        node.BecomeConnecting();
        return true;
    }

    void Complete(const TActorContext&) override {
        BLOG_D("THive::TTxRegisterNode(" << Local.NodeId() << ")::Complete");
        TNodeInfo* node = Self->FindNode(Local.NodeId());
        if (node != nullptr && node->Local) { // we send ping on every RegisterNode because we want to re-sync tablets upon every reconnection
            Self->NodePingsInProgress.erase(node->Id);
            node->Ping();
            Self->ProcessNodePingQueue();
        }
    }
};

ITransaction* THive::CreateRegisterNode(const TActorId& local, NKikimrLocal::TEvRegisterNode rec) {
    return new TTxRegisterNode(local, std::move(rec), this);
}

} // NHive
} // NKikimr
