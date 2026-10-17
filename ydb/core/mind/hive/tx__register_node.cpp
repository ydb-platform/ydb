#include "hive_impl.h"
#include "hive_log.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

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
        YDB_LOG_DEBUG("THive::TTxRegisterNode::Execute registering node",
            {"logPrefix", GetLogPrefix()},
            {"nodeId", Local.NodeId()});
        NIceDb::TNiceDb db(txc.DB);
        TNodeId nodeId = Local.NodeId();
        TNodeInfo& node = Self->GetNode(nodeId);
        const bool localChanged = node.Local != Local;
        const TActorId previousLocal = node.Local;
        const auto previousState = node.GetVolatileState();
        if (localChanged) {
            Self->RemoveNodeFromSegments(&node);

            TInstant now = TActivationContext::Now();
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
                if (!node.LastSeenServicedDomains.empty()) {
                    Self->RecordNodeEvent(node, EHiveEventType::TenantChanged, EHiveEventReason::ServicedDomainsChanged,
                        TStringBuilder() << "from=" << node.LastSeenServicedDomains << " to=" << servicedDomains);
                }
                node.SetDown(false, EHiveEventReason::TenantChanged);
                node.SetFreeze(false, EHiveEventReason::TenantChanged);
                db.Table<Schema::Node>().Key(nodeId).Update<Schema::Node::Down, Schema::Node::Freeze>(false, false);
            }
            if (node.BecomeUpOnRestart) {
                YDB_LOG_TRACE("THive::TTxRegisterNode::Execute node became up on restart",
                    {"logPrefix", GetLogPrefix()},
                    {"nodeId", Local.NodeId()});
                node.SetDown(false, EHiveEventReason::BecomeUpOnRestart);
                node.BecomeUpOnRestart = false;
                db.Table<Schema::Node>().Key(nodeId).Update<Schema::Node::Down, Schema::Node::BecomeUpOnRestart>(false, false);
            }
            node.Local = Local;
            node.ServicedDomains.swap(servicedDomains);
            node.LastSeenServicedDomains = node.ServicedDomains;
            node.Name = name;
            if (Record.HasBridgePileId()) {
                node.BridgePileId = TBridgePileId::FromProto(&Record, &decltype(Record)::GetBridgePileId);
                Self->GetPile(node.BridgePileId).Nodes.insert(nodeId);
                db.Table<Schema::Node>().Key(nodeId).Update<Schema::Node::BridgePileId>(node.BridgePileId.GetLocalDb());
            } else {
                Y_ENSURE(!Self->BridgeInfo, "Running in bridge mode, but node " << nodeId << " has no pile");
            }

            Self->UpdateNodeSegments(&node);

            // Restore metrics accounting cleared by BecomeDisconnected().
            // The new Local cannot restore it: the tablet is managed by an external owner.
            if (Self->CurrentConfig.GetLockedTabletsSendMetrics()) {
                for (TLeaderTabletInfo* tablet : node.LockedTablets) {
                    if (!tablet->IsDeleting()) {
                        tablet->BecomeUnknown(&node);
                    }
                }
            }
        }
        if (Record.HasSystemLocation() && Record.GetSystemLocation().HasDataCenter()) {
            node.SetLocation(TNodeLocation(Record.GetSystemLocation()), EHiveEventReason::RegisterNode);
        }
        node.TabletAvailability.clear();
        for (const NKikimrLocal::TTabletAvailability& tabletAvailability : Record.GetTabletAvailability()) {
            auto tabletType = tabletAvailability.GetType();
            auto [itAvail, _] = node.TabletAvailability.emplace(tabletType, tabletAvailability);
            auto itRestr = node.TabletAvailabilityRestrictions.find(tabletType);
            if (itRestr != node.TabletAvailabilityRestrictions.end()) {
                itAvail->second.UpdateRestriction(itRestr->second);
            }
        }
        if (node.BecomeConnecting() || localChanged) {
            TStringBuilder details;
            details << "previousState=" << TNodeInfo::EVolatileStateName(previousState);
            if (localChanged && previousLocal) {
                details << " previousLocal=" << previousLocal;
            }
            details << " domains=" << node.ServicedDomains
                << " location=" << GetLocationString(node.Location)
                << " tabletTypesAvailable=" << node.TabletAvailability.size();
            Self->RecordNodeEvent(node, EHiveEventType::Registered,
                localChanged ? EHiveEventReason::NewLocalActor : EHiveEventReason::SameLocalActor, std::move(details));
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        YDB_LOG_DEBUG("THive::TTxRegisterNode::Complete",
            {"logPrefix", GetLogPrefix()},
            {"nodeId", Local.NodeId()});
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
