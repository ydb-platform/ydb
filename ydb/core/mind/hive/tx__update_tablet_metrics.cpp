#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxUpdateTabletMetrics : public TTransactionBase<THive> {
    TEvHive::TEvTabletMetrics::TPtr Event;
    TAutoPtr<TEvLocal::TEvTabletMetricsAck> Reply;
public:
    TTxUpdateTabletMetrics(TEvHive::TEvTabletMetrics::TPtr& ev, THive* hive)
        : TBase(hive)
        , Event(ev)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_UPDATE_TABLET_METRIC; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        TInstant now = TInstant::Now();
        Reply = new TEvLocal::TEvTabletMetricsAck;
        auto& record = Event->Get()->Record;
        TNodeId nodeId = Event->Sender.NodeId();
        NIceDb::TNiceDb db(txc.DB);
        for (const auto& metrics : record.GetTabletMetrics()) {
            TTabletId tabletId = metrics.GetTabletID();
            TFollowerId followerId = metrics.GetFollowerID();
            //BLOG_D("THive::TTxUpdateTabletMetrics::Execute Tablet: " << tabletId);
            TTabletInfo* tablet = Self->FindTablet(tabletId, followerId);
            if (tablet != nullptr && metrics.HasResourceUsage()) {
                tablet->UpdateResourceUsage(metrics.GetResourceUsage());
                const NKikimrTabletBase::TMetrics& metrics(tablet->GetResourceValues());

                db.Table<Schema::Metrics>().Key(tabletId, followerId).Update<Schema::Metrics::ProtoMetrics>(metrics);

                db.Table<Schema::Metrics>().Key(tabletId, followerId).Update<Schema::Metrics::MaximumCPU>(tablet->GetResourceMetricsAggregates().MaximumCPU);
                db.Table<Schema::Metrics>().Key(tabletId, followerId).Update<Schema::Metrics::MaximumMemory>(tablet->GetResourceMetricsAggregates().MaximumMemory);
                db.Table<Schema::Metrics>().Key(tabletId, followerId).Update<Schema::Metrics::MaximumNetwork>(tablet->GetResourceMetricsAggregates().MaximumNetwork);

                tablet->Statistics.SetLastAliveTimestamp(now.MilliSeconds());
                tablet->ActualizeTabletStatistics(now);
                    
                if (tablet->IsLeader()) {
                    db.Table<Schema::Tablet>()
                        .Key(tabletId)
                        .Update<Schema::Tablet::Statistics>(tablet->Statistics);
                } else {
                    db.Table<Schema::TabletFollowerTablet>()
                        .Key(tabletId, followerId)
                        .Update<Schema::TabletFollowerTablet::Statistics>(tablet->Statistics);
                }
            }
            Reply->Record.AddTabletId(tabletId);
            Reply->Record.AddFollowerId(followerId);
        }
        TNodeInfo* node = Self->FindNode(nodeId);
        if (node != nullptr) {
            node->UpdateResourceMaximum(record.GetResourceMaximum());
            node->UpdateResourceTotalUsage(record);
            node->Statistics.SetLastAliveTimestamp(now.MilliSeconds());
            node->ActualizeNodeStatistics(now);
            BLOG_TRACE("THive::TTxUpdateTabletMetrics UpdateResourceTotalUsage node "
                       << nodeId
                       << " value "
                       << ResourceRawValuesFromMetrics(record.GetTotalResourceUsage())
                       << " accumulated to "
                       << node->ResourceTotalValues);
            db.Table<Schema::Node>().Key(nodeId).Update<Schema::Node::Statistics>(node->Statistics);
            db.Table<Schema::Node>().Key(nodeId).Update<Schema::Node::MaximumCPUUsage>(node->MaximumCPUUsage);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Event->Sender, Reply.Release());
        Self->UpdateTabletMetricsInProgress--;
    }
};

ITransaction* THive::CreateUpdateTabletMetrics(TEvHive::TEvTabletMetrics::TPtr& ev) {
    return new TTxUpdateTabletMetrics(ev, this);
}

} // NHive
} // NKikimr
