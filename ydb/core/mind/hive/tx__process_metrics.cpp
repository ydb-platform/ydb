#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxProcessMetrics : public TTransactionBase<THive> {
    TSideEffects SideEffects;

    static constexpr size_t MAX_UPDATES_PROCESSED = 200;
public:
    TTxProcessMetrics(THive* hive)
        : TBase(hive)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_PROCESS_METRICS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        BLOG_D("TTxProcessMetrics::Execute()");
        NIceDb::TNiceDb db(txc.DB);
        SideEffects.Reset(Self->SelfId());
        for (size_t i = 0; !Self->ProcessMetricsQueue.empty() && i < MAX_UPDATES_PROCESSED; ++i) {
            auto tabletId = Self->ProcessMetricsQueue.front();
            Self->ProcessMetricsQueue.pop();
            auto* tablet = Self->FindTablet(tabletId);
            if (tablet == nullptr) {
                continue;
            }
            tablet->UpdateMetricsEnqueued = false;
            NKikimrTabletBase::TMetrics protoMetrics;
            tablet->GetResourceValues().ToProto(&protoMetrics);
            db.Table<Schema::Metrics>().Key(tabletId).Update<Schema::Metrics::ProtoMetrics>(protoMetrics);
            db.Table<Schema::Metrics>().Key(tabletId).Update<Schema::Metrics::MaximumCPU>(tablet->GetResourceMetricsAggregates().MaximumCPU);
            db.Table<Schema::Metrics>().Key(tabletId).Update<Schema::Metrics::MaximumMemory>(tablet->GetResourceMetricsAggregates().MaximumMemory);
            db.Table<Schema::Metrics>().Key(tabletId).Update<Schema::Metrics::MaximumNetwork>(tablet->GetResourceMetricsAggregates().MaximumNetwork);
        }
        if (Self->ProcessMetricsQueue.empty()) {
            Self->ProcessMetricsScheduled = false;
        } else {
            SideEffects.Send(Self->SelfId(), new TEvPrivate::TEvProcessMetrics);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SideEffects.Complete(ctx);
    }
};

ITransaction* THive::CreateProcessMetrics() {
    return new TTxProcessMetrics(this);
}

} // NHive
} // NKikimr
