#include "hive_impl.h"
#include "hive_log.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

namespace NKikimr {
namespace NHive {

class TTxProcessTabletMetrics : public TTransactionBase<THive> {
    TSideEffects SideEffects;
    struct TPersistedProtoMetrics {
        TFullTabletId TabletId;
        ui64 Revision;
    };
    TVector<TPersistedProtoMetrics> PersistedProtoMetrics;

    static constexpr size_t MAX_UPDATES_PROCESSED = 200;
public:
    TTxProcessTabletMetrics(THive* hive)
        : TBase(hive)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_PROCESS_TABLET_METRICS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        YDB_LOG_DEBUG("THive::TTxProcessTabletMetrics::Execute processing tablet metrics",
            {"logPrefix", GetLogPrefix()});
        NIceDb::TNiceDb db(txc.DB);
        SideEffects.Reset(Self->SelfId());
        PersistedProtoMetrics.clear();
        for (size_t i = 0; !Self->ProcessTabletMetricsQueue.empty() && i < MAX_UPDATES_PROCESSED; ++i) {
            auto tabletId = Self->ProcessTabletMetricsQueue.front();
            Self->ProcessTabletMetricsQueue.pop();
            auto* tablet = Self->FindTablet(tabletId);
            if (tablet == nullptr) {
                continue;
            }
            tablet->UpdateMetricsEnqueued = false;
            const auto& aggregates = tablet->GetResourceMetricsAggregates();
            if (tablet->IsProtoMetricsDirty()) {
                NKikimrTabletBase::TMetrics protoMetrics;
                tablet->GetResourceValues().ToProto(&protoMetrics);
                db.Table<Schema::Metrics>().Key(tabletId).Update(
                    NIceDb::TUpdate<Schema::Metrics::ProtoMetrics>(protoMetrics),
                    NIceDb::TUpdate<Schema::Metrics::MaximumCPU>(aggregates.MaximumCPU),
                    NIceDb::TUpdate<Schema::Metrics::MaximumMemory>(aggregates.MaximumMemory),
                    NIceDb::TUpdate<Schema::Metrics::MaximumNetwork>(aggregates.MaximumNetwork));
                if (PersistedProtoMetrics.empty()) {
                    PersistedProtoMetrics.reserve(MAX_UPDATES_PROCESSED);
                }
                PersistedProtoMetrics.push_back({tabletId, tablet->GetProtoMetricsRevision()});
            } else {
                db.Table<Schema::Metrics>().Key(tabletId).Update(
                    NIceDb::TUpdate<Schema::Metrics::MaximumCPU>(aggregates.MaximumCPU),
                    NIceDb::TUpdate<Schema::Metrics::MaximumMemory>(aggregates.MaximumMemory),
                    NIceDb::TUpdate<Schema::Metrics::MaximumNetwork>(aggregates.MaximumNetwork));
            }
        }
        if (Self->ProcessTabletMetricsQueue.empty()) {
            Self->ProcessTabletMetricsScheduled = false;
        } else {
            SideEffects.Send(Self->SelfId(), new TEvPrivate::TEvProcessTabletMetrics);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        // Revisions are unique across tablet incarnations within this Hive.
        for (const auto& saved : PersistedProtoMetrics) {
            if (auto* tablet = Self->FindTablet(saved.TabletId)) {
                tablet->ConfirmProtoMetricsPersistence(saved.Revision);
            }
        }
        SideEffects.Complete(ctx, Self->Requests);
    }
};

ITransaction* THive::CreateProcessTabletMetrics() {
    return new TTxProcessTabletMetrics(this);
}

} // NHive
} // NKikimr
