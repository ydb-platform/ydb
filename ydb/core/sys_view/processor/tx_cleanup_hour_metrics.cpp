#include "processor_impl.h"
#include "query_metrics_retention_db.h"

namespace NKikimr {
namespace NSysView {

struct TSysViewProcessor::TTxCleanupHourMetrics : public TTxBase {
    bool More = false;
    size_t Deleted = 0;
    ui64 SizeEvictedBuckets = 0;
    ui64 NewEvictBeforeHourEndUs = 0;

    explicit TTxCleanupHourMetrics(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_CLEANUP_HOUR_METRICS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        More = false;
        Deleted = 0;
        SizeEvictedBuckets = 0;
        NewEvictBeforeHourEndUs = Self->MetricsOneHourEvictBeforeHourEndUs;

        NIceDb::TNiceDb db(txc.DB);
        auto rowset = db.Table<Schema::IntervalMetricsOneHour>().Range().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        const ui64 currentHourEndUs = Self->CurrentHourEnd.MicroSeconds();
        while (!rowset.EndOfSet()) {
            const ui64 hourEndUs =
                rowset.GetValue<Schema::IntervalMetricsOneHour::HourEnd>();
            if (hourEndUs >= currentHourEndUs) {
                break;
            }

            const TQueryHash queryHash =
                rowset.GetValue<Schema::IntervalMetricsOneHour::QueryHash>();
            db.Table<Schema::IntervalMetricsOneHour>().Key(hourEndUs, queryHash).Delete();

            if (++Deleted == NQueryMetricsLimits::OneHourCleanupBatchSize) {
                More = true;
                break;
            }

            if (!rowset.Next()) {
                return false;
            }
        }

        if (Deleted < NQueryMetricsLimits::OneHourCleanupBatchSize
            && NewEvictBeforeHourEndUs)
        {
            TQueryMetricsOneHourCleanupResult cleanup;
            if (!CleanupQueryMetricsOneHour(
                    db,
                    NewEvictBeforeHourEndUs,
                    NQueryMetricsLimits::OneHourCleanupBatchSize - Deleted,
                    cleanup))
            {
                return false;
            }
            Deleted += cleanup.Deleted;
            SizeEvictedBuckets += cleanup.EvictedBuckets;
            NewEvictBeforeHourEndUs = cleanup.NewCutoff;
            More = More || cleanup.More;
        }

        if (NewEvictBeforeHourEndUs != Self->MetricsOneHourEvictBeforeHourEndUs) {
            Self->PersistMetricsOneHourEvictBeforeHourEnd(
                db, NewEvictBeforeHourEndUs);
        }

        return true;
    }

    void Complete(const TActorContext&) override {
        Self->MetricsOneHourEvictBeforeHourEndUs = NewEvictBeforeHourEndUs;
        SVLOG_D("[" << Self->TabletID() << "] TTxCleanupHourMetrics::Complete: "
            << "deleted# " << Deleted
            << ", size evicted buckets# " << SizeEvictedBuckets
            << ", more# " << More);

        Self->UpdateMetricsOneHourRetentionCounters(
            Self->MetricsOneHourRetainedBytes, SizeEvictedBuckets);

        if (More) {
            Self->HourMetricsCleanupInFlight = false;
            Self->ScheduleHourMetricsCleanup();
        } else {
            Self->HourMetricsCleanupInFlight = false;
        }
    }
};

void TSysViewProcessor::Handle(TEvPrivate::TEvCleanupHourMetrics::TPtr&) {
    Execute(new TTxCleanupHourMetrics(this), TActivationContext::AsActorContext());
}

} // NSysView
} // NKikimr
