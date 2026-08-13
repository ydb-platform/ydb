#include "processor_impl.h"

namespace NKikimr {
namespace NSysView {

struct TSysViewProcessor::TTxCleanupHourMetrics : public TTxBase {
    static constexpr size_t BatchSize = 512;

    bool More = false;
    size_t Deleted = 0;
    ui64 SizeEvictedBuckets = 0;

    explicit TTxCleanupHourMetrics(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_COLLECT; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        More = false;
        Deleted = 0;
        SizeEvictedBuckets = 0;

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

            if (++Deleted == BatchSize) {
                More = true;
                break;
            }

            if (!rowset.Next()) {
                return false;
            }
        }

        if (Deleted < BatchSize && Self->MetricsOneHourEvictBeforeHourEndUs) {
            auto publicRowset = db.Table<Schema::MetricsOneHour>().Range().Select();
            if (!publicRowset.IsReady()) {
                return false;
            }

            while (!publicRowset.EndOfSet()) {
                const ui64 hourEndUs =
                    publicRowset.GetValue<Schema::MetricsOneHour::IntervalEnd>();
                if (hourEndUs >= Self->MetricsOneHourEvictBeforeHourEndUs) {
                    Self->MetricsOneHourEvictBeforeHourEndUs = 0;
                    break;
                }

                const ui32 rank = publicRowset.GetValue<Schema::MetricsOneHour::Rank>();
                db.Table<Schema::MetricsOneHour>().Key(hourEndUs, rank).Delete();
                SizeEvictedBuckets += rank == 1;

                if (++Deleted == BatchSize) {
                    More = true;
                    break;
                }

                if (!publicRowset.Next()) {
                    return false;
                }
            }
            if (publicRowset.EndOfSet()) {
                Self->MetricsOneHourEvictBeforeHourEndUs = 0;
            }
        }

        return true;
    }

    void Complete(const TActorContext&) override {
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
