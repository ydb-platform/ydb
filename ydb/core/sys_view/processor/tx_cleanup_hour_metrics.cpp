#include "processor_impl.h"

namespace NKikimr {
namespace NSysView {

struct TSysViewProcessor::TTxCleanupHourMetrics : public TTxBase {
    static constexpr size_t BatchSize = 512;

    bool More = false;
    size_t Deleted = 0;

    explicit TTxCleanupHourMetrics(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_COLLECT; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        More = false;
        Deleted = 0;

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

        return true;
    }

    void Complete(const TActorContext&) override {
        SVLOG_D("[" << Self->TabletID() << "] TTxCleanupHourMetrics::Complete: "
            << "deleted# " << Deleted
            << ", more# " << More);

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
