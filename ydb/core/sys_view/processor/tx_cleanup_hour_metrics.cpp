#include "processor_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::SYSTEM_VIEWS

namespace NKikimr::NSysView {

struct TSysViewProcessor::TTxCleanupHourMetrics : public TTxBase {
    bool More = false;
    ui64 CleanupBeforeHourEndUs = 0;

    explicit TTxCleanupHourMetrics(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_CLEANUP_HOUR_METRICS;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        auto table = db.Table<Schema::IntervalMetricsOneHour>();
        auto rowset = table.Range().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        const ui64 currentHourEndUs = Self->CurrentHourEnd.MicroSeconds();
        CleanupBeforeHourEndUs = currentHourEndUs;
        size_t deleted = 0;
        while (!rowset.EndOfSet()) {
            const ui64 hourEndUs = rowset.GetValue<Schema::IntervalMetricsOneHour::HourEnd>();
            if (hourEndUs >= currentHourEndUs) {
                break;
            }

            const auto queryHash = rowset.GetValue<Schema::IntervalMetricsOneHour::QueryHash>();
            table.Key(hourEndUs, queryHash).Delete();
            if (++deleted == Self->HourMetricsCleanupBatchSize) {
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
        const bool hourChanged = CleanupBeforeHourEndUs != Self->CurrentHourEnd.MicroSeconds();
        Self->HourMetricsCleanupInFlight = false;
        if (More || hourChanged) {
            Self->ScheduleCleanupHourMetrics();
        }
    }
};

void TSysViewProcessor::Handle(TEvPrivate::TEvCleanupHourMetrics::TPtr&) {
    Execute(new TTxCleanupHourMetrics(this), TActivationContext::AsActorContext());
}

} // namespace NKikimr::NSysView
