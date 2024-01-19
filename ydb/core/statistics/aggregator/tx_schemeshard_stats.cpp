#include "aggregator_impl.h"

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxSchemeShardStats : public TTxBase {
    NKikimrStat::TEvSchemeShardStats Record;

    TTxSchemeShardStats(TSelf* self, NKikimrStat::TEvSchemeShardStats&& record)
        : TTxBase(self)
        , Record(std::move(record))
    {}

    TTxType GetTxType() const override { return TXTYPE_SCHEMESHARD_STATS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        ui64 schemeShardId = Record.GetSchemeShardId();
        const auto& stats = Record.GetStats();

        SA_LOG_D("[" << Self->TabletID() << "] TTxSchemeShardStats::Execute: "
            << "schemeshard id# " << schemeShardId
            << ", stats size# " << stats.size());

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::BaseStats>().Key(schemeShardId).Update(
            NIceDb::TUpdate<Schema::BaseStats::Stats>(stats));

        Self->BaseStats[schemeShardId] = stats;

        return true;
    }

    void Complete(const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxSchemeShardStats::Complete");
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvSchemeShardStats::TPtr& ev) {
    auto& record = ev->Get()->Record;
    Execute(new TTxSchemeShardStats(this, std::move(record)),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat
