#include "aggregator_impl.h"

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxInitSchema : public TTxBase {
    explicit TTxInitSchema(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT_SCHEMA; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxInitSchema::Execute");

        NIceDb::TNiceDb(txc.DB).Materialize<Schema>();

        static constexpr NIceDb::TTableId bigTableIds[] = {
            Schema::BaseStatistics::TableId,
            Schema::ColumnStatistics::TableId,
            Schema::ScanTables::TableId
        };

        for (auto id : bigTableIds) {
            const auto* tableInfo = txc.DB.GetScheme().GetTableInfo(id);
            if (!tableInfo || !tableInfo->CompactionPolicy) {
                txc.DB.Alter().SetCompactionPolicy(id, *NLocalDb::CreateDefaultUserTablePolicy());
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxInitSchema::Complete");

        Self->Execute(Self->CreateTxInit(), ctx);
    }
};

NTabletFlatExecutor::ITransaction* TStatisticsAggregator::CreateTxInitSchema() {
    return new TTxInitSchema(this);
}

} // NKikimr::NStat
