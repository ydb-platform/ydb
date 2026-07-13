#include "aggregator_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxInitSchema : public TTxBase {
    explicit TTxInitSchema(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT_SCHEMA; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        YDB_LOG_DEBUG("TTxInitSchema::Execute",
            {"tabletId", Self->TabletID()});

        NIceDb::TNiceDb(txc.DB).Materialize<Schema>();

        static constexpr NIceDb::TTableId bigTableIds[] = {
            Schema::BaseStatistics::TableId,
            Schema::ColumnStatistics::TableId,
            Schema::ScheduleTraversals::TableId
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
        YDB_LOG_DEBUG("TTxInitSchema::Complete",
            {"tabletId", Self->TabletID()});

        Self->Execute(Self->CreateTxInit(), ctx);
    }
};

NTabletFlatExecutor::ITransaction* TStatisticsAggregator::CreateTxInitSchema() {
    return new TTxInitSchema(this);
}

} // NKikimr::NStat
