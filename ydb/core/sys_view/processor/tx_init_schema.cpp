#include "processor_impl.h"
#include <ydb/core/base/feature_flags.h>

namespace NKikimr {
namespace NSysView {

struct TSysViewProcessor::TTxInitSchema : public TTxBase {
    explicit TTxInitSchema(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT_SCHEMA; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SVLOG_D("[" << Self->TabletID() << "] TTxInitSchema::Execute");

        NIceDb::TNiceDb(txc.DB).Materialize<Schema>();

        static constexpr NIceDb::TTableId resultTableIds[] = {
            Schema::MetricsOneMinute::TableId,
            Schema::MetricsOneHour::TableId,
            Schema::TopByDurationOneMinute::TableId,
            Schema::TopByDurationOneHour::TableId,
            Schema::TopByReadBytesOneMinute::TableId,
            Schema::TopByReadBytesOneHour::TableId,
            Schema::TopByCpuTimeOneMinute::TableId,
            Schema::TopByCpuTimeOneHour::TableId,
            Schema::TopByRequestUnitsOneMinute::TableId,
            Schema::TopByRequestUnitsOneHour::TableId,
            Schema::TopPartitionsOneMinute::TableId,
            Schema::TopPartitionsOneHour::TableId
        };

        for (auto id : resultTableIds) {
            txc.DB.Alter().SetCompactionPolicy(id, *NLocalDb::CreateDefaultUserTablePolicy());
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SVLOG_D("[" << Self->TabletID() << "] TTxInitSchema::Complete");

        if (!AppData()->FeatureFlags.GetEnablePersistentQueryStats()) {
            SVLOG_D("[" << Self->TabletID() << "] tablet is offline");
            Self->SignalTabletActive(ctx);
            Self->Become(&TThis::StateOffline);
            return;
        }

        Self->Execute(Self->CreateTxInit(), ctx);
    }
};

NTabletFlatExecutor::ITransaction* TSysViewProcessor::CreateTxInitSchema() {
    return new TTxInitSchema(this);
}

} // NSysView
} // NKikimr
