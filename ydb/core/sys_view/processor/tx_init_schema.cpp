#include "processor_impl.h"
#include <ydb/core/base/feature_flags.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::SYSTEM_VIEWS

namespace NKikimr {
namespace NSysView {

struct TSysViewProcessor::TTxInitSchema : public TTxBase {
    explicit TTxInitSchema(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT_SCHEMA; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        YDB_LOG_DEBUG("TTxInitSchema::Execute",
            {"tabletId", Self->TabletID()});

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
        YDB_LOG_DEBUG("TTxInitSchema::Complete",
            {"tabletId", Self->TabletID()});

        if (!AppData()->FeatureFlags.GetEnablePersistentQueryStats()) {
            YDB_LOG_DEBUG("Tablet is offline",
                {"tabletId", Self->TabletID()});
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
