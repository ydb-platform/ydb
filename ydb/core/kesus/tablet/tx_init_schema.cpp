#include "tablet_impl.h"

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxInitSchema : public TTxBase {
    explicit TTxInitSchema(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT_SCHEMA; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::KESUS_TABLET, "[%lu] TTxInitSchema::Execute", Self->TabletID());
        NIceDb::TNiceDb db(txc.DB);
        db.Materialize<Schema>();
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::KESUS_TABLET, "[%lu] TTxInitSchema::Complete", Self->TabletID());
        Self->Execute(Self->CreateTxInit(), ctx);
    }
};

NTabletFlatExecutor::ITransaction* TKesusTablet::CreateTxInitSchema() {
    return new TTxInitSchema(this);
}

}
}
