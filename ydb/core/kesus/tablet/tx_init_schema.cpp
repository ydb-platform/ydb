#include "tablet_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KESUS_TABLET

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxInitSchema : public TTxBase {
    explicit TTxInitSchema(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT_SCHEMA; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_DEBUG_CTX(ctx, "[u] TTxInitSchema::Execute",
            {"tabletId", Self->TabletID()});
        NIceDb::TNiceDb db(txc.DB);
        db.Materialize<Schema>();
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG_CTX(ctx, "[u] TTxInitSchema::Complete",
            {"tabletId", Self->TabletID()});
        Self->Execute(Self->CreateTxInit(), ctx);
    }
};

NTabletFlatExecutor::ITransaction* TKesusTablet::CreateTxInitSchema() {
    return new TTxInitSchema(this);
}

}
}
