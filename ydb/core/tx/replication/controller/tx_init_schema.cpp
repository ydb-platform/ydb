#include "controller_impl.h"

namespace NKikimr::NReplication::NController {

class TController::TTxInitSchema: public TTxBase {
public:
    explicit TTxInitSchema(TController* self)
        : TTxBase("TxInitSchema", self)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_INIT_SCHEMA;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        CLOG_D(ctx, "Execute");

        NIceDb::TNiceDb db(txc.DB);
        db.Materialize<Schema>();

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        CLOG_D(ctx, "Complete");
        Self->RunTxInit(ctx);
    }

}; // TTxInitSchema

void TController::RunTxInitSchema(const TActorContext& ctx) {
    Execute(new TTxInitSchema(this), ctx);
}

}
