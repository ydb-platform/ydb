#include "controller_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

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
        YDB_LOG_DEBUG_CTX(ctx, "Execute",
            {"logPrefix", LogPrefix});

        NIceDb::TNiceDb db(txc.DB);
        db.Materialize<Schema>();

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG_CTX(ctx, "Complete",
            {"logPrefix", LogPrefix});
        Self->RunTxInit(ctx);
    }

}; // TTxInitSchema

void TController::RunTxInitSchema(const TActorContext& ctx) {
    Execute(new TTxInitSchema(this), ctx);
}

}
