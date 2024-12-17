#include "tablet_impl.h"

namespace NKikimr::NBackup {

class TBackupController::TTxInitSchema
    : public TTxBase
{
public:
    explicit TTxInitSchema(TBackupController* self)
        : TTxBase("TxInitSchema", self)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_INIT_SCHEMA;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(ctx);

        NIceDb::TNiceDb db(txc.DB);
        db.Materialize<Schema>();

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Self->RunTxInit(ctx);
    }

}; // TTxInitSchema

void TBackupController::RunTxInitSchema(const TActorContext& ctx) {
    Execute(new TTxInitSchema(this), ctx);
}

}
