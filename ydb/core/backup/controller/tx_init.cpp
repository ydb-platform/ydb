#include "tablet_impl.h"

namespace NKikimr::NBackup {

class TBackupController::TTxInit
    : public TTxBase
{
    inline bool Load(NIceDb::TNiceDb& db) {
        Y_UNUSED(db);
        Self->Reset();
        return true;
    }

    inline bool Load(NTable::TDatabase& toughDb) {
        NIceDb::TNiceDb db(toughDb);
        return Load(db);
    }

public:
    explicit TTxInit(TSelf* self)
        : TTxBase("TxInit", self)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_INIT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(ctx);
        return Load(txc.DB);
    }

    void Complete(const TActorContext& ctx) override {
        Self->SwitchToWork(ctx);
    }
}; // TTxInit

void TBackupController::RunTxInit(const TActorContext& ctx) {
    Execute(new TTxInit(this), ctx);
}

}
