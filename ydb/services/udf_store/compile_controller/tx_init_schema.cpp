#include "controller_impl.h"

namespace NKikimr::NUdfStore {

class TWasmCompileController::TTxInitSchema : public TTxBase {
public:
    explicit TTxInitSchema(TWasmCompileController* self)
        : TTxBase("TxInitSchema", self)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_INIT_SCHEMA;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        db.Materialize<Schema>();
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Self->RunTxInit(ctx);
    }
};

void TWasmCompileController::RunTxInitSchema(const TActorContext& ctx) {
    Execute(new TTxInitSchema(this), ctx);
}

} // namespace NKikimr::NUdfStore
