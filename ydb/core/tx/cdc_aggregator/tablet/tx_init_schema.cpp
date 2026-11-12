#include "tablet_impl.h"

namespace NKikimr::NCdcAggregator {

class TCdcAggregator::TTxInitSchema : public TTxBase {
public:
    explicit TTxInitSchema(TCdcAggregator* self)
        : TTxBase(self)
    {
    }

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

void TCdcAggregator::RunTxInitSchema(const TActorContext& ctx) {
    Execute(new TTxInitSchema(this), ctx);
}

} // namespace NKikimr::NCdcAggregator
