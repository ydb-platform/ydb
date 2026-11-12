#include "schema.h"
#include "tablet_impl.h"

namespace NKikimr::NCdcAggregator {

class TCdcAggregator::TTxInitSchema : public NTabletFlatExecutor::TTransactionBase<TCdcAggregator> {
public:
    explicit TTxInitSchema(TCdcAggregator* self)
        : TTransactionBase(self)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_INIT_SCHEMA;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        db.Materialize<TCdcAggregatorSchema>();
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
